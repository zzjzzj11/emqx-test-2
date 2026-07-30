%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc EMQX Kafka 插件核心模块。
%% 负责挂载 EMQX 钩子、按优先级路由消息到 Kafka、
%% 提供熔断器降级、配置缓存、指标暴露与优雅停机。
-module(emqx_plugin_kafka).

%% for #message{} record
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

%% ETS 表名（由 emqx_plugin_kafka_client_srv 创建与持有）
-define(TOPIC_PARTITIONS, topic_partitions).

%% 熔断器 ETS 表名
-define(CB_TABLE, kafka_circuit_breaker).

%% 熔断器配置
-define(FAILURE_THRESHOLD, 5).
-define(COOLDOWN_MS, 30000).

%% 配置缓存 key
-define(CONFIG_KEY, {emqx_plugin_kafka, kafka_config}).

-export([ load/1
        , unload/0
        , get_metrics/0
        , init_tables/0
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connect/3
        , on_client_connack/4
        , on_client_connected/3
        , on_client_disconnected/4
        , on_client_authenticate/3
        , on_client_authorize/5
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_resumed/3
        , on_session_discarded/3
        , on_session_takeovered/3
        , on_session_terminated/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        , on_message_dropped/4
        ]).

%% Called when the plugin application start
%% @doc 挂载所有钩子并初始化熔断器表与配置缓存。
-spec load(map()) -> ok.
load(Env) ->
  cache_config(Env),
  init_tables(),
  hook_all(Env).

%% @doc 卸载所有钩子并停止 Kafka 客户端（优雅停机）。
-spec unload() -> ok.
unload() ->
  unhook_all(),
  emqx_plugin_kafka_client_srv:stop_clients(),
  clear_cache().

%% @doc 获取 Kafka 生产指标（成功与失败计数）。
-spec get_metrics() -> #{success => integer(), failed => integer()}.
get_metrics() ->
  #{ success => read_metric(success)
   , failed => read_metric(failed)
   }.

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

%% @doc 客户端连接钩子。
-spec on_client_connect(map(), map(), map()) -> {ok, map()}.
on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
  logger:debug("Client(~s) connect, ConnInfo: ~p, Props: ~p~n", [ClientId, ConnInfo, Props]),
  {ok, Props}.

%% @doc 客户端 connack 钩子。
-spec on_client_connack(map(), term(), map(), map()) -> {ok, map()}.
on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  logger:debug("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n", [ClientId, ConnInfo, Rc, Props]),
  {ok, Props}.

%% @doc 客户端已连接钩子，发送上线事件到 Kafka。
-spec on_client_connected(map(), map(), map()) -> ok.
on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
  Now = erlang:system_time(millisecond),
  Payload = build_connect_payload(ClientId, ClientInfo, ConnInfo, Now),
  Topic = get_kafka_topic(1),
  produce_kafka_payload(client2, ClientId, Payload, Topic),
  logger:info("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n", [ClientId, ClientInfo, ConnInfo]).

%% @doc 客户端断开连接钩子，发送下线事件到 Kafka。
-spec on_client_disconnected(map(), term(), map(), map()) -> ok.
on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, _ConnInfo, _Env) ->
  Now = erlang:system_time(millisecond),
  Payload = build_disconnect_payload(ClientId, ClientInfo, ReasonCode, Now),
  Topic = get_kafka_topic(1),
  produce_kafka_payload(client2, ClientId, Payload, Topic),
  logger:info("Client(~s) disconnected due to ~p~n", [ClientId, ReasonCode]).

%% @doc 客户端认证钩子。
-spec on_client_authenticate(map(), map(), map()) -> {ok, map()}.
on_client_authenticate(ClientInfo = #{clientid := ClientId}, Result, _Env) ->
  logger:debug("Client(~s) authenticate, ClientInfo:~n~p~n, Result:~p~n", [ClientId, ClientInfo, Result]),
  {ok, Result}.

%% @doc 客户端授权钩子。
%% publish 动作下对非 iot-service 账号进行 topic 白名单校验，
%% 仅允许发布 /data/rep/${productKey}/${deviceName} 与 /data/req/${productKey}/${deviceName}。
-spec on_client_authorize(map(), atom() | map(), binary(), map(), map()) -> {ok, map()} | {stop, map()}.
on_client_authorize(ClientInfo = #{clientid := ClientId}, PubSub, Topic, Result, _Env) ->
  logger:debug("Client(~s) authorize, ~p to topic(~s) ", [ClientId, PubSub, Topic]),
  case is_publish_action(PubSub) of
    true -> authorize_publish(ClientInfo, Topic, Result);
    false -> {ok, Result}
  end.

%% @doc 客户端订阅钩子。
-spec on_client_subscribe(map(), map(), list(), map()) -> {ok, list()}.
on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  logger:debug("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

%% @doc 客户端取消订阅钩子。
-spec on_client_unsubscribe(map(), map(), list(), map()) -> {ok, list()}.
on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  logger:debug("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session LifeCircle Hooks
%%--------------------------------------------------------------------

%% @doc 会话创建钩子。
-spec on_session_created(map(), map(), map()) -> ok.
on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
    logger:debug("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

%% @doc 会话订阅钩子。
-spec on_session_subscribed(map(), binary(), map(), map()) -> ok.
on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    logger:debug("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

%% @doc 会话取消订阅钩子。
-spec on_session_unsubscribed(map(), binary(), map(), map()) -> ok.
on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    logger:debug("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

%% @doc 会话恢复钩子。
-spec on_session_resumed(map(), map(), map()) -> ok.
on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
    logger:debug("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

%% @doc 会话丢弃钩子。
-spec on_session_discarded(map(), map(), map()) -> ok.
on_session_discarded(#{clientid := ClientId}, SessInfo, _Env) ->
    logger:debug("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

%% @doc 会话接管钩子。
-spec on_session_takeovered(map(), map(), map()) -> ok.
on_session_takeovered(#{clientid := ClientId}, SessInfo, _Env) ->
    logger:debug("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

%% @doc 会话终止钩子。
-spec on_session_terminated(map(), term(), map(), map()) -> ok.
on_session_terminated(#{clientid := ClientId}, Reason, SessInfo, _Env) ->
    logger:debug("Session(~s) is terminated due to ~p~nSession Info: ~p~n", [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% @doc 消息丢弃钩子（系统主题）。
-spec on_message_dropped(#message{}, map(), term(), map()) -> ok.
on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;

%% @doc 消息丢弃钩子。
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
  logger:debug("Message dropped by node ~p due to ~p:~n~p~n",[Node, Reason, emqx_message:to_map(Message)]).

%% @doc 消息发布钩子（系统主题直接放行）。
-spec on_message_publish(#message{}, map()) -> {ok, #message{}}.
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  {ok, Message};

%% @doc 消息发布钩子，按配置匹配后发送到 Kafka。
on_message_publish(Message, _Env) ->
  logger:debug("before message publish: ~p", [emqx_message:to_map(Message)]),
  maybe_produce_to_kafka(Message),
  {ok, Message}.

%% @doc 消息送达钩子。
-spec on_message_delivered(map(), #message{}, map()) -> {ok, #message{}}.
on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  logger:debug("Message delivered to client(~s):~n~p~n", [ClientId, emqx_message:to_map(Message)]),
  {ok, Message}.

%% @doc 消息 ACK 钩子。
-spec on_message_acked(map(), #message{}, map()) -> ok.
on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  logger:debug("Message acked by client(~s):~n~p~n", [ClientId, emqx_message:to_map(Message)]).

%%--------------------------------------------------------------------
%% Kafka produce with circuit breaker
%%--------------------------------------------------------------------

%% @doc 完整接口：检查熔断器后发送消息到 Kafka。
-spec produce_kafka_payload(atom(), term(), term(), binary()) -> ok | {error, term()}.
produce_kafka_payload(Client, Key, Message, Topic) ->
  case check_circuit() of
    allow ->
      do_produce(Client, Key, Message, Topic);
    deny ->
      increment_metric(failed),
      logger:warning("[KAFKA PLUGIN]Circuit breaker open, message dropped for topic ~s", [Topic]),
      {error, circuit_open}
  end.

%% @doc 实际执行 Kafka 生产，检查返回值并更新熔断器与指标。
-spec do_produce(atom(), term(), term(), binary()) -> ok | {error, term()}.
do_produce(Client, Key, Message, Topic) ->
  MessageBody = jsx:encode(Message),
  Partition = select_partition(Topic),
  AckCb = fun(Part, BaseOffset) ->
    handle_produce_ack(Part, BaseOffset)
  end,
  case brod:produce_cb(Client, Topic, Partition, Key, MessageBody, AckCb) of
    ok ->
      increment_metric(success),
      reset_failures();
    {ok, _Partition} ->
      increment_metric(success),
      reset_failures();
    {error, Reason} ->
      increment_metric(failed),
      record_failure(),
      logger:error("[KAFKA PLUGIN]Produce failed for topic ~s: ~p", [Topic, Reason]),
      {error, Reason}
  end.

%% @doc 处理 produce ack 回调。
-spec handle_produce_ack(term(), term()) -> ok.
handle_produce_ack(Partition, BaseOffset) ->
  logger:debug("Produced to partition ~p at base-offset ~p", [Partition, BaseOffset]),
  ok.

%%--------------------------------------------------------------------
%% Circuit breaker
%%--------------------------------------------------------------------

%% @doc 初始化熔断器 ETS 表与指标表。
-spec init_tables() -> ok.
init_tables() ->
  case ets:info(?CB_TABLE, name) of
    undefined ->
      ets:new(?CB_TABLE, [named_table, public, set]),
      ets:insert(?CB_TABLE, [{state, closed}, {failure_count, 0}, {opened_at, 0}]),
      ets:new(kafka_metrics, [named_table, public, set]),
      ets:insert(kafka_metrics, [{success, 0}, {failed, 0}]),
      ok;
    _ ->
      ok
  end.

%% @doc 检查熔断器状态，返回 allow 或 deny。
-spec check_circuit() -> allow | deny.
check_circuit() ->
  case read_cb_state() of
    closed -> allow;
    half_open -> allow;
    open -> maybe_half_open()
  end.

%% @doc open 状态下检查是否已过冷却时间，可转入 half_open。
-spec maybe_half_open() -> allow | deny.
maybe_half_open() ->
  OpenedAt = read_cb_value(opened_at, 0),
  Now = erlang:system_time(millisecond),
  case Now - OpenedAt >= ?COOLDOWN_MS of
    true ->
      set_cb_state(half_open),
      allow;
    false ->
      deny
  end.

%% @doc 记录一次失败，达到阈值则打开熔断器。
-spec record_failure() -> ok.
record_failure() ->
  Count = read_cb_value(failure_count, 0) + 1,
  ets:insert(?CB_TABLE, {failure_count, Count}),
  case Count >= ?FAILURE_THRESHOLD of
    true ->
      set_cb_state(open),
      ets:insert(?CB_TABLE, {opened_at, erlang:system_time(millisecond)}),
      logger:warning("[KAFKA PLUGIN]Circuit breaker opened after ~p failures", [Count]);
    false ->
      ok
  end.

%% @doc 重置失败计数并关闭熔断器。
-spec reset_failures() -> ok.
reset_failures() ->
  ets:insert(?CB_TABLE, [{failure_count, 0}, {state, closed}]),
  ok.

%% @doc 读取熔断器状态。
-spec read_cb_state() -> closed | open | half_open.
read_cb_state() ->
  read_cb_value(state, closed).

%% @doc 读取熔断器 ETS 值。
-spec read_cb_value(atom(), term()) -> term().
read_cb_value(Key, Default) ->
  case ets:lookup(?CB_TABLE, Key) of
    [{_, Val}] -> Val;
    [] -> Default
  end.

%% @doc 设置熔断器状态。
-spec set_cb_state(atom()) -> ok.
set_cb_state(State) ->
  ets:insert(?CB_TABLE, {state, State}),
  ok.

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

%% @doc 递增指定指标计数器。
-spec increment_metric(success | failed) -> ok.
increment_metric(success) ->
  update_metric(success);
increment_metric(failed) ->
  update_metric(failed).

%% @doc 更新指标计数器。
-spec update_metric(atom()) -> ok.
update_metric(Key) ->
  case ets:lookup(kafka_metrics, Key) of
    [{_, Val}] -> ets:insert(kafka_metrics, {Key, Val + 1});
    [] -> ets:insert(kafka_metrics, {Key, 1})
  end,
  ok.

%% @doc 读取指标计数器。
-spec read_metric(atom()) -> integer().
read_metric(Key) ->
  case ets:lookup(kafka_metrics, Key) of
    [{_, Val}] -> Val;
    [] -> 0
  end.

%%--------------------------------------------------------------------
%% Config caching
%%--------------------------------------------------------------------

%% @doc 将 Kafka 配置缓存到 persistent_term。
-spec cache_config(map()) -> ok.
cache_config(Env) ->
  persistent_term:put(?CONFIG_KEY, Env),
  ok.

%% @doc 清除配置缓存。
-spec clear_cache() -> ok.
clear_cache() ->
  persistent_term:erase(?CONFIG_KEY),
  ok.

%% @doc 从缓存读取 Kafka 配置。
-spec get_cached_config() -> map().
get_cached_config() ->
  persistent_term:get(?CONFIG_KEY, #{}).

%% @doc 根据优先级获取对应的 Kafka topic（从缓存读取配置）。
-spec get_kafka_topic(integer() | undefined) -> binary().
get_kafka_topic(Priority) ->
  Conf = get_cached_config(),
  DefaultTopic = maps:get(topic, Conf, undefined),
  case Priority of
    2 -> maps:get(topic_high, Conf, DefaultTopic);
    1 -> maps:get(topic_medium, Conf, DefaultTopic);
    0 -> maps:get(topic_low, Conf, DefaultTopic);
    _ -> DefaultTopic
  end.

%%--------------------------------------------------------------------
%% Partition selection
%%--------------------------------------------------------------------

%% @doc 随机选择 topic 的分区号。
-spec select_partition(binary()) -> non_neg_integer() | random.
select_partition(Topic) ->
  Partitions = emqx_plugin_kafka_client_srv:get_partition(Topic),
  case Partitions of
    N when is_integer(N) andalso N > 0 ->
      rand:uniform(N) - 1;
    _ ->
      random
  end.

%%--------------------------------------------------------------------
%% Payload builders
%%--------------------------------------------------------------------

%% @doc 构建客户端上线事件 payload。
-spec build_connect_payload(binary(), map(), map(), integer()) -> list().
build_connect_payload(ClientId, ClientInfo, ConnInfo, Now) ->
  [ {action, <<"connected">>}
  , {device_id, ClientId}
  , {username, maps:get(username, ClientInfo)}
  , {keepalive, maps:get(keepalive, ConnInfo)}
  , {ipaddress, iolist_to_binary(ntoa(element(1, maps:get(peername, ConnInfo))))}
  , {proto_name, maps:get(proto_name, ConnInfo)}
  , {proto_ver, maps:get(proto_ver, ConnInfo)}
  , {ts, Now}
  , {online, 1}
  ].

%% @doc 构建客户端下线事件 payload。
-spec build_disconnect_payload(binary(), map(), term(), integer()) -> list().
build_disconnect_payload(ClientId, ClientInfo, ReasonCode, Now) ->
  [ {action, <<"disconnected">>}
  , {device_id, ClientId}
  , {username, maps:get(username, ClientInfo)}
  , {reason, ReasonCode}
  , {ts, Now}
  , {online, 0}
  ].

%%--------------------------------------------------------------------
%% Message publish routing
%%--------------------------------------------------------------------

%% @doc 根据配置的 mqtt_topics 匹配规则决定是否发送到 Kafka。
-spec maybe_produce_to_kafka(#message{}) -> ok.
maybe_produce_to_kafka(Message) ->
  Conf = get_cached_config(),
  case maps:get(mqtt_topics, Conf, undefined) of
    undefined ->
      produce_kafka_msg(Message);
    Topics ->
      case topic_matches(Message, Topics) of
        true -> produce_kafka_msg(Message);
        false -> ok
      end
  end.

%% @doc 检查消息 topic 是否匹配配置的 topic 模式列表。
-spec topic_matches(#message{}, [binary()]) -> boolean().
topic_matches(Message, Topics) ->
  Topic = emqx_message:topic(Message),
  lists:any(fun(Pattern) -> emqx_topic:match(Topic, Pattern) end, Topics).

%% @doc 按优先级路由消息到对应 Kafka topic 与 client。
-spec produce_kafka_msg(#message{}) -> {ok, #message{}}.
produce_kafka_msg(Message) ->
  case format_payload(Message) of
    {ok, ClientId, Payload} ->
      PriorityInt = extract_priority(Message),
      Topic = get_kafka_topic(PriorityInt),
      Client = client_for_priority(PriorityInt),
      produce_kafka_payload(Client, ClientId, Payload, Topic),
      {ok, Message};
    {error, Reason} ->
      logger:error("[KAFKA PLUGIN]Failed to format payload: ~p", [Reason]),
      {ok, Message}
  end.

%% @doc 从消息 User-Property 提取 priority 并转为整数。
-spec extract_priority(#message{}) -> integer() | undefined.
extract_priority(Message) ->
  case emqx_message:get_header(properties, Message) of
    undefined -> undefined;
    Properties ->
      extract_priority_from_props(Properties)
  end.

%% @doc 从 properties map 提取 priority。
-spec extract_priority_from_props(map()) -> integer() | undefined.
extract_priority_from_props(Properties) ->
  case maps:get('User-Property', Properties, undefined) of
    undefined -> undefined;
    UserProps ->
      case lists:keyfind(<<"priority">>, 1, UserProps) of
        {_, P} -> safe_to_integer(P);
        false -> undefined
      end
  end.

%% @doc 安全将 binary 转为整数。
-spec safe_to_integer(binary()) -> integer() | undefined.
safe_to_integer(Bin) ->
  try list_to_integer(binary_to_list(Bin)) of
    Int -> Int
  catch
    _:_ -> undefined
  end.

%% @doc 根据优先级返回对应的 client 原子。
-spec client_for_priority(integer() | undefined) -> atom().
client_for_priority(2) -> client3;
client_for_priority(1) -> client2;
client_for_priority(0) -> client1;
client_for_priority(_) -> client1.

%% @doc 格式化 MQTT 消息为 Kafka payload。
-spec format_payload(#message{}) -> {ok, term(), list()} | {error, term()}.
format_payload(Message) ->
  try
    Username = emqx_message:get_header(username, Message),
    Topic = Message#message.topic,
    ClientId = Message#message.from,
    MsgPayload = Message#message.payload,
    MsgPayload64 = encode_payload(Topic, MsgPayload),
    UserProperty = extract_user_property(Message),
    Payload = [ {action, message_publish}
              , {device_id, ClientId}
              , {username, Username}
              , {topic, Topic}
              , {payload, MsgPayload64}
              , {ts, Message#message.timestamp}
              , {user_property, UserProperty}
              ],
    {ok, ClientId, Payload}
  catch
    _:{badkey, Key} -> {error, {bad_message_format, Key}};
    _:Reason -> {error, Reason}
  end.

%% @doc 根据 topic 后缀编码 payload（_raw 后缀用 base64，否则 hex）。
-spec encode_payload(binary(), binary()) -> binary().
encode_payload(Topic, MsgPayload) ->
  Tail = string:right(binary_to_list(Topic), 4),
  case string:equal(Tail, <<"_raw">>) of
    true ->
      list_to_binary(base64:encode_to_string(MsgPayload));
    false ->
      binary:encode_hex(MsgPayload)
  end.

%% @doc 提取消息的 User-Property 头。
-spec extract_user_property(#message{}) -> term().
extract_user_property(Message) ->
  case emqx_message:get_header(properties, Message) of
    undefined -> undefined;
    Properties -> maps:get('User-Property', Properties, undefined)
  end.

%%--------------------------------------------------------------------
%% Hooks helpers
%%--------------------------------------------------------------------

%% @doc 挂载所有钩子。
-spec hook_all(map()) -> ok.
hook_all(Env) ->
  hook('client.connect',      {?MODULE, on_client_connect, [Env]}),
  hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
  hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
  hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
  hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
  hook('client.authorize',    {?MODULE, on_client_authorize, [Env]}),
  hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
  hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
  hook('session.created',     {?MODULE, on_session_created, [Env]}),
  hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
  hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
  hook('session.resumed',     {?MODULE, on_session_resumed, [Env]}),
  hook('session.discarded',   {?MODULE, on_session_discarded, [Env]}),
  hook('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}),
  hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
  hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
  hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
  hook('message.acked',       {?MODULE, on_message_acked, [Env]}),
  hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}),
  ok.

%% @doc 卸载所有钩子。
-spec unhook_all() -> ok.
unhook_all() ->
  unhook('client.connect',      {?MODULE, on_client_connect}),
  unhook('client.connack',      {?MODULE, on_client_connack}),
  unhook('client.connected',    {?MODULE, on_client_connected}),
  unhook('client.disconnected', {?MODULE, on_client_disconnected}),
  unhook('client.authenticate', {?MODULE, on_client_authenticate}),
  unhook('client.authorize',    {?MODULE, on_client_authorize}),
  unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
  unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
  unhook('session.created',     {?MODULE, on_session_created}),
  unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
  unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
  unhook('session.resumed',     {?MODULE, on_session_resumed}),
  unhook('session.discarded',   {?MODULE, on_session_discarded}),
  unhook('session.takeovered',  {?MODULE, on_session_takeovered}),
  unhook('session.terminated',  {?MODULE, on_session_terminated}),
  unhook('message.publish',     {?MODULE, on_message_publish}),
  unhook('message.delivered',   {?MODULE, on_message_delivered}),
  unhook('message.acked',       {?MODULE, on_message_acked}),
  unhook('message.dropped',     {?MODULE, on_message_dropped}),
  ok.

%% @doc 注册单个钩子（最高优先级）。
-spec hook(atom(), {module(), atom(), list()}) -> ok.
hook(HookPoint, MFA) ->
  emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

%% @doc 注销单个钩子。
-spec unhook(atom(), {module(), atom()}) -> ok.
unhook(HookPoint, MFA) ->
  emqx_hooks:del(HookPoint, MFA).

%%--------------------------------------------------------------------
%% Utility functions
%%--------------------------------------------------------------------

%% @doc IPv4/IPv6 地址转字符串。
-spec ntoa(term()) -> string().
ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).

%%--------------------------------------------------------------------
%% Publish authorization
%%--------------------------------------------------------------------

%% @doc 判断授权动作是否为 publish（兼容 atom 与 EMQX 5.x action map 两种形式）。
-spec is_publish_action(atom() | map()) -> boolean().
is_publish_action(publish) -> true;
is_publish_action(#{action_type := publish}) -> true;
is_publish_action(_) -> false.

%% @doc 对 publish 动作进行授权：iot-service 账号直接放行，
%% 其他账号校验 topic 是否在允许列表内，不匹配则拒绝。
-spec authorize_publish(map(), binary(), map()) -> {ok, map()} | {stop, map()}.
authorize_publish(#{username := <<"iot-service">>}, _Topic, Result) ->
  {ok, Result};
authorize_publish(#{clientid := ClientId, username := Username}, Topic, Result) ->
  case is_topic_allowed(Username, Topic) of
    true ->
      {ok, Result};
    false ->
      logger:warning("[KAFKA PLUGIN]Client(~s) username(~s) publish to topic(~s) denied",
                     [ClientId, Username, Topic]),
      {stop, Result#{result => deny}}
  end;
authorize_publish(#{clientid := ClientId}, Topic, Result) ->
  %% 无 username 的账号视为非 iot-service，拒绝发布
  logger:warning("[KAFKA PLUGIN]Client(~s) without username publish to topic(~s) denied",
                 [ClientId, Topic]),
  {stop, Result#{result => deny}}.

%% @doc 校验 topic 是否为该设备允许发布的 topic。
%% username 格式为 ${productKey}&${deviceName}，先做精确匹配再做通配模式匹配。
-spec is_topic_allowed(binary() | undefined, binary()) -> boolean().
is_topic_allowed(Username, Topic) when is_binary(Username) ->
  case binary:split(Username, <<"&">>) of
    [ProductKey, DeviceName] when ProductKey =/= <<>>, DeviceName =/= <<>> ->
      lists:member(Topic, allowed_pub_topics_exact(ProductKey, DeviceName))
        orelse topic_matches_patterns(Topic, allowed_pub_topic_patterns(ProductKey, DeviceName));
    _ ->
      false
  end;
is_topic_allowed(_, _) ->
  false.

%% @doc 精确匹配允许的发布 topic 列表。
-spec allowed_pub_topics_exact(binary(), binary()) -> [binary()].
allowed_pub_topics_exact(ProductKey, DeviceName) ->
  [ <<"/data/rep/", ProductKey/binary, "/", DeviceName/binary>>
  , <<"/data/req/", ProductKey/binary, "/", DeviceName/binary>>
  ].

%% @doc 通配模式匹配允许的发布 topic 列表（+ 匹配单级通配符）。
-spec allowed_pub_topic_patterns(binary(), binary()) -> [binary()].
allowed_pub_topic_patterns(ProductKey, DeviceName) ->
  [ <<"/cmd/sync/resp/", ProductKey/binary, "/", DeviceName/binary, "/+">>
  ].

%% @doc 用 MQTT 通配规则检查 topic 是否匹配任一模式。
-spec topic_matches_patterns(binary(), [binary()]) -> boolean().
topic_matches_patterns(Topic, Patterns) ->
  lists:any(fun(Pattern) -> emqx_topic:match(Topic, Pattern) end, Patterns).
