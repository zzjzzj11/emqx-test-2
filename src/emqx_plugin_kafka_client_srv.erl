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

%% @doc EMQX Kafka 插件客户端管理服务。
%% 作为 gen_server 运行于监督树下，持有 ETS 表 `topic_partitions`，
%% 负责启动/停止 brod 客户端与 producer，并提供分区查询接口。
%% 单个 client 启动失败不影响其他 client，实现降级运行。
-module(emqx_plugin_kafka_client_srv).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-define(TOPIC_PARTITIONS, topic_partitions).
-define(SERVER, ?MODULE).
%% 熔断器 ETS 表名（与 emqx_plugin_kafka 共享）
-define(CB_TABLE, kafka_circuit_breaker).
%% 探测周期：15 秒，比 30 秒熔断冷却时间短，能在 half_open 之前主动恢复
-define(PROBE_INTERVAL_MS, 15000).
%% 单次探测超时：5 秒，略大于 Kafka 默认 network timeout
-define(PROBE_TIMEOUT_MS, 5000).

%% API
-export([ start_link/1
        , get_partition/1
        , stop_clients/0
        ]).

%% Health monitoring (exported for testing and internal use)
-export([ init_health_metrics/0
        , schedule_probe/1
        , mark_kafka_down/1
        , mark_kafka_up/1
        , monitor_clients/1
        , monitor_one/1
        , demonitor_all/1
        , do_probe/1
        , probe_kafka/1
        , make_test_state/1
        , get_kafka_status/1
        , restart_client/2
        , handle_client_down/3
        , handle_producer_exit/3
        , get_monitors/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% States
-record(state, { clients :: [atom()]
               , topics :: [binary()]
               %% Kafka 整体健康状态：up 或 down
               , kafka_status :: up | down
               %% 上次故障时间戳 (ms)，undefined 表示未发生故障
               , down_since :: integer() | undefined
               %% 探测周期 (ms)
               , probe_interval :: integer()
               %% client 监控引用列表：[{ClientId, MonitorRef}]
               , monitors :: [{atom(), reference()}]
               }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc 启动客户端管理服务。
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Env) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Env, []).

%% @doc 查询 topic 对应的分区数。
-spec get_partition(binary()) -> non_neg_integer() | undefined.
get_partition(Topic) ->
    case ets:lookup(?TOPIC_PARTITIONS, Topic) of
        [{_, Partitions}] -> Partitions;
        [] -> undefined
    end.

%% @doc 停止所有 brod 客户端（优雅停机）。
-spec stop_clients() -> ok.
stop_clients() ->
    gen_server:cast(?SERVER, stop_clients).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

%% @doc 初始化：创建 ETS 表、检查 NIF、启动 brod 客户端、启用健康监控。
-spec init(map()) -> {ok, #state{}}.
init(Env) ->
    logger:info("[KAFKA PLUGIN]Start to init emqx plugin kafka client srv..... ~n"),
    ets:new(?TOPIC_PARTITIONS, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(crc32cer),
    {ok, _} = application:ensure_all_started(brod),
    case emqx_plugin_kafka_health:check_crc32cer_nif() of
        ok ->
            logger:info("[KAFKA PLUGIN]crc32cer NIF loaded successfully");
        {error, Diag} ->
            logger:error("[KAFKA PLUGIN]crc32cer NIF check failed, producers may fail: ~p", [Diag])
    end,
    process_flag(trap_exit, true),
    {Clients, Topics} = start_all_clients(Env),
    Monitors = monitor_clients(Clients),
    init_health_metrics(),
    schedule_probe(?PROBE_INTERVAL_MS),
    {ok, #state{clients = Clients, topics = Topics,
                kafka_status = up, down_since = undefined,
                probe_interval = ?PROBE_INTERVAL_MS, monitors = Monitors}}.

%% @doc 处理同步请求。
-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}}.
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @doc 处理异步请求：停止所有客户端。
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast(stop_clients, State) ->
    stop_all_clients(State#state.clients),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc 处理异步消息。
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc 终止回调：停止所有 brod 客户端。
-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, State) ->
    stop_all_clients(State#state.clients),
    catch ets:delete(?TOPIC_PARTITIONS),
    ok.

%% @doc 代码热更新回调。
-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc 启动所有优先级的 brod 客户端与 producer。
%% 单个 client 启动失败不影响其他 client。
-spec start_all_clients(map()) -> {[atom()], [binary()]}.
start_all_clients(Env) ->
    AddressList = translate(maps:get(address_list, Env)),
    logger:info("[KAFKA PLUGIN]KafkaAddressList = ~p~n", [AddressList]),
    Topics = priority_topics(Env),
    {ok, Clients} = start_priority_clients(AddressList, Topics),
    {Clients, Topics}.

%% @doc 获取三个优先级对应的 topic 列表。
-spec priority_topics(map()) -> [binary()].
priority_topics(Env) ->
    [ get_topic(Env, 0)
    , get_topic(Env, 1)
    , get_topic(Env, 2)
    ].

%% @doc 按优先级启动 3 个 client 与 producer，单个失败不中断。
-spec start_priority_clients([term()], [binary()]) -> {ok, [atom()]}.
start_priority_clients(AddressList, [T0, T1, T2]) ->
    C1 = start_one_client(client1, AddressList, T0),
    C2 = start_one_client(client2, AddressList, T1),
    C3 = start_one_client(client3, AddressList, T2),
    Started = lists:filter(fun is_atom/1, [C1, C2, C3]),
    {ok, Started}.

%% @doc 启动单个 brod client 与 producer，失败返回 {error, Reason}。
-spec start_one_client(atom(), [term()], binary()) -> atom() | {error, term()}.
start_one_client(ClientId, AddressList, Topic) ->
    try
        ok = brod:start_client(AddressList, ClientId),
        ok = brod:start_producer(ClientId, Topic, []),
        get_topic_partitions(ClientId, Topic),
        ClientId
    catch
        _:{Reason, _} ->
            logger:error("[KAFKA PLUGIN]Failed to start ~p: ~p", [ClientId, Reason]),
            {error, Reason}
    end.

%% @doc 获取并缓存 topic 分区数。
-spec get_topic_partitions(atom(), binary()) -> ok.
get_topic_partitions(Client, Topic) ->
    case brod:get_partitions_count(Client, Topic) of
        {ok, Partitions} ->
            logger:info("[KAFKA PLUGIN]Topic ~s has ~p partitions~n", [Topic, Partitions]),
            ets:insert(?TOPIC_PARTITIONS, {Topic, Partitions});
        {error, Reason} ->
            logger:error("[KAFKA PLUGIN]Failed to get partitions count for topic ~s: ~p~n",
                         [Topic, Reason])
    end.

%% @doc 停止所有已启动的 brod 客户端。
-spec stop_all_clients([atom()]) -> ok.
stop_all_clients(Clients) ->
    lists:foreach(fun stop_one_client/1, Clients).

%% @doc 停止单个 brod 客户端，忽略错误。
-spec stop_one_client(atom()) -> ok.
stop_one_client(ClientId) ->
    try
        brod:stop_client(ClientId)
    catch
        _:_ -> ok
    end.

%% @doc 根据优先级获取对应的 Kafka topic。
-spec get_topic(map(), integer()) -> binary().
get_topic(Env, Priority) ->
    DefaultTopic = maps:get(topic, Env, undefined),
    case Priority of
        2 -> maps:get(topic_high, Env, DefaultTopic);
        1 -> maps:get(topic_medium, Env, DefaultTopic);
        0 -> maps:get(topic_low, Env, DefaultTopic);
        _ -> DefaultTopic
    end.

%% @doc 将地址列表字符串解析为 brod 端点格式。
-spec translate(binary() | string()) -> [{string(), integer()}].
translate(AddressList) ->
    Fun = fun(S) ->
        case string:split(S, ":", trailing) of
            [Domain]       -> {Domain, 9092};
            [Domain, Port] -> {Domain, list_to_integer(Port)}
        end
    end,
    S = string:tokens(binary_to_list(AddressList), ","),
    [Fun(S1) || S1 <- S].

%%--------------------------------------------------------------------
%% Health monitoring functions
%%--------------------------------------------------------------------

%% @doc 初始化健康监控相关的 ETS 条目。
%% 使用 insert_new 确保幂等：若条目已存在则不覆盖。
-spec init_health_metrics() -> ok.
init_health_metrics() ->
    ets:insert_new(kafka_metrics, {kafka_status, up}),
    ets:insert_new(kafka_metrics, {kafka_down_count, 0}),
    ets:insert_new(kafka_metrics, {last_down_at, 0}),
    ets:insert_new(kafka_metrics, {last_recovered_at, 0}),
    ets:insert_new(kafka_metrics, {reconnect_attempts, 0}),
    ok.

%% @doc 调度下一次 Kafka 健康探测。
%% 通过 erlang:send_after/3 发送 atom 消息 'probe_kafka' 到本进程。
-spec schedule_probe(integer()) -> ok.
schedule_probe(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), probe_kafka),
    ok.

%% @doc 标记 Kafka 故障：立即打开熔断器，更新指标。
%% 使用 try/catch 防止 ETS 表不存在时崩溃。
-spec mark_kafka_down(term()) -> ok.
mark_kafka_down(_State) ->
    Now = erlang:system_time(millisecond),
    try
        ets:insert(?CB_TABLE, [{state, open}, {opened_at, Now}]),
        ets:insert(kafka_metrics, {kafka_status, down}),
        ets:insert(kafka_metrics, {last_down_at, Now}),
        ets:update_counter(kafka_metrics, kafka_down_count, 1),
        logger:warning("[KAFKA PLUGIN]Kafka marked DOWN, circuit breaker opened")
    catch
        _:_ ->
            logger:error("[KAFKA PLUGIN]Failed to mark Kafka down (ETS table missing)")
    end,
    ok.

%% @doc 标记 Kafka 恢复：立即关闭熔断器，重置失败计数，更新指标。
-spec mark_kafka_up(term()) -> ok.
mark_kafka_up(_State) ->
    Now = erlang:system_time(millisecond),
    try
        ets:insert(?CB_TABLE, [{state, closed}, {failure_count, 0}, {opened_at, 0}]),
        ets:insert(kafka_metrics, {kafka_status, up}),
        ets:insert(kafka_metrics, {last_recovered_at, Now}),
        logger:info("[KAFKA PLUGIN]Kafka marked UP, circuit breaker closed")
    catch
        _:_ ->
            logger:error("[KAFKA PLUGIN]Failed to mark Kafka up (ETS table missing)")
    end,
    ok.

%% @doc 批量监控所有 client 进程，返回 {ClientId, MonitorRef} 列表。
%% 使用 lists:filtermap/2 过滤掉未运行的 client（monitor_one 返回 false）。
-spec monitor_clients([atom()]) -> [{atom(), reference()}].
monitor_clients(Clients) ->
    lists:filtermap(
        fun(ClientId) ->
            case monitor_one(ClientId) of
                false -> false;
                {_, _} = Monitor -> {true, Monitor}
            end
        end,
        Clients).

%% @doc 监控单个 brod client 进程。
%% 通过 brod_sup:find_client/1 获取 PID，调用 erlang:monitor/2。
%% 返回 {ClientId, Ref} 或 false（若 client 未运行）。
-spec monitor_one(atom()) -> {atom(), reference()} | false.
monitor_one(ClientId) ->
    case brod_sup:find_client(ClientId) of
        [Pid] when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            {ClientId, Ref};
        [] ->
            false
    end.

%% @doc 清除所有 monitor 引用。
%% 使用 [flush] 选项清除任何待处理的 'DOWN' 消息。
-spec demonitor_all([{atom(), reference()}]) -> ok.
demonitor_all(Monitors) ->
    lists:foreach(fun({_, Ref}) -> erlang:demonitor(Ref, [flush]) end, Monitors),
    ok.

%% @doc 测试辅助函数：从 map 创建 #state{} 记录。
%% 仅用于测试，生产代码不应调用。
-spec make_test_state(map()) -> #state{}.
make_test_state(Overrides) ->
    Base = #state{clients = [], topics = [],
                  kafka_status = up, down_since = undefined,
                  probe_interval = ?PROBE_INTERVAL_MS, monitors = []},
    maps:fold(fun(kafka_status, V, S) -> S#state{kafka_status = V};
                 (down_since, V, S) -> S#state{down_since = V};
                 (probe_interval, V, S) -> S#state{probe_interval = V};
                 (clients, V, S) -> S#state{clients = V};
                 (topics, V, S) -> S#state{topics = V};
                 (monitors, V, S) -> S#state{monitors = V};
                 (_, _, S) -> S
              end, Base, Overrides).

%% @doc 对单个 client 执行健康探测。
%% 使用 brod:get_partitions_count/2 验证 Kafka 连通性。
%% 从 topic_partitions ETS 取已知 topic，无则用 <<"probe">>。
-spec do_probe(atom()) -> ok | {error, term()}.
do_probe(ClientId) ->
    case brod_sup:find_client(ClientId) of
        [Pid] when is_pid(Pid) ->
            Topic = probe_topic(),
            try brod:get_partitions_count(ClientId, Topic) of
                {ok, _} -> ok;
                {error, unknown_topic_or_partition} -> ok;
                {error, Reason} -> {error, Reason}
            catch
                _:Reason -> {error, Reason}
            end;
        [] ->
            {error, client_not_found}
    end.

%% @doc 获取探测使用的 topic：优先从 ETS 取已知 topic，无则用 <<"probe">>。
-spec probe_topic() -> binary().
probe_topic() ->
    try ets:first(?TOPIC_PARTITIONS) of
        '$end_of_table' -> <<"probe">>;
        Topic when is_binary(Topic) -> Topic
    catch
        _:_ -> <<"probe">>
    end.

%% @doc 执行 Kafka 健康探测并返回新 State。
%% 根据当前 kafka_status 和探测结果决定状态转换。
-spec probe_kafka(#state{}) -> #state{}.
probe_kafka(State) ->
    case do_probe(first_client(State)) of
        ok ->
            handle_probe_success(State);
        {error, _} ->
            handle_probe_failure(State)
    end.

%% @doc 处理探测成功：若之前是 down，则标记恢复并重新监控。
-spec handle_probe_success(#state{}) -> #state{}.
handle_probe_success(State) ->
    case State#state.kafka_status of
        down ->
            mark_kafka_up(State),
            Monitors = monitor_clients(State#state.clients),
            State#state{kafka_status = up, down_since = undefined, monitors = Monitors};
        up ->
            State
    end.

%% @doc 处理探测失败：若之前是 up，则标记故障。
-spec handle_probe_failure(#state{}) -> #state{}.
handle_probe_failure(State) ->
    case State#state.kafka_status of
        up ->
            mark_kafka_down(State),
            demonitor_all(State#state.monitors),
            State#state{kafka_status = down,
                        down_since = erlang:system_time(millisecond),
                        monitors = []};
        down ->
            State
    end.

%% @doc 获取 state.clients 的第一个 client，若无则返回 client1。
-spec first_client(#state{}) -> atom().
first_client(State) ->
    case State#state.clients of
        [C | _] -> C;
        [] -> client1
    end.

%% @doc 测试辅助：从 state 读取 kafka_status 字段。
-spec get_kafka_status(#state{}) -> up | down.
get_kafka_status(State) ->
    State#state.kafka_status.

%% @doc 重启单个 brod client 与 producer。
%% 先停止旧 client（若存在），再重新启动。
%% 返回 {ok, ClientId} 或 {error, Reason}。
-spec restart_client(atom(), binary()) -> {ok, atom()} | {error, term()}.
restart_client(ClientId, Topic) ->
    catch brod:stop_client(ClientId),
    case brod:start_client(get_address_list(), ClientId) of
        ok ->
            case brod:start_producer(ClientId, Topic, []) of
                ok ->
                    try get_topic_partitions(ClientId, Topic)
                    catch _:_ -> ok
                    end,
                    {ok, ClientId};
                {error, Reason} ->
                    logger:error("[KAFKA PLUGIN]Failed to restart producer for ~p: ~p",
                                 [ClientId, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            logger:error("[KAFKA PLUGIN]Failed to restart client ~p: ~p",
                         [ClientId, Reason]),
            {error, Reason}
    end.

%% @doc 获取 Kafka 地址列表（从 persistent_term 缓存读取）。
%% 注意：persistent_term key 需与 emqx_plugin_kafka.erl 中的 ?CONFIG_KEY 保持一致。
-spec get_address_list() -> [{string(), integer()}].
get_address_list() ->
    case persistent_term:get({emqx_plugin_kafka, kafka_config}, undefined) of
        undefined ->
            logger:warning("[KAFKA PLUGIN]Kafka config not cached, using default localhost:9092"),
            [{"localhost", 9092}];
        Env ->
            translate(maps:get(address_list, Env))
    end.

%% @doc 处理 client 进程崩溃事件（由 monitor 触发）。
%% 记录故障，标记 Kafka down，等待下次 probe 周期重启。
-spec handle_client_down(atom(), term(), #state{}) -> #state{}.
handle_client_down(ClientId, Reason, State) ->
    logger:warning("[KAFKA PLUGIN]Client ~p crashed: ~p", [ClientId, Reason]),
    case State#state.kafka_status of
        up ->
            mark_kafka_down(State),
            State#state{kafka_status = down,
                        down_since = erlang:system_time(millisecond)};
        down ->
            State
    end.

%% @doc 处理 producer 进程崩溃事件（由 trap_exit 触发）。
%% producer 崩溃通常意味着 Kafka 不可达，标记 Kafka down。
-spec handle_producer_exit(pid(), term(), #state{}) -> #state{}.
handle_producer_exit(Pid, Reason, State) ->
    logger:warning("[KAFKA PLUGIN]Producer ~p exited: ~p", [Pid, Reason]),
    case State#state.kafka_status of
        up ->
            mark_kafka_down(State),
            demonitor_all(State#state.monitors),
            State#state{kafka_status = down,
                        down_since = erlang:system_time(millisecond),
                        monitors = []};
        down ->
            State
    end.

%% @doc 测试辅助：从 state 读取 monitors 字段。
-spec get_monitors(#state{}) -> [{atom(), reference()}].
get_monitors(State) ->
    State#state.monitors.
