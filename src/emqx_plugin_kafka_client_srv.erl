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

%% API
-export([ start_link/1
        , get_partition/1
        , stop_clients/0
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

%% @doc 初始化：创建 ETS 表、检查 NIF、启动 brod 客户端。
-spec init(map()) -> {ok, #state{}}.
init(Env) ->
    logger:info("[KAFKA PLUGIN]Start to init emqx plugin kafka client srv..... ~n"),
    ets:new(?TOPIC_PARTITIONS, [named_table, public, set]),
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(crc32cer),
    {ok, _} = application:ensure_all_started(brod),
    case emqx_plugin_kafka_health:check_crc32cer_nif() of
        ok ->
            logger:info("[KAFKA PLUGIN]crc32cer NIF loaded successfully");
        {error, Diag} ->
            logger:error("[KAFKA PLUGIN]crc32cer NIF check failed, producers may fail: ~p", [Diag])
    end,
    {Clients, Topics} = start_all_clients(Env),
    {ok, #state{clients = Clients, topics = Topics}}.

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
