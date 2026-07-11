%% @doc Common Test suite for emqx_plugin_kafka_client_srv health monitoring.
%% Tests state transitions, monitor handling, and probe-based recovery.
-module(emqx_plugin_kafka_client_srv_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([ all/0
        , init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

%% Test cases
-export([ t_suite_loads/1
        , t_init_health_metrics/1
        , t_schedule_probe/1
        , t_mark_kafka_down/1
        , t_mark_kafka_up/1
        , t_monitor_one/1
        , t_monitor_clients/1
        , t_demonitor_all/1
        , t_do_probe_success/1
        , t_do_probe_failure/1
        , t_probe_kafka_down_to_up/1
        , t_restart_client_success/1
        , t_restart_client_failure/1
        , t_restart_client_producer_failure/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure,
          t_restart_client_producer_failure].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(crc32cer),
    {ok, _} = application:ensure_all_started(brod),
    Config.

end_per_suite(_Config) ->
    application:stop(brod),
    application:stop(crc32cer),
    application:stop(crypto),
    ok.

init_per_testcase(_TestCase, Config) ->
    meck:new(brod, [unstick, passthrough]),
    meck:new(brod_sup, [unstick, passthrough]),
    %% Ensure clean ETS state
    catch ets:delete(topic_partitions),
    catch ets:delete(kafka_circuit_breaker),
    catch ets:delete(kafka_metrics),
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(brod_sup),
    meck:unload(brod),
    catch ets:delete(topic_partitions),
    catch ets:delete(kafka_circuit_breaker),
    catch ets:delete(kafka_metrics),
    ok.

%% @doc Smoke test: suite loads and basic module is callable.
%% Calls module_info(module) which returns the module atom; this crashes
%% with undef if the module is not compiled/loaded, so it is a real
%% loadability check rather than a trivially-true assertion.
t_suite_loads(_Config) ->
    ?assertEqual(emqx_plugin_kafka_client_srv,
                 emqx_plugin_kafka_client_srv:module_info(module)),
    ok.

%% @doc init_health_metrics/0 should create all required ETS entries in kafka_metrics.
t_init_health_metrics(_Config) ->
    %% Create the kafka_metrics table (normally done by emqx_plugin_kafka:init_tables/0)
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    ?assertEqual(up, ets:lookup_element(kafka_metrics, kafka_status, 2)),
    ?assertEqual(0, ets:lookup_element(kafka_metrics, kafka_down_count, 2)),
    ?assertEqual(0, ets:lookup_element(kafka_metrics, last_down_at, 2)),
    ?assertEqual(0, ets:lookup_element(kafka_metrics, last_recovered_at, 2)),
    ?assertEqual(0, ets:lookup_element(kafka_metrics, reconnect_attempts, 2)),
    ok.

%% @doc schedule_probe/1 should send 'probe_kafka' message after the interval.
t_schedule_probe(_Config) ->
    emqx_plugin_kafka_client_srv:schedule_probe(50),
    receive
        probe_kafka ->
            ok
    after
        1000 ->
            ?assert(false)
    end,
    ok.

%% @doc mark_kafka_down/1 should open circuit breaker and update metrics.
t_mark_kafka_down(_Config) ->
    %% Setup: create ETS tables as init_tables would
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    ets:insert(kafka_circuit_breaker, [{state, closed}, {failure_count, 0}, {opened_at, 0}]),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    State = #{},
    emqx_plugin_kafka_client_srv:mark_kafka_down(State),
    ?assertEqual(open, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    ?assertEqual(down, ets:lookup_element(kafka_metrics, kafka_status, 2)),
    ?assertEqual(1, ets:lookup_element(kafka_metrics, kafka_down_count, 2)),
    Now = erlang:system_time(millisecond),
    LastDown = ets:lookup_element(kafka_metrics, last_down_at, 2),
    ?assert(Now - LastDown < 5000),
    ok.

%% @doc mark_kafka_up/1 should close circuit breaker and update metrics.
t_mark_kafka_up(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    ets:insert(kafka_circuit_breaker, [{state, open}, {failure_count, 5}, {opened_at, 1}]),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    ets:insert(kafka_metrics, {kafka_status, down}),
    State = #{},
    emqx_plugin_kafka_client_srv:mark_kafka_up(State),
    ?assertEqual(closed, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    ?assertEqual(0, ets:lookup_element(kafka_circuit_breaker, failure_count, 2)),
    ?assertEqual(up, ets:lookup_element(kafka_metrics, kafka_status, 2)),
    Now = erlang:system_time(millisecond),
    LastRecovered = ets:lookup_element(kafka_metrics, last_recovered_at, 2),
    ?assert(Now - LastRecovered < 5000),
    ok.

%% @doc monitor_one/1 should monitor the brod client PID and return {ClientId, Ref}.
t_monitor_one(_Config) ->
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    {client1, Ref} = emqx_plugin_kafka_client_srv:monitor_one(client1),
    ?assert(is_reference(Ref)),
    erlang:demonitor(Ref),
    ok.

%% @doc monitor_clients/1 should monitor all clients and return list of {ClientId, Ref}.
t_monitor_clients(_Config) ->
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    Monitors = emqx_plugin_kafka_client_srv:monitor_clients([client1, client2, client3]),
    ?assertEqual(3, length(Monitors)),
    [?assertEqual(true, is_tuple(M) andalso size(M) == 2) || M <- Monitors],
    [erlang:demonitor(Ref) || {_, Ref} <- Monitors],
    ok.

%% @doc demonitor_all/1 should remove all monitors.
t_demonitor_all(_Config) ->
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    Monitors = emqx_plugin_kafka_client_srv:monitor_clients([client1, client2]),
    emqx_plugin_kafka_client_srv:demonitor_all(Monitors),
    ok.

%% @doc do_probe/1 should return ok when brod:get_partitions_count succeeds.
t_do_probe_success(_Config) ->
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    meck:expect(brod, get_partitions_count, fun(_Client, _Topic) -> {ok, 3} end),
    ?assertEqual(ok, emqx_plugin_kafka_client_srv:do_probe(client1)).

%% @doc do_probe/1 should return {error, Reason} when brod:get_partitions_count fails.
t_do_probe_failure(_Config) ->
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    meck:expect(brod, get_partitions_count, fun(_Client, _Topic) -> {error, no_leader} end),
    Result = emqx_plugin_kafka_client_srv:do_probe(client1),
    ?assertMatch({error, _}, Result).

%% @doc probe_kafka/1 should transition from down to up when probe succeeds.
t_probe_kafka_down_to_up(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [self()] end),
    meck:expect(brod, get_partitions_count, fun(_Client, _Topic) -> {ok, 3} end),
    DownState = emqx_plugin_kafka_client_srv:make_test_state(#{kafka_status => down}),
    NewState = emqx_plugin_kafka_client_srv:probe_kafka(DownState),
    ?assertEqual(up, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ?assertEqual(closed, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    ok.

%% @doc restart_client/2 should restart client and producer, returning {ok, ClientId}.
t_restart_client_success(_Config) ->
    meck:expect(brod, stop_client, fun(_ClientId) -> ok end),
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> ok end),
    meck:expect(brod, start_producer, fun(_ClientId, _Topic, _Opts) -> ok end),
    meck:expect(brod, get_partitions_count, fun(_ClientId, _Topic) -> {ok, 3} end),
    Result = emqx_plugin_kafka_client_srv:restart_client(client1, <<"test-topic">>),
    ?assertEqual({ok, client1}, Result).

%% @doc restart_client/2 should return {error, Reason} when brod:start_client fails.
t_restart_client_failure(_Config) ->
    meck:expect(brod, stop_client, fun(_ClientId) -> ok end),
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> {error, no_leader} end),
    Result = emqx_plugin_kafka_client_srv:restart_client(client1, <<"test-topic">>),
    ?assertEqual({error, no_leader}, Result).

%% @doc restart_client/2 should return {error, Reason} when brod:start_producer fails.
t_restart_client_producer_failure(_Config) ->
    meck:expect(brod, stop_client, fun(_ClientId) -> ok end),
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> ok end),
    meck:expect(brod, start_producer, fun(_ClientId, _Topic, _Opts) ->
        {error, producer_down}
    end),
    Result = emqx_plugin_kafka_client_srv:restart_client(client1, <<"test-topic">>),
    ?assertEqual({error, producer_down}, Result).
