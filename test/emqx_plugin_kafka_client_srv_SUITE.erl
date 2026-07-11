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
        , t_handle_client_down/1
        , t_handle_producer_exit/1
        , t_init_health_monitoring/1
        , t_handle_info_probe_kafka/1
        , t_handle_info_exit/1
        , t_handle_info_down/1
        , t_handle_producer_exit_normal/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure,
          t_restart_client_producer_failure,
          t_handle_client_down, t_handle_producer_exit,
          t_init_health_monitoring,
          t_handle_info_probe_kafka, t_handle_info_exit,
          t_handle_info_down, t_handle_producer_exit_normal].

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

%% @doc handle_client_down/3 should mark Kafka down when client crash detected.
t_handle_client_down(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>]}),
    NewState = emqx_plugin_kafka_client_srv:handle_client_down(
                  client1, {exit, crashed}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ?assertEqual(open, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    %% Idempotency: calling again when already down should be a no-op
    DownCount1 = ets:lookup_element(kafka_metrics, kafka_down_count, 2),
    UnchangedState = emqx_plugin_kafka_client_srv:handle_client_down(
                       client1, {exit, crashed}, NewState),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(UnchangedState)),
    ?assertEqual(DownCount1, ets:lookup_element(kafka_metrics, kafka_down_count, 2)),
    ok.

%% @doc handle_producer_exit/3 should mark Kafka down on producer crash and clear monitors.
t_handle_producer_exit(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    TestRef = make_ref(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>],
                monitors => [{client1, TestRef}]}),
    ?assertEqual([{client1, TestRef}],
                 emqx_plugin_kafka_client_srv:get_monitors(State)),
    NewState = emqx_plugin_kafka_client_srv:handle_producer_exit(
                  self(), {reached_max_retries, no_leader_connection}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ?assertEqual(open, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    ?assertEqual([], emqx_plugin_kafka_client_srv:get_monitors(NewState)),
    %% Idempotency: calling again when already down should be a no-op
    DownCount1 = ets:lookup_element(kafka_metrics, kafka_down_count, 2),
    UnchangedState = emqx_plugin_kafka_client_srv:handle_producer_exit(
                       self(), {reached_max_retries, no_leader_connection}, NewState),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(UnchangedState)),
    ?assertEqual(DownCount1, ets:lookup_element(kafka_metrics, kafka_down_count, 2)),
    ok.

%% @doc init/1 should enable trap_exit, create ETS, init metrics, and schedule probe.
t_init_health_monitoring(_Config) ->
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> ok end),
    meck:expect(brod, start_producer, fun(_ClientId, _Topic, _Opts) -> ok end),
    meck:expect(brod, get_partitions_count, fun(_ClientId, _Topic) -> {ok, 3} end),
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [] end),
    meck:expect(brod, stop_client, fun(_ClientId) -> ok end),
    Env = #{address_list => <<"localhost:9092">>,
            topic_low => <<"t-low">>, topic_medium => <<"t-med">>, topic_high => <<"t-high">>},
    emqx_plugin_kafka:init_tables(),
    {ok, Pid} = emqx_plugin_kafka_client_srv:start_link(Env),
    %% Verify process is alive
    ?assertEqual(true, is_process_alive(Pid)),
    %% Verify ETS metrics were initialized
    ?assertEqual(up, ets:lookup_element(kafka_metrics, kafka_status, 2)),
    %% Verify trap_exit is enabled via process_info
    {trap_exit, TrapExit} = process_info(Pid, trap_exit),
    ?assertEqual(true, TrapExit),
    gen_server:stop(Pid),
    ok.

%% @doc handle_info(probe_kafka, State) should trigger probe and reschedule.
t_handle_info_probe_kafka(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    meck:expect(brod, get_partitions_count, fun(_Client, _Topic) -> {ok, 3} end),
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [] end),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>], probe_interval => 50}),
    {noreply, NewState} = emqx_plugin_kafka_client_srv:handle_info(probe_kafka, State),
    %% Verify probe was executed (status transitions to down because brod_sup:find_client returns [])
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    %% Verify probe rescheduled (message arrives in test process mailbox)
    receive
        probe_kafka -> ok
    after
        1000 -> ?assert(false)
    end,
    ok.

%% @doc handle_info({'EXIT', Pid, Reason}, State) should mark Kafka down.
t_handle_info_exit(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>]}),
    {noreply, NewState} = emqx_plugin_kafka_client_srv:handle_info(
                  {'EXIT', self(), {reached_max_retries, no_leader_connection}}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ok.

%% @doc handle_info({'DOWN', Ref, ...}, State) should mark Kafka down and clear monitors.
t_handle_info_down(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    TestRef = make_ref(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>],
                monitors => [{client1, TestRef}]}),
    {noreply, NewState} = emqx_plugin_kafka_client_srv:handle_info(
                  {'DOWN', TestRef, process, self(), crashed}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ?assertEqual([], emqx_plugin_kafka_client_srv:get_monitors(NewState)),
    ok.

%% @doc handle_producer_exit/3 should ignore normal/shutdown exit reasons.
t_handle_producer_exit_normal(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>]}),
    %% normal exit should not change state
    State1 = emqx_plugin_kafka_client_srv:handle_producer_exit(self(), normal, State),
    ?assertEqual(up, emqx_plugin_kafka_client_srv:get_kafka_status(State1)),
    %% shutdown exit should not change state
    State2 = emqx_plugin_kafka_client_srv:handle_producer_exit(self(), shutdown, State1),
    ?assertEqual(up, emqx_plugin_kafka_client_srv:get_kafka_status(State2)),
    %% {shutdown, Reason} exit should not change state
    State3 = emqx_plugin_kafka_client_srv:handle_producer_exit(self(), {shutdown, test}, State2),
    ?assertEqual(up, emqx_plugin_kafka_client_srv:get_kafka_status(State3)),
    ok.
