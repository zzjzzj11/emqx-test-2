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
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up].

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
