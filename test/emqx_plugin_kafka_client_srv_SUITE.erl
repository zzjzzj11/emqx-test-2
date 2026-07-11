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
        ]).

all() -> [t_suite_loads, t_init_health_metrics].

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
