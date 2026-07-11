# Kafka Health Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add health monitoring and auto-reconnect to `emqx_plugin_kafka_client_srv` so Kafka broker outages or brod producer crashes trigger immediate circuit-breaker opening, periodic probe-based recovery, and automatic client restart.

**Architecture:** Extend the existing `emqx_plugin_kafka_client_srv` gen_server with `trap_exit`, `erlang:monitor/2` for client PIDs, and a periodic probe timer. On Kafka-down, write `open` to the existing `kafka_circuit_breaker` ETS table. On recovery, write `closed`. No new modules.

**Tech Stack:** Erlang/OTP, brod 3.16.4, Common Test (CT), meck 0.9.2 for mocking, EMQX v5.0.3 plugin framework.

**Spec:** [2026-07-11-kafka-health-monitoring-design.md](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/docs/superpowers/specs/2026-07-11-kafka-health-monitoring-design.md)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `rebar.config` | Modify | Add `test` profile with meck dependency |
| `src/emqx_plugin_kafka.erl` | Modify | Export `init_tables/0` (1-line change) |
| `src/emqx_plugin_kafka_client_srv.erl` | Modify | Add trap_exit, monitor, probe, 13 new methods |
| `test/emqx_plugin_kafka_client_srv_SUITE.erl` | Create | CT suite for client_srv health monitoring |
| `test/emqx_plugin_kafka_health_metrics_SUITE.erl` | Create | CT suite for ETS metrics & circuit breaker integration |

---

### Task 1: Add test profile with meck dependency

**Files:**
- Modify: `rebar.config`

- [ ] **Step 1: Add test profile to rebar.config**

Add the following lines to the end of `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/rebar.config`:

```erlang
{profiles, [
  {test, [
    {deps, [
      {meck, "0.9.2"}
    ]}
  ]}
]}.

{ct_opts, [{enable_builtin_hooks, false}]}.
```

- [ ] **Step 2: Fetch dependencies and verify meck loads**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test deps`
Expected: completes without errors; `_build/test/lib/meck/` exists.

- [ ] **Step 3: Verify CT runs with empty suite list**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct -v`
Expected: `0 tests, 0 failures` (no test dir yet, but CT machinery works).

- [ ] **Step 4: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add rebar.config
git commit -m "build: add test profile with meck dependency"
```

---

### Task 2: Export init_tables/0 from emqx_plugin_kafka.erl

**Files:**
- Modify: `src/emqx_plugin_kafka.erl:40-43`

- [ ] **Step 1: Add init_tables/0 to the export list**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka.erl`, modify the first `-export` block (lines 40-43) from:

```erlang
-export([ load/1
        , unload/0
        , get_metrics/0
        ]).
```

to:

```erlang
-export([ load/1
        , unload/0
        , get_metrics/0
        , init_tables/0
        ]).
```

- [ ] **Step 2: Verify compilation**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 compile`
Expected: compiles without warnings or errors.

- [ ] **Step 3: Verify xref passes**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 xref`
Expected: no undefined function calls.

- [ ] **Step 4: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add src/emqx_plugin_kafka.erl
git commit -m "feat: export init_tables/0 for cross-module use"
```

---

### Task 3: Create test suite skeleton with init_per_suite

**Files:**
- Create: `test/emqx_plugin_kafka_client_srv_SUITE.erl`

- [ ] **Step 1: Create test directory**

Run: `mkdir -p /Users/lute/IdeaProjects/emqx-plugin-kafkav5/test`

- [ ] **Step 2: Write the test suite skeleton**

Create `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl` with:

```erlang
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
-export([ t_suite_loads/1 ]).

all() -> [t_suite_loads].

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
t_suite_loads(_Config) ->
    ?assertEqual(true, is_atom(emqx_plugin_kafka_client_srv:module_info())),
    ok.
```

- [ ] **Step 3: Run the smoke test and verify it passes**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE -v`
Expected: `1 tests, 0 failures`.

- [ ] **Step 4: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl
git commit -m "test: add client_srv CT suite skeleton with smoke test"
```

---

### Task 4: Extend state record and constants in client_srv

**Files:**
- Modify: `src/emqx_plugin_kafka_client_srv.erl:27-48`

- [ ] **Step 1: Add new constants after the existing ?SERVER define**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, replace lines 27-28:

```erlang
-define(TOPIC_PARTITIONS, topic_partitions).
-define(SERVER, ?MODULE).
```

with:

```erlang
-define(TOPIC_PARTITIONS, topic_partitions).
-define(SERVER, ?MODULE).
%% 熔断器 ETS 表名（与 emqx_plugin_kafka 共享）
-define(CB_TABLE, kafka_circuit_breaker).
%% 探测周期：15 秒，比 30 秒熔断冷却时间短，能在 half_open 之前主动恢复
-define(PROBE_INTERVAL_MS, 15000).
%% 单次探测超时：5 秒，略大于 Kafka 默认 network timeout
-define(PROBE_TIMEOUT_MS, 5000).
```

- [ ] **Step 2: Extend the #state record**

In the same file, replace the existing `-record(state, ...)` (lines 46-48):

```erlang
-record(state, { clients :: [atom()]
               , topics :: [binary()]
               }).
```

with:

```erlang
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
```

- [ ] **Step 3: Verify compilation**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 compile`
Expected: compiles. The `init/1` function will fail to match the new record — that's expected, we'll fix it in Task 11.

Note: If compilation fails due to `init/1` returning the old record shape, temporarily change the `init/1` return to use the new fields with `undefined` placeholders:

```erlang
    {ok, #state{clients = Clients, topics = Topics,
                kafka_status = up, down_since = undefined,
                probe_interval = ?PROBE_INTERVAL_MS, monitors = []}}.
```

- [ ] **Step 4: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: extend client_srv state record with health monitoring fields"
```

---

### Task 5: Implement init_health_metrics/0 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing test for init_health_metrics**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add `t_init_health_metrics` to the `all/0` list and export:

```erlang
-export([ t_suite_loads/1
        , t_init_health_metrics/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics].
```

Then add the test case function (after `t_suite_loads/1`):

```erlang
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_init_health_metrics -v`
Expected: FAIL with `undefined function emqx_plugin_kafka_client_srv:init_health_metrics/0`

- [ ] **Step 3: Implement init_health_metrics/0**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append the following function after the existing `translate/1` function (at the end of the file, after line 216):

```erlang

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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_init_health_metrics -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement init_health_metrics/0 with idempotent ETS init"
```

---

### Task 6: Implement schedule_probe/1 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing test for schedule_probe**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add `t_schedule_probe` to exports and `all/0`:

```erlang
-export([ t_suite_loads/1
        , t_init_health_metrics/1
        , t_schedule_probe/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe].
```

Add the test case:

```erlang
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_schedule_probe -v`
Expected: FAIL with `undefined function emqx_plugin_kafka_client_srv:schedule_probe/1`

- [ ] **Step 3: Implement schedule_probe/1**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after the `init_health_metrics/0` function:

```erlang

%% @doc 调度下一次 Kafka 健康探测。
%% 通过 erlang:send_after/3 发送 atom 消息 'probe_kafka' 到本进程。
-spec schedule_probe(integer()) -> ok.
schedule_probe(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), probe_kafka),
    ok.
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_schedule_probe -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement schedule_probe/1 for periodic health checks"
```

---

### Task 7: Implement mark_kafka_down/1 and mark_kafka_up/1 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing tests for mark_kafka_down and mark_kafka_up**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
-export([ t_suite_loads/1
        , t_init_health_metrics/1
        , t_schedule_probe/1
        , t_mark_kafka_down/1
        , t_mark_kafka_up/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up].
```

Add the test cases:

```erlang
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_mark_kafka_down --case=t_mark_kafka_up -v`
Expected: FAIL with `undefined function mark_kafka_down/1` and `mark_kafka_up/1`

- [ ] **Step 3: Implement mark_kafka_down/1 and mark_kafka_up/1**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `schedule_probe/1`:

```erlang

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
```

- [ ] **Step 4: Export the new functions for testing**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, update the API export block (lines 31-34) from:

```erlang
%% API
-export([ start_link/1
        , get_partition/1
        , stop_clients/0
        ]).
```

to:

```erlang
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
        ]).
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_mark_kafka_down --case=t_mark_kafka_up -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement mark_kafka_down/1 and mark_kafka_up/1 with circuit breaker integration"
```

---

### Task 8: Implement monitor_clients/1, monitor_one/1, demonitor_all/1 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing tests for monitor functions**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
-export([ t_suite_loads/1
        , t_init_health_metrics/1
        , t_schedule_probe/1
        , t_mark_kafka_down/1
        , t_mark_kafka_up/1
        , t_monitor_one/1
        , t_monitor_clients/1
        , t_demonitor_all/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all].
```

Add the test cases:

```erlang
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_monitor_one --case=t_monitor_clients --case=t_demonitor_all -v`
Expected: FAIL with `undefined function monitor_one/1`, `monitor_clients/1`, `demonitor_all/1`

- [ ] **Step 3: Implement the three monitor functions**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `mark_kafka_up/1`:

```erlang

%% @doc 批量监控所有 client 进程，返回 {ClientId, MonitorRef} 列表。
-spec monitor_clients([atom()]) -> [{atom(), reference()}].
monitor_clients(Clients) ->
    lists:filtermap(fun monitor_one/1, Clients).

%% @doc 监控单个 brod client 进程。
%% 通过 brod_sup:find_client/1 获取 PID，调用 erlang:monitor/2。
%% 返回 {true, {ClientId, Ref}} 或 false（若 client 未运行）。
-spec monitor_one(atom()) -> {true, {atom(), reference()}} | false.
monitor_one(ClientId) ->
    case brod_sup:find_client(ClientId) of
        [Pid] when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            {true, {ClientId, Ref}};
        [] ->
            false
    end.

%% @doc 清除所有 monitor 引用。
-spec demonitor_all([{atom(), reference()}]) -> ok.
demonitor_all(Monitors) ->
    lists:foreach(fun({_, Ref}) -> erlang:demonitor(Ref, [flush]) end, Monitors),
    ok.
```

- [ ] **Step 4: Export the new functions**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, update the health monitoring export block to include the new functions:

```erlang
%% Health monitoring (exported for testing and internal use)
-export([ init_health_metrics/0
        , schedule_probe/1
        , mark_kafka_down/1
        , mark_kafka_up/1
        , monitor_clients/1
        , monitor_one/1
        , demonitor_all/1
        ]).
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_monitor_one --case=t_monitor_clients --case=t_demonitor_all -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement monitor_clients/1, monitor_one/1, demonitor_all/1"
```

---

### Task 9: Implement do_probe/1 and probe_kafka/1 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing tests for do_probe and probe_kafka**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
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
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up].
```

Add the test cases:

```erlang
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
```

Note: `make_test_state/1` is a test helper we'll add to the module. Alternatively, use a record accessor. Since `#state{}` is module-private, we expose `make_test_state/1`:

```erlang
%% @doc Test helper: create a #state{} from a map of field overrides.
-spec make_test_state(map()) -> #state{}.
make_test_state(Overrides) ->
    Base = #state{clients = [], topics = [],
                  kafka_status = up, down_since = undefined,
                  probe_interval = ?PROBE_INTERVAL_MS, monitors = []},
    maps:fold(fun(kafka_status, V, S) -> S#state{kafka_status = V};
                 (down_since, V, S) -> S#state{down_since = V};
                 (probe_interval, V, S) -> S#state{probe_interval = V};
                 (_, _, S) -> S
              end, Base, Overrides).
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_do_probe_success --case=t_do_probe_failure --case=t_probe_kafka_down_to_up -v`
Expected: FAIL with `undefined function do_probe/1`, `probe_kafka/1`, `make_test_state/1`

- [ ] **Step 3: Implement do_probe/1, probe_kafka/1, and make_test_state/1**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `demonitor_all/1`:

```erlang

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
```

- [ ] **Step 4: Export the new functions**

Update the health monitoring export block:

```erlang
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
        ]).
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_do_probe_success --case=t_do_probe_failure --case=t_probe_kafka_down_to_up -v`
Expected: PASS

- [ ] **Step 5b: Add get_kafka_status/1 accessor**

Since `#state{}` is module-private, tests need an accessor. In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `first_client/1`:

```erlang

%% @doc 测试辅助：从 state 读取 kafka_status 字段。
-spec get_kafka_status(#state{}) -> up | down.
get_kafka_status(State) ->
    State#state.kafka_status.
```

Update the export block to include it:

```erlang
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
        ]).
```

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement probe_kafka/1 and do_probe/1 with state transitions"
```

---

### Task 10: Implement restart_client/2 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing test for restart_client**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
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
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure].
```

Add the test cases:

```erlang
%% @doc restart_client/2 should restart client and producer, returning {ok, ClientId}.
t_restart_client_success(_Config) ->
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> ok end),
    meck:expect(brod, start_producer, fun(_ClientId, _Topic, _Opts) -> ok end),
    meck:expect(brod, get_partitions_count, fun(_ClientId, _Topic) -> {ok, 3} end),
    Result = emqx_plugin_kafka_client_srv:restart_client(client1, <<"test-topic">>),
    ?assertEqual({ok, client1}, Result).

%% @doc restart_client/2 should return {error, Reason} when brod:start_client fails.
t_restart_client_failure(_Config) ->
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> {error, no_leader} end),
    Result = emqx_plugin_kafka_client_srv:restart_client(client1, <<"test-topic">>),
    ?assertMatch({error, _}, Result).
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_restart_client_success --case=t_restart_client_failure -v`
Expected: FAIL with `undefined function restart_client/2`

- [ ] **Step 3: Implement restart_client/2**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `first_client/1`:

```erlang

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
                    get_topic_partitions(ClientId, Topic),
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
-spec get_address_list() -> [{string(), integer()}].
get_address_list() ->
    case persistent_term:get({emqx_plugin_kafka, kafka_config}, undefined) of
        undefined -> [{"localhost", 9092}];
        Env -> translate(maps:get(address_list, Env))
    end.
```

- [ ] **Step 4: Export the new function**

Update the export block:

```erlang
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
        ]).
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_restart_client_success --case=t_restart_client_failure -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement restart_client/2 for client crash recovery"
```

---

### Task 11: Implement handle_client_down/3 and handle_producer_exit/3 (TDD)

**Files:**
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`
- Modify: `src/emqx_plugin_kafka_client_srv.erl`

- [ ] **Step 1: Write failing tests for handle_client_down and handle_producer_exit**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
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
        , t_handle_client_down/1
        , t_handle_producer_exit/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure,
          t_handle_client_down, t_handle_producer_exit].
```

Add the test cases:

```erlang
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
    ok.

%% @doc handle_producer_exit/3 should mark Kafka down on producer crash.
t_handle_producer_exit(_Config) ->
    ets:new(kafka_circuit_breaker, [named_table, public, set]),
    ets:new(kafka_metrics, [named_table, public, set]),
    emqx_plugin_kafka:init_tables(),
    emqx_plugin_kafka_client_srv:init_health_metrics(),
    State = emqx_plugin_kafka_client_srv:make_test_state(
              #{clients => [client1], topics => [<<"t1">>]}),
    NewState = emqx_plugin_kafka_client_srv:handle_producer_exit(
                  self(), {reached_max_retries, no_leader_connection}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ?assertEqual(open, ets:lookup_element(kafka_circuit_breaker, state, 2)),
    ok.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_handle_client_down --case=t_handle_producer_exit -v`
Expected: FAIL with `undefined function handle_client_down/3`, `handle_producer_exit/3`

- [ ] **Step 3: Implement handle_client_down/3 and handle_producer_exit/3**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `get_address_list/0`:

```erlang

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
```

- [ ] **Step 4: Export the new functions**

Update the export block:

```erlang
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
        ]).
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_handle_client_down --case=t_handle_producer_exit -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: implement handle_client_down/3 and handle_producer_exit/3"
```

---

### Task 12: Wire up init/1 with trap_exit, monitor, and probe

**Files:**
- Modify: `src/emqx_plugin_kafka_client_srv.erl:77-91`
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`

- [ ] **Step 1: Write failing test for init with health monitoring**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add `t_init_health_monitoring` to exports and `all/0`:

```erlang
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
        , t_handle_client_down/1
        , t_handle_producer_exit/1
        , t_init_health_monitoring/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure,
          t_handle_client_down, t_handle_producer_exit,
          t_init_health_monitoring].
```

Add the test case:

```erlang
%% @doc init/1 should enable trap_exit, create ETS, init metrics, and schedule probe.
t_init_health_monitoring(_Config) ->
    meck:expect(brod, start_client, fun(_Addrs, _ClientId) -> ok end),
    meck:expect(brod, start_producer, fun(_ClientId, _Topic, _Opts) -> ok end),
    meck:expect(brod, get_partitions_count, fun(_ClientId, _Topic) -> {ok, 3} end),
    meck:expect(brod_sup, find_client, fun(_ClientId) -> [] end),
    Env = #{address_list => <<"localhost:9092">>,
            topic_low => <<"t-low">>, topic_medium => <<"t-med">>, topic_high => <<"t-high">>},
    %% Pre-create ETS tables as emqx_plugin_kafka:load/1 would
    catch ets:new(kafka_circuit_breaker, [named_table, public, set]),
    catch ets:new(kafka_metrics, [named_table, public, set]),
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_init_health_monitoring -v`
Expected: FAIL (probe message never arrives, or process flag not set)

- [ ] **Step 3: Modify init/1 to wire up health monitoring**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, replace the existing `init/1` function (lines 77-91):

```erlang
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
```

with:

```erlang
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_init_health_monitoring -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: wire up init/1 with trap_exit, monitor, and probe scheduling"
```

---

### Task 13: Modify handle_info/2 with new clauses

**Files:**
- Modify: `src/emqx_plugin_kafka_client_srv.erl:108-110`
- Modify: `test/emqx_plugin_kafka_client_srv_SUITE.erl`

- [ ] **Step 1: Write failing tests for handle_info clauses**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/test/emqx_plugin_kafka_client_srv_SUITE.erl`, add to exports and `all/0`:

```erlang
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
        , t_handle_client_down/1
        , t_handle_producer_exit/1
        , t_init_health_monitoring/1
        , t_handle_info_probe_kafka/1
        , t_handle_info_exit/1
        ]).

all() -> [t_suite_loads, t_init_health_metrics, t_schedule_probe,
          t_mark_kafka_down, t_mark_kafka_up,
          t_monitor_one, t_monitor_clients, t_demonitor_all,
          t_do_probe_success, t_do_probe_failure, t_probe_kafka_down_to_up,
          t_restart_client_success, t_restart_client_failure,
          t_handle_client_down, t_handle_producer_exit,
          t_init_health_monitoring,
          t_handle_info_probe_kafka, t_handle_info_exit].
```

Add the test cases:

```erlang
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
    emqx_plugin_kafka_client_srv:handle_info(probe_kafka, State),
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
    NewState = emqx_plugin_kafka_client_srv:handle_info(
                  {'EXIT', self(), {reached_max_retries, no_leader_connection}}, State),
    ?assertEqual(down, emqx_plugin_kafka_client_srv:get_kafka_status(NewState)),
    ok.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_handle_info_probe_kafka --case=t_handle_info_exit -v`
Expected: FAIL (current `handle_info` ignores all messages)

- [ ] **Step 3: Modify handle_info/2 to add new clauses**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, replace the existing `handle_info/2` (lines 108-110):

```erlang
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.
```

with:

```erlang
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(probe_kafka, State) ->
    NewState = probe_kafka(State),
    schedule_probe(State#state.probe_interval),
    {noreply, NewState};
handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
    NewState = handle_down_by_ref(Ref, Reason, State),
    {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, State) ->
    NewState = handle_producer_exit(Pid, Reason, State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.
```

- [ ] **Step 4: Implement handle_down_by_ref/3 helper**

In `/Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_client_srv.erl`, append after `handle_producer_exit/3`:

```erlang

%% @doc 通过 MonitorRef 查找对应的 ClientId 并处理崩溃。
-spec handle_down_by_ref(reference(), term(), #state{}) -> #state{}.
handle_down_by_ref(Ref, Reason, State) ->
    case lists:keyfind(Ref, 2, State#state.monitors) of
        {ClientId, Ref} ->
            NewMonitors = lists:delete({ClientId, Ref}, State#state.monitors),
            handle_client_down(ClientId, Reason,
                                State#state{monitors = NewMonitors});
        false ->
            logger:debug("[KAFKA PLUGIN]Unknown monitor ref down: ~p", [Ref]),
            State
    end.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct --suite=emqx_plugin_kafka_client_srv_SUITE --case=t_handle_info_probe_kafka --case=t_handle_info_exit -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add test/emqx_plugin_kafka_client_srv_SUITE.erl src/emqx_plugin_kafka_client_srv.erl
git commit -m "feat: add handle_info clauses for probe_kafka, DOWN, and EXIT messages"
```

---

### Task 14: Run full test suite and verify all passes

**Files:**
- None (verification only)

- [ ] **Step 1: Run the complete CT suite**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 as test ct -v`
Expected: All 18 tests pass, 0 failures.

- [ ] **Step 2: Run xref to verify no undefined functions**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 xref`
Expected: no undefined function calls, no warnings.

- [ ] **Step 3: Verify compilation without test profile**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && ./rebar3 compile`
Expected: compiles cleanly.

- [ ] **Step 4: Final commit (if any cleanup needed)**

If any files were modified for cleanup:

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add -A
git commit -m "chore: cleanup after full test suite run"
```

---

### Task 15: Integration test in EMQX Docker container

**Files:**
- None (manual verification)

- [ ] **Step 1: Build the plugin release**

Run: `cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5 && make rel`
Expected: `emqx_plugin_kafka-5.0.0.tar.gz` created.

- [ ] **Step 2: Deploy to EMQX Docker container**

Copy the release to the EMQX container's plugins directory and extract.

- [ ] **Step 3: Start the plugin**

Run: `docker exec emqx emqx_ctl plugins start emqx_plugin_kafka-5.0.0`
Expected: `{"result":"ok"}`

- [ ] **Step 4: Verify health monitoring is active**

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_metrics, kafka_status).'`
Expected: `[{kafka_status, up}]`

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_circuit_breaker, state).'`
Expected: `[{state, closed}]` (if Kafka is reachable) or `[{state, open}]` (if not)

- [ ] **Step 5: Test Kafka-down scenario**

Stop the Kafka broker (or make it unreachable).

Wait 15-30 seconds for probe to detect.

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_metrics, kafka_status).'`
Expected: `[{kafka_status, down}]`

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_circuit_breaker, state).'`
Expected: `[{state, open}]`

- [ ] **Step 6: Test Kafka-recovery scenario**

Restart the Kafka broker.

Wait 15-30 seconds for probe to detect recovery.

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_metrics, kafka_status).'`
Expected: `[{kafka_status, up}]`

Run: `docker exec emqx emqx eval 'ets:lookup(kafka_circuit_breaker, state).'`
Expected: `[{state, closed}]`

- [ ] **Step 7: Document test results and commit**

Create a brief test report in the commit message:

```bash
cd /Users/lute/IdeaProjects/emqx-plugin-kafkav5
git add -A
git commit -m "test: verify Kafka health monitoring in EMQX Docker container

- Plugin starts successfully with health monitoring active
- Kafka-down scenario: circuit breaker opens within 15-30s
- Kafka-recovery scenario: circuit breaker closes within 15-30s
- All metrics ETS entries created and updated correctly"
```

---

## Self-Review

**Spec coverage check:**
- ✅ Architecture: trap_exit + monitor + periodic probe (Tasks 12, 13)
- ✅ State record: 4 new fields (Task 4)
- ✅ Circuit breaker integration: mark_kafka_down/mark_kafka_up (Task 7)
- ✅ Metrics: 5 new ETS entries (Task 5)
- ✅ 13 new methods (Tasks 5-13: init_health_metrics, schedule_probe, mark_kafka_down, mark_kafka_up, monitor_clients, monitor_one, demonitor_all, do_probe, probe_kafka, make_test_state, get_kafka_status, restart_client, get_address_list, handle_client_down, handle_producer_exit, handle_down_by_ref = 16 total, within 100 limit)
- ✅ init_tables export (Task 2)
- ✅ ETS creation order fix (Task 12, step 3)
- ✅ Constants: ?CB_TABLE, ?PROBE_INTERVAL_MS, ?PROBE_TIMEOUT_MS (Task 4)

**Placeholder scan:** No TBD, TODO, or placeholder text. All steps have complete code.

**Type consistency check:**
- `mark_kafka_down(term())` — used in tests as `mark_kafka_down(State)` where State is `#{}` (map). The function ignores the argument, so this works. In production, called with `#state{}` record. ✅
- `probe_kafka(#state{})` — consistently uses the state record. ✅
- `make_test_state/1` returns `#state{}` — used in tests to construct state. ✅
- `get_kafka_status/1` takes `#state{}` — used in tests. ✅
- `restart_client(atom(), binary())` — called with `client1` and `<<"test-topic">>`. ✅
- `handle_client_down(atom(), term(), #state{})` — called with `client1, {exit, crashed}, State`. ✅
- `handle_producer_exit(pid(), term(), #state{})` — called with `self(), Reason, State`. ✅

**Note on method count:** The spec said 13 new methods, but the actual implementation has 16 (added `make_test_state/1`, `get_kafka_status/1`, `get_address_list/0`, `handle_down_by_ref/3`, `probe_topic/1`, `first_client/1`, `handle_probe_success/1`, `handle_probe_failure/1` as helpers). Total methods: 18 (existing) + 16 (new) = 34. Still well within the 100 limit.

All checks pass. Plan is complete.
