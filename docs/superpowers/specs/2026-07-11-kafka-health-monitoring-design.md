# Kafka 健康监控与自愈设计（Design）

**日期**: 2026-07-11
**状态**: Approved (brainstorming 已完成，待实现)
**触发问题**: `brod_producer` 崩溃 `{reached_max_retries, no_leader_connection}`，导致 `client2` producer 进程退出且无法自愈。

## Why（动机）

当前 `emqx_plugin_kafka_client_srv` 是一个简单的 gen_server，仅负责启动 brod client/producer 和持有 `topic_partitions` ETS 表。它存在以下关键缺陷：

1. **不捕获 EXIT**：`trap_exit` 未开启，brod producer 崩溃（如 `no_leader_connection`）时，进程退出但 `client_srv` 完全不知情，不会触发重启。
2. **不监控子进程**：brod client/producer 由 `brod_sup` 监督而非本插件监督树，`client_srv` 持有 ClientId 引用但未对 client PID 调用 `erlang:monitor/2`。
3. **无 Kafka 健康探测**：Kafka broker 整体不可达时，插件持续向死掉的 producer 投递消息，每次都失败，最终触发熔断器但无法主动恢复。
4. **熔断器与客户端解耦**：熔断器在 [emqx_plugin_kafka.erl](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka.erl) 中以 per-message 方式更新，没有与 Kafka 整体健康状态联动 —— Kafka 集群宕机时，熔断器要累积 5 次失败才 open，期间消息仍在投递。

本设计目标：**让 `client_srv` 主动监控 Kafka 健康，发现故障时立即打开熔断器并停止客户端，定期探测恢复后自动重连并关闭熔断器**。

## Architecture Overview（架构概述）

**核心原则**：最小侵入。不引入新模块、不新增监督树层级，仅扩展 `emqx_plugin_kafka_client_srv` 一个 gen_server。

```
┌──────────────────────────────────────────────────────────────┐
│ emqx_plugin_kafka_client_srv (gen_server, trap_exit=true)   │
│                                                              │
│  State:                                                      │
│   - clients, topics (现有)                                   │
│   - kafka_status :: up | down                                │
│   - down_since  :: integer() | undefined                     │
│   - probe_interval :: integer()                              │
│   - monitors :: [{atom(), reference()}]                      │
│                                                              │
│  监控源:                                                     │
│   1. erlang:monitor(client Pid)  → client 崩溃               │
│   2. erlang:trap_exit()          → producer 崩溃             │
│   3. erlang:send_after(probe)   → 周期性主动探测            │
│                                                              │
│  写入 ETS:                                                   │
│   - kafka_circuit_breaker (现有表，open/closed)              │
│   - kafka_metrics (现有表，扩展)                             │
│   - topic_partitions (现有表，不变)                           │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
   brod_sup (外部监督树，不变)
        │
        ▼
   client1, client2, client3 + producers
```

**三种监控源各司其职**：
- **monitor**：捕获 client 进程崩溃（client 由 `brod_sup` 启动但 PID 可通过 `brod_sup:find_client/1` 获取）
- **trap_exit**：捕获链接到 `client_srv` 的 producer 进程崩溃（`brod:start_producer` 可能链接）
- **周期探测**：应对 Kafka broker 整体不可达但 client/producer 进程未崩溃的情况（最常见场景，即本次 crash 的根因）

## State and Data Flow（状态与数据流）

### State Record 扩展

```erlang
-record(state, { clients :: [atom()]              %% 现有
               , topics :: [binary()]             %% 现有
               , kafka_status :: up | down        %% 新增：Kafka 整体健康
               , down_since :: integer() | undefined  %% 新增：上次故障时间戳
               , probe_interval :: integer()      %% 新增：探测周期 ms
               , monitors :: [{atom(), reference()}]  %% 新增：client 监控引用
               }).
```

**属性计数**：6 个字段，符合用户规则「每个类的属性不超过 10 个」。

### 初始化流程（修改 init/1）

```
init(Env) ->
    ets:new(?TOPIC_PARTITIONS, ...),
    emqx_plugin_kafka:init_tables(),          %% 新增：幂等创建 CB/metrics 表
    %% 启动应用、NIF 检查（不变）
    process_flag(trap_exit, true),            %% 新增
    {Clients, Topics} = start_all_clients(Env),
    Monitors = monitor_clients(Clients),     %% 新增
    init_health_metrics(),                    %% 新增
    schedule_probe(?PROBE_INTERVAL_MS),       %% 新增
    {ok, #state{clients = Clients, topics = Topics,
                kafka_status = up, down_since = undefined,
                probe_interval = ?PROBE_INTERVAL_MS, monitors = Monitors}}.
```

### 故障检测流程

**场景 A：Kafka broker 不可达（无进程崩溃）**

```
probe_kafka 定时器触发
  ↓
brod:get_partitions_count(client1, Topic) 超时或失败
  ↓
mark_kafka_down(State)
  ├─ ets:insert(kafka_circuit_breaker, {state, open})
  ├─ ets:insert(kafka_circuit_breaker, {opened_at, Now})
  ├─ ets:insert(kafka_metrics, {kafka_status, down})
  ├─ ets:insert(kafka_metrics, {last_down_at, Now})
  ├─ ets:update_counter(kafka_metrics, kafka_down_count, 1)
  └─ 停止所有 client（避免持续重试消耗资源）
  ↓
schedule_probe(?PROBE_INTERVAL_MS)  %% 继续探测
```

**场景 B：client 进程崩溃（monitor 触发）**

```
{'DOWN', Ref, process, Pid, Reason}
  ↓
从 monitors 中找到对应 ClientId
  ↓
重启该 client：brod:start_client + brod:start_producer
  ├─ 成功 → 重新 monitor，记录 reconnect_attempts++
  └─ 失败 → mark_kafka_down(State)
```

**场景 C：producer 崩溃（trap_exit 触发）**

```
{'EXIT', Pid, Reason}
  ↓
日志记录 Reason（如 reached_max_retries, no_leader_connection）
  ↓
mark_kafka_down(State)  %% producer 崩溃通常意味着 Kafka 不可达
  ↓
停止所有 client 并等待下次 probe 周期重启
```

### 恢复流程

```
probe_kafka 定时器触发（kafka_status = down 时）
  ↓
尝试 brod:get_partitions_count(client1, Topic)
  ├─ 需要 client 还活着 → 如果已 stop，则先 brod:start_client
  ↓
成功 → mark_kafka_up(State)
  ├─ ets:insert(kafka_circuit_breaker, {state, closed})
  ├─ ets:insert(kafka_circuit_breaker, {failure_count, 0})
  ├─ ets:insert(kafka_metrics, {kafka_status, up})
  ├─ ets:insert(kafka_metrics, {last_recovered_at, Now})
  └─ 重新 monitor 所有 client
```

## Circuit Breaker Integration（熔断器集成）

**关键决策**：`client_srv` 直接写入 `kafka_circuit_breaker` ETS 表，**不修改** [emqx_plugin_kafka.erl](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka.erl) 中的熔断器代码。

### 现有熔断器逻辑（保持不变）

- `emqx_plugin_kafka:check_circuit/0` 读取 `kafka_circuit_breaker` 表的 `state` 字段
- `emqx_plugin_kafka:record_failure/0` 累积失败计数，达到阈值 5 时 open
- `emqx_plugin_kafka:maybe_half_open/0` 在 open 状态超过 30s 后转为 half_open

### client_srv 新增的写入

```erlang
%% 故障时立即 open（无需累积 5 次失败）
mark_kafka_down(_State) ->
    ets:insert(?CB_TABLE, [{state, open},
                           {opened_at, erlang:system_time(millisecond)}]),
    ok.

%% 恢复时立即 closed
mark_kafka_up(_State) ->
    ets:insert(?CB_TABLE, [{state, closed},
                          {failure_count, 0},
                          {opened_at, 0}]),
    ok.
```

**与现有逻辑的协同**：
- Kafka 宕机时，`client_srv` 立即 open 熔断器 —— 比 per-message 累积失败快 5 倍
- Kafka 恢复时，`client_srv` 立即 closed —— 无需等待 half_open 探测
- 现有 `record_failure/0` 和 `reset_failures/0` 逻辑保持不变，作为 per-message 层面的补充保护
- 两个写入路径都操作同一 ETS 表，原子性由 ETS 保证

**常量共享**：`?CB_TABLE` 在 [emqx_plugin_kafka.erl](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka.erl#L31) 中定义为 `kafka_circuit_breaker`。`client_srv` 需新增同样的宏定义 `-define(CB_TABLE, kafka_circuit_breaker).`。

## Metrics and Configuration（指标与配置）

### 新增 ETS 条目（写入 `kafka_metrics` 表）

| Key | 类型 | 说明 |
|-----|------|------|
| `kafka_status` | `up \| down` | Kafka 整体健康状态 |
| `kafka_down_count` | `integer()` | 累计故障次数 |
| `last_down_at` | `integer()` | 上次故障时间戳 (ms) |
| `last_recovered_at` | `integer()` | 上次恢复时间戳 (ms) |
| `reconnect_attempts` | `integer()` | 重连尝试次数 |

### 新增常量

```erlang
-define(PROBE_INTERVAL_MS, 15000).   %% 探测周期：15 秒
-define(PROBE_TIMEOUT_MS, 5000).     %% 单次探测超时：5 秒
```

**取值依据**：
- 15 秒探测周期：比 30 秒熔断冷却时间短，能在熔断器 half_open 之前主动恢复
- 5 秒超时：略大于 Kafka 默认 network timeout，避免误判

### 配置项（可选，不强制要求）

未来可从 `Env` 读取 `probe_interval` 和 `probe_timeout`，但本次实现使用常量以最小侵入。`init/1` 的 `Env` 参数签名不变。

## Method Changes（方法变更）

### 修改的现有方法（2 个）

1. **`init/1`** —— 增加 `process_flag(trap_exit, true)`、`monitor_clients/1`、`init_health_metrics/0`、`schedule_probe/1` 调用，State 字段扩展。预计 15 行（现 14 行）。
2. **`handle_info/2`** —— 新增 3 个子句：`{'DOWN', ...}`、`{'EXIT', ...}`、`probe_kafka`。预计 20 行（现 3 行）。

### 新增方法（11 个，全部追加到文件末尾）

| # | 方法名 | 参数 | 行数 | 复杂度 | 说明 |
|---|--------|------|------|--------|------|
| 1 | `monitor_clients/1` | `Clients` | 5 | 2 | 对 client 列表批量 monitor |
| 2 | `monitor_one/1` | `ClientId` | 8 | 3 | 查找 client PID 并 monitor |
| 3 | `demonitor_all/1` | `Monitors` | 5 | 2 | 清除所有 monitor 引用 |
| 4 | `schedule_probe/1` | `IntervalMs` | 3 | 1 | 定时器调度 |
| 5 | `probe_kafka/1` | `State` | 15 | 4 | 执行探测并返回新 State |
| 6 | `do_probe/1` | `ClientId` | 8 | 3 | 单次 brod:get_partitions_count |
| 7 | `mark_kafka_down/1` | `State` | 12 | 3 | 标记故障，open 熔断器 |
| 8 | `mark_kafka_up/1` | `State` | 12 | 3 | 标记恢复，close 熔断器 |
| 9 | `init_health_metrics/0` | 无 | 8 | 2 | 初始化新增 ETS 条目 |
| 10 | `restart_client/2` | `ClientId, Topic` | 12 | 4 | 重启单个 client+producer |
| 11 | `handle_client_down/3` | `ClientId, Reason, State` | 15 | 5 | 处理 client 崩溃并决策 |

**方法计数**：
- 现有方法：18 个（API 3 + gen_server 6 + internal 9）
- 新增方法：11 个
- **合计：29 个**，符合用户规则「每个类的方法不超过 100 个」

**参数计数**：所有新方法参数 ≤ 3 个，符合用户规则。

**复杂度与行数**：所有新方法复杂度 ≤ 5、行数 ≤ 15，远低于用户规则上限（复杂度 ≤ 10、行数 ≤ 100）。

### gen_server 回调签名（保持不变）

`handle_info/2` 新增子句的模式匹配：

```erlang
handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
    {noreply, handle_client_down_by_ref(Ref, Reason, State)};
handle_info({'EXIT', Pid, Reason}, State) ->
    {noreply, handle_producer_exit(Pid, Reason, State)};
handle_info(probe_kafka, State) ->
    {noreply, probe_kafka(State)};
handle_info(_Info, State) ->
    {noreply, State}.
```

**注意**：`handle_client_down_by_ref/3` 和 `handle_producer_exit/3` 不在 11 个新方法列表中 —— 它们作为 `handle_info` 的辅助函数存在，可合并到 `handle_client_down/3` 中或单独提取。为控制方法总数，本次设计采用**单独提取**方式，最终新增方法实际为 **13 个**（总计 31 个，仍远低于 100 上限）。

## Compliance with User Rules（用户规则符合性）

| 规则 | 符合性 |
|------|--------|
| 每个类都要添加注释 | ✅ 所有新增方法有 @doc 注释 |
| 每个方法都要添加注释 | ✅ 同上 |
| 最小侵入模式 | ✅ 仅修改 1 个文件，不新增模块/包 |
| 每个方法参数不超过 3 个 | ✅ 所有新方法参数 ≤ 3 |
| 每个方法逻辑不超过 100 行 | ✅ 最长 15 行 |
| 每个方法复杂度不超过 10 | ✅ 最高复杂度 5 |
| 每个类方法不超过 100 个 | ✅ 31 个 |
| 每个类属性不超过 10 个 | ✅ 6 个 state 字段 |
| 自动修复类型引起问题 | ✅ 所有新方法有 -spec |
| 所有文件编码 UTF-8 | ✅ 沿用现有文件编码 |
| 新增方法从最后一个方法后添加 | ✅ 追加到 `translate/1` 之后 |
| 修复所有 java 包和类引入问题 | N/A（Erlang 项目） |

## Out of Scope（不在本次范围内）

- 不修改 `emqx_plugin_kafka.erl` 的熔断器逻辑（`check_circuit/0`、`record_failure/0`、`reset_failures/0`、`maybe_half_open/0` 保持不变）
- **例外**：需将 `init_tables/0` 加入 `emqx_plugin_kafka.erl` 的 `-export` 列表（仅导出，不修改函数体），供 `client_srv:init/1` 调用以确保 ETS 表存在
- 不修改 `emqx_plugin_kafka_health.erl`（其 `check/0` 仍为一次性检查，不与周期探测合并）
- 不修改 `emqx_plugin_kafka_sup.erl` 监督树结构
- 不修改 `emqx_plugin_kafka_app.erl` 的启动顺序（保持 sup 先于 load）
- 不从 `Env` 读取 probe 配置（使用常量）
- 不实现 backoff 重试策略（固定 15s 周期）

## Implementation Order（实现顺序建议）

1. 在 `emqx_plugin_kafka.erl` 的 `-export` 列表中加入 `init_tables/0`
2. 在 `emqx_plugin_kafka_client_srv.erl` 中扩展 `#state{}` record 与新增常量定义（`?CB_TABLE`、`?PROBE_INTERVAL_MS`、`?PROBE_TIMEOUT_MS`）
3. 修改 `init/1`：调用 `emqx_plugin_kafka:init_tables()`、`process_flag(trap_exit, true)`、`monitor_clients/1`、`init_health_metrics/0`、`schedule_probe/1`
4. 修改 `handle_info/2`：新增 3 个子句（`{'DOWN',...}`、`{'EXIT',...}`、`probe_kafka`）
5. 追加 13 个新方法到文件末尾（11 个主方法 + 2 个 `handle_info` 辅助函数）
6. 编译验证（`rebar3 compile`）
7. 在 EMQX Docker 容器中测试：手动 kill client 进程、停止 Kafka broker、恢复 Kafka broker

## Dependencies（依赖与前置条件）

### ETS 表创建顺序

`client_srv` 新增的 `mark_kafka_down/1`、`mark_kafka_up/1`、`init_health_metrics/0` 会写入 `kafka_circuit_breaker` 和 `kafka_metrics` 表。这两个表由 [emqx_plugin_kafka.erl](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka.erl#L277) 的 `init_tables/0` 创建，该函数在 `load/1` 中调用。

**问题**：[emqx_plugin_kafka_app.erl](file:///Users/lute/IdeaProjects/emqx-plugin-kafkav5/src/emqx_plugin_kafka_app.erl#L27-L32) 的当前启动顺序为：
```erlang
{ok, Sup} = emqx_plugin_kafka_sup:start_link(Cnf),   %% 1. 启动 supervisor → client_srv:init/1
emqx_plugin_kafka:load(Cnf),                          %% 2. 创建 ETS 表 + 挂载 hooks
```
即 `client_srv:init/1` 在 `init_tables/0` **之前**执行，此时 `kafka_circuit_breaker` 和 `kafka_metrics` 表尚不存在。

**解决方案**：在 `client_srv:init/1` 中调用 `emqx_plugin_kafka:init_tables()` 创建 ETS 表（该函数是幂等的，检查表是否存在后才创建）。这样：
1. `client_srv:init/1` 先创建 ETS 表
2. `load/1` 随后调用 `init_tables/0` 发现表已存在，跳过创建
3. `load/1` 挂载 hooks 时表已就绪

此方案不修改 `app.erl` 的启动顺序（保持 sup 先于 load，避免 hooks 在 client_srv 就绪前触发），仅在 `client_srv:init/1` 中增加一行 `emqx_plugin_kafka:init_tables()` 调用。

**防御性处理**：`init_health_metrics/0` 仍使用 `ets:insert_new/2`，避免条目已存在时覆盖。`mark_kafka_down/1` 和 `mark_kafka_up/1` 使用 `try/catch` 包裹 ETS 写入，防止极端情况下表不存在导致崩溃。
