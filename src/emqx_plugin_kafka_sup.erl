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

%% @doc EMQX Kafka 插件顶层监督器。
%% 监督 `emqx_plugin_kafka_client_srv`（持有 ETS 表与 brod 客户端），
%% 重启策略 `{one_for_one, 5, 10}`：单进程崩溃自动重启，10 秒内最多 5 次重启。
-module(emqx_plugin_kafka_sup).

-behaviour(supervisor).

-export([start_link/0, start_link/1]).

-export([init/1]).

%% @doc 启动监督器（无参数，使用空配置）。
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc 启动监督器并传入 Kafka 配置。
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Env) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Env).

%% @doc 监督器初始化：启动 client_srv 子进程。
-spec init(map()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Env) ->
    SupFlags = #{ strategy => one_for_one
                , intensity => 5
                , period => 10
                },
    ClientSrv = #{ id => emqx_plugin_kafka_client_srv
                  , start => {emqx_plugin_kafka_client_srv, start_link, [Env]}
                  , restart => permanent
                  , shutdown => 5000
                  , type => worker
                  , modules => [emqx_plugin_kafka_client_srv]
                  },
    {ok, {SupFlags, [ClientSrv]}}.
