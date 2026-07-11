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

%% @doc EMQX Kafka 插件健康检查模块。
%% 提供 NIF 加载状态检查与 brod 客户端连接状态检查，
%% 在插件启动前调用以避免静默失败。
-module(emqx_plugin_kafka_health).

-export([ check/0
        , check_crc32cer_nif/0
        , check_clients/1
        ]).

-define(CHECK_TIMEOUT_MS, 5000).

%% @doc 综合健康检查：NIF + 所有已注册的 Kafka 客户端。
-spec check() -> ok | {error, term()}.
check() ->
    case check_crc32cer_nif() of
        ok ->
            check_clients(all_registered_clients());
        {error, _} = NifErr ->
            NifErr
    end.

%% @doc 检查 crc32cer NIF 是否已加载。
%% NIF 未加载时返回诊断信息（SoPath、存在性、工作目录、架构、OTP release）。
-spec check_crc32cer_nif() -> ok | {error, term()}.
check_crc32cer_nif() ->
    try crc32cer:nif(<<>>) of
        0 -> ok;
        _Other -> ok
    catch
        _:_ ->
            Diag = build_nif_diagnostics(),
            logger:error("[KAFKA PLUGIN]crc32cer NIF not loaded. Diagnostics:~n~p", [Diag]),
            {error, {crc32cer_nif_not_loaded, Diag}}
    end.

%% @doc 检查指定客户端列表的连接状态。
-spec check_clients([atom()]) -> ok | {error, term()}.
check_clients(Clients) ->
    Bad = lists:filtermap(fun check_client_status/1, Clients),
    case Bad of
        [] -> ok;
        [#{client := C, reason := R} | _] -> {error, {client_unhealthy, C, R}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc 所有已注册的 Kafka 客户端 ID。
-spec all_registered_clients() -> [atom()].
all_registered_clients() -> [client1, client2, client3].

%% @doc 检查单个 brod 客户端状态。
%% 返回 false 表示健康，{true, Map} 表示异常。
-spec check_client_status(atom()) -> false | {true, map()}.
check_client_status(ClientId) ->
    case brod_sup:find_client(ClientId) of
        [_Pid] ->
            false;
        [] ->
            {true, #{client => ClientId, reason => not_started}}
    end.

%% @doc 构建 NIF 加载失败诊断信息。
-spec build_nif_diagnostics() -> map().
build_nif_diagnostics() ->
    {ok, Cwd} = file:get_cwd(),
    SoPath = guess_so_path(),
    SoExists = SoPath =/= undefined andalso filelib:is_file(SoPath),
    #{ so_path => SoPath
     , so_exists => SoExists
     , working_dir => Cwd
     , system_architecture => erlang:system_info(system_architecture)
     , otp_release => erlang:system_info(otp_release)
     , nif_bin_dir_env => os:getenv("NIF_BIN_DIR")
     , priv_dir => safe_priv_dir()
     }.

%% @doc 尝试猜测 NIF so 路径（不依赖 crc32cer 内部函数）。
-spec guess_so_path() -> string() | undefined.
guess_so_path() ->
    Candidates = candidate_nif_dirs(),
    find_nif_in_dirs(Candidates).

%% @doc 构建 NIF 候选目录列表。
-spec candidate_nif_dirs() -> [string() | false].
candidate_nif_dirs() ->
    {ok, Cwd} = file:get_cwd(),
    [ safe_priv_dir()
    , filename:join([Cwd, "..", "priv"])
    , filename:join(Cwd, "priv")
    , os:getenv("NIF_BIN_DIR")
    ].

%% @doc 安全获取 crc32cer priv 目录。
-spec safe_priv_dir() -> string() | false.
safe_priv_dir() ->
    try code:priv_dir(crc32cer) of
        Dir when is_list(Dir) -> Dir;
        _ -> false
    catch
        _:_ -> false
    end.

%% @doc 在候选目录列表中查找包含 NIF 的目录。
-spec find_nif_in_dirs([string() | false]) -> string() | undefined.
find_nif_in_dirs([]) -> undefined;
find_nif_in_dirs([false | Rest]) -> find_nif_in_dirs(Rest);
find_nif_in_dirs([Dir | Rest]) ->
    case filelib:wildcard(filename:join([Dir, "crc32cer_nif*"])) of
        [] -> find_nif_in_dirs(Rest);
        [_ | _] -> filename:join([Dir, "crc32cer_nif"])
    end.
