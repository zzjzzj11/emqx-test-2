%% @doc Multi-architecture NIF loader for crc32cer.
%% Selects the correct architecture .so at startup and reloads crc32cer.
-module(emqx_plugin_kafka_nif_loader).

-export([ensure_nif_loaded/0]).

-include_lib("emqx/include/logger.hrl").

%% Architecture to .so filename mapping (Linux only — plugin runs in Docker)
-define(ARCH_SO_MAP, [
    {<<"aarch64-unknown-linux-gnu">>, <<"crc32cer_nif_aarch64.so">>},
    {<<"x86_64-unknown-linux-gnu">>,  <<"crc32cer_nif_x86_64.so">>}
]).

%% @doc Ensures the crc32cer NIF is loaded for the current architecture.
%% If already working, returns ok immediately. Otherwise selects the
%% correct .so, places it, and reloads crc32cer.
-spec ensure_nif_loaded() -> ok | {error, term()}.
ensure_nif_loaded() ->
    case nif_already_working() of
        true -> ok;
        false -> select_and_reload()
    end.

%% @doc Checks if the NIF is already functional by calling crc32cer:nif/1.
-spec nif_already_working() -> boolean().
nif_already_working() ->
    try crc32cer:nif(<<>>) of
        _ -> true
    catch
        _:_ -> false
    end.

%% @doc Selects the architecture-specific .so, places it, and reloads crc32cer.
-spec select_and_reload() -> ok | {error, term()}.
select_and_reload() ->
    Arch = erlang:system_info(system_architecture),
    case arch_to_so_name(Arch) of
        undefined ->
            logger:error("[KAFKA PLUGIN] Unsupported architecture: ~s", [Arch]),
            {error, {unsupported_arch, Arch}};
        SoName ->
            case place_arch_so(SoName) of
                ok -> reload_crc32cer();
                {error, _} = Err -> Err
            end
    end.

%% @doc Maps system architecture string to the corresponding .so filename.
-spec arch_to_so_name(binary()) -> binary() | undefined.
arch_to_so_name(Arch) ->
    proplists:get_value(Arch, ?ARCH_SO_MAP).

%% @doc Copies the architecture-specific .so from plugin priv to crc32cer priv.
-spec place_arch_so(binary()) -> ok | {error, term()}.
place_arch_so(SoName) ->
    case {plugin_nif_dir(), crc32cer_priv_dir()} of
        {undefined, _} -> {error, plugin_priv_not_found};
        {_, undefined} -> {error, crc32cer_priv_not_found};
        {SrcDir, DstDir} -> copy_so_file(SrcDir, DstDir, SoName)
    end.

%% @doc Copies a single .so file from source to the expected crc32cer path.
-spec copy_so_file(file:filename(), file:filename(), binary()) -> ok | {error, term()}.
copy_so_file(SrcDir, DstDir, SoName) ->
    Src = filename:join(SrcDir, SoName),
    Dst = filename:join(DstDir, "crc32cer_nif.so"),
    case file:copy(Src, Dst) of
        {ok, _} ->
            logger:info("[KAFKA PLUGIN] Placed NIF ~s -> ~s", [Src, Dst]),
            ok;
        {error, Reason} ->
            logger:error("[KAFKA PLUGIN] Failed to copy NIF ~s: ~p", [Src, Reason]),
            {error, {copy_failed, Reason}}
    end.

%% @doc Purges and reloads crc32cer to re-trigger on_load with the new .so.
-spec reload_crc32cer() -> ok | {error, term()}.
reload_crc32cer() ->
    code:purge(crc32cer),
    code:delete(crc32cer),
    case code:load_file(crc32cer) of
        {module, _} -> verify_nif_loaded();
        {error, Reason} -> {error, {reload_failed, Reason}}
    end.

%% @doc Verifies the NIF is functional after reload.
-spec verify_nif_loaded() -> ok | {error, term()}.
verify_nif_loaded() ->
    case nif_already_working() of
        true ->
            logger:info("[KAFKA PLUGIN] crc32cer NIF reloaded successfully"),
            ok;
        false ->
            logger:error("[KAFKA PLUGIN] crc32cer NIF still not loaded after reload"),
            {error, nif_still_not_loaded}
    end.

%% @doc Returns the plugin's priv/nif directory path.
-spec plugin_nif_dir() -> file:filename() | undefined.
plugin_nif_dir() ->
    case code:priv_dir(emqx_plugin_kafka) of
        {error, _} -> undefined;
        Dir -> filename:join(Dir, "nif")
    end.

%% @doc Returns crc32cer's priv directory path.
-spec crc32cer_priv_dir() -> file:filename() | undefined.
crc32cer_priv_dir() ->
    case code:priv_dir(crc32cer) of
        {error, _} -> undefined;
        Dir -> Dir
    end.
