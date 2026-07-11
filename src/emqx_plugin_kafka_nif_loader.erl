%% @doc Multi-architecture NIF loader for crc32cer.
%% Places the correct architecture .so before crc32cer is loaded.
-module(emqx_plugin_kafka_nif_loader).

-export([ensure_nif_loaded/0]).

%% Architecture to .so filename mapping (Linux only — plugin runs in Docker)
-define(ARCH_SO_MAP, [
    {<<"aarch64-unknown-linux-gnu">>, <<"crc32cer_nif_aarch64.so">>},
    {<<"x86_64-unknown-linux-gnu">>,  <<"crc32cer_nif_x86_64.so">>}
]).

%% @doc Ensures the crc32cer NIF is loaded for the current architecture.
%% CRITICAL: Must check if NIF already works BEFORE copying .so.
%% Overwriting a .so that is currently loaded/memory-mapped causes SIGSEGV.
%% This happens on container restart when the correct .so was placed on
%% the first boot — on_load loads it, and file:copy then overwrites it.
-spec ensure_nif_loaded() -> ok | {error, term()}.
ensure_nif_loaded() ->
    Arch0 = erlang:system_info(system_architecture),
    Arch = unicode:characters_to_binary(Arch0),
    log("NIF loader started, architecture: ~s (raw: ~p)", [Arch, Arch0]),
    case nif_already_working() of
        true ->
            log("crc32cer NIF already working, skipping .so placement", []),
            ok;
        false ->
            place_and_load(Arch)
    end.

%% @doc Places the correct .so and loads/reloads crc32cer as needed.
-spec place_and_load(binary()) -> ok | {error, term()}.
place_and_load(Arch) ->
    case arch_to_so_name(Arch) of
        undefined ->
            log("ERROR: Unsupported architecture: ~s", [Arch]),
            {error, {unsupported_arch, Arch}};
        SoName ->
            case place_arch_so(SoName) of
                ok -> handle_nif_after_placement();
                {error, _} = Err -> Err
            end
    end.

%% @doc Handles NIF state after .so is placed.
%% Checks module load state BEFORE calling any crc32cer function to avoid
%% triggering auto-load (which can segfault the VM during on_load).
-spec handle_nif_after_placement() -> ok | {error, term()}.
handle_nif_after_placement() ->
    case is_module_loaded(crc32cer) of
        false ->
            log("crc32cer not yet loaded, loading with .so in place...", []),
            load_crc32cer_fresh();
        true ->
            check_and_maybe_reload()
    end.

%% @doc Loads crc32cer for the first time using code:load_file.
%% This is the safe path — calling crc32cer:nif/1 to trigger auto-load
%% can cause a segfault during on_load that kills the VM.
-spec load_crc32cer_fresh() -> ok | {error, term()}.
load_crc32cer_fresh() ->
    case code:load_file(crc32cer) of
        {module, _} ->
            case nif_already_working() of
                true ->
                    log("crc32cer NIF loaded successfully on first load", []),
                    ok;
                false ->
                    log("ERROR: crc32cer NIF not loaded after first load", []),
                    {error, nif_not_loaded}
            end;
        {error, Reason} ->
            log("ERROR: Failed to load crc32cer: ~p", [Reason]),
            {error, {load_failed, Reason}}
    end.

%% @doc Checks if NIF works for an already-loaded module; reloads if broken.
-spec check_and_maybe_reload() -> ok | {error, term()}.
check_and_maybe_reload() ->
    case nif_already_working() of
        true ->
            log("crc32cer NIF already working", []),
            ok;
        false ->
            log("crc32cer loaded but NIF broken, reloading...", []),
            reload_crc32cer()
    end.

%% @doc Checks if the NIF is already functional.
-spec nif_already_working() -> boolean().
nif_already_working() ->
    try crc32cer:nif(<<>>) of
        _ -> true
    catch
        _:_ -> false
    end.

%% @doc Returns true if the module is currently loaded.
-spec is_module_loaded(atom()) -> boolean().
is_module_loaded(Mod) ->
    case code:is_loaded(Mod) of
        false -> false;
        {file, _} -> true
    end.

%% @doc Maps system architecture string to the corresponding .so filename.
-spec arch_to_so_name(binary()) -> binary() | undefined.
proplists_get_value(Key, [{Key, Value} | _]) -> Value;
proplists_get_value(Key, [_ | Rest]) -> proplists_get_value(Key, Rest);
proplists_get_value(_, []) -> undefined.

arch_to_so_name(Arch) ->
    proplists_get_value(Arch, ?ARCH_SO_MAP).

%% @doc Copies the architecture-specific .so from plugin priv to crc32cer priv.
-spec place_arch_so(binary()) -> ok | {error, term()}.
place_arch_so(SoName) ->
    SrcDir = plugin_nif_dir(),
    DstDir = crc32cer_priv_dir(),
    log("NIF src dir: ~p, dst dir: ~p", [SrcDir, DstDir]),
    case {SrcDir, DstDir} of
        {undefined, _} ->
            log("ERROR: plugin priv/nif dir not found", []),
            {error, plugin_priv_not_found};
        {_, undefined} ->
            log("ERROR: crc32cer priv dir not found", []),
            {error, crc32cer_priv_not_found};
        {Src, Dst} ->
            copy_so_file(Src, Dst, SoName)
    end.

%% @doc Copies a single .so file from source to the expected crc32cer path.
-spec copy_so_file(file:filename(), file:filename(), binary()) -> ok | {error, term()}.
copy_so_file(SrcDir, DstDir, SoName) ->
    Src = filename:join(SrcDir, SoName),
    Dst = filename:join(DstDir, "crc32cer_nif.so"),
    case file:read_file_info(Src) of
        {ok, _} ->
            case file:copy(Src, Dst) of
                {ok, _} ->
                    log("Placed NIF ~s -> ~s", [Src, Dst]),
                    ok;
                {error, Reason} ->
                    log("ERROR: Failed to copy NIF ~s: ~p", [Src, Reason]),
                    {error, {copy_failed, Reason}}
            end;
        {error, Reason} ->
            log("ERROR: Source NIF not found ~s: ~p", [Src, Reason]),
            {error, {src_not_found, Src, Reason}}
    end.

%% @doc Purges and reloads crc32cer to re-trigger on_load with the new .so.
-spec reload_crc32cer() -> ok | {error, term()}.
reload_crc32cer() ->
    code:purge(crc32cer),
    code:delete(crc32cer),
    case code:load_file(crc32cer) of
        {module, _} ->
            case nif_already_working() of
                true ->
                    log("crc32cer NIF reloaded successfully", []),
                    ok;
                false ->
                    log("ERROR: crc32cer NIF still not loaded after reload", []),
                    {error, nif_still_not_loaded}
            end;
        {error, Reason} ->
            log("ERROR: Failed to reload crc32cer: ~p", [Reason]),
            {error, {reload_failed, Reason}}
    end.

%% @doc Returns the plugin's priv/nif directory path.
-spec plugin_nif_dir() -> file:filename() | undefined.
plugin_nif_dir() ->
    case code:priv_dir(emqx_plugin_kafka) of
        {error, _} ->
            %% Fallback: search code path for emqx_plugin_kafka
            find_priv_dir(emqx_plugin_kafka, "nif");
        Dir ->
            filename:join(Dir, "nif")
    end.

%% @doc Returns crc32cer's priv directory path.
-spec crc32cer_priv_dir() -> file:filename() | undefined.
crc32cer_priv_dir() ->
    case code:priv_dir(crc32cer) of
        {error, _} ->
            find_priv_dir(crc32cer, "");
        Dir ->
            Dir
    end.

%% @doc Fallback: search code path for the application's priv directory.
-spec find_priv_dir(atom(), string()) -> file:filename() | undefined.
find_priv_dir(App, SubDir) ->
    Path = code:which(App),
    log("code:which(~p) -> ~p", [App, Path]),
    case Path of
        non_existing -> undefined;
        BeamPath ->
            %% BeamPath is like ".../crc32cer-0.1.8/ebin/crc32cer.beam"
            EbinDir = filename:dirname(BeamPath),
            AppDir = filename:dirname(EbinDir),
            PrivDir = filename:join(AppDir, "priv"),
            case file:list_dir(PrivDir) of
                {ok, _} ->
                    case SubDir of
                        "" -> PrivDir;
                        _ -> filename:join(PrivDir, SubDir)
                    end;
                {error, _} -> undefined
            end
    end.

%% @doc Logs to both io:format and logger for reliability during early startup.
log(Format, Args) ->
    Msg = io_lib:format("[KAFKA NIF LOADER] " ++ Format ++ "~n", Args),
    io:format(Msg),
    logger:info(Msg).
