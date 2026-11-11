"""Run clang-tidy with the selected preset's compilation database."""

import json
import os
import re
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory

import typer
from invoke import Result

from . import utils
from .cmd_build import ensure_configured
from .config import DEFAULT_JOBS, get_build_directory
from .output import Output
from .tidy_stage import stage_sources, strip_reflection_annotations
from .utils import collect_cpp_files, find_clang_tool, run_action, selection

DIAGNOSTIC_CHECK_RE = re.compile(r"(?:warning|error):\s+.*\[([^\],\s]+)(?:,[^\]]*)?\]\s*$")
LOCATION_DIAG_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+(warning|error):\s+(.*)\[([^\],\s]+)(?:,[^\]]*)?\]\s*$")
BARE_LOCATION_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+(warning|error):\s+(.*)$")
NOTE_LOCATION_RE = re.compile(r"^([^:\n]+):(\d+):(\d+):\s+note:\s+(.*)$")
LOCATION_PREFIX_RE = re.compile(r"^[^:\n]+:\d+:\d+:\s+(?:warning|error|note):\s+")

_DIAG_TOKENS = "^^ [[= [: std::meta define_static_ meta/meta"
REFLECTION_DIAGNOSTIC_MARKERS = (
    *_DIAG_TOKENS.split(),
    "no member named 'meta' in namespace 'std'",
    "template for",
    "blocks support disabled",
    "'meta' file not found",
    "reflection is only",
    "expansion statements",
    "iterating expansion statements are not yet supported",
)

_SEED_TOKENS = (
    "uarch:: ZeroRegisterCapability RegisterPassable NoPadding tiers::as_str Subsystem(token) uring::Subsystem( "
    "Subsystem( Slot( Member( Generation( Slot(token) State(h) ServiceContract ExplicitContract ServiceName ContractMethods HasFourShapes RowConcept "
    "RowSchema TypedColumnarBatch BorrowedRow RowView ViewDecodedRow ReflectedColumnBuf MemberType DataMembers "
    "DescribeRow TableName ColumnMeta ValidCoefficient annotations.h reflected_row.h reflected_column.h "
    "wire_row_view.h bench_fixture.h TidyMember_"
)
_REFLECTION_SEED_MARKERS = (*REFLECTION_DIAGNOSTIC_MARKERS, *_SEED_TOKENS.split())

_SKIP_ROW = "Uses annotations.h/reflected_row.h P2996 ^^ and splices."
_SKIP_TRANSITIVE = "Transitively includes annotations.h/reflected_row.h P2996 syntax."
_SKIP_DIRECT = "Direct P2996 reflection in example (^^ / splices)."
REFLECTION_ANALYZER_SKIP_TUS: dict[str, str] = {
    "src/greptimedb/client/client.cc": _SKIP_ROW,
    "tests/units/proto/test_proto_smoke.cc": "Includes grpcxx/reflection contract metadata and client_stub expansion.",
    "tests/units/row/test_reflected_row.cc": _SKIP_ROW,
    "tests/units/row/test_multi_table_reflected.cc": _SKIP_ROW,
    "tests/units/row/test_row_hardening.cc": _SKIP_ROW,
    "tests/units/threading/test_threading_pools.cc": "Transitively includes cares/uring P2996 reflection headers.",
    "tests/units/client/test_unary_client_operations.cc": _SKIP_ROW,
    "tests/units/client/test_client_session_factories.cc": _SKIP_ROW,
    "tests/units/client/test_client_audit.cc": _SKIP_ROW,
    "tests/units/adversarial/test_adversarial_regressions.cc": _SKIP_ROW,
    "tests/e2e/test_docker_greptimedb_e2e.cc": _SKIP_TRANSITIVE,
    "tests/units/failover/test_failover_production.cc": _SKIP_TRANSITIVE,
    "examples/01_quickstart_unary_ddl_and_crud.cc": _SKIP_ROW,
    "examples/02_reflected_row_and_zero_copy_views.cc": _SKIP_DIRECT,
    "examples/03_memory_policy_and_batch_reuse.cc": _SKIP_ROW,
    "examples/06_arrow_flight_doput_bulk_writer.cc": _SKIP_ROW,
    "examples/07_arrow_flight_doget_and_promql.cc": _SKIP_ROW,
    "examples/08_thread_local_and_worker_striping.cc": _SKIP_TRANSITIVE,
    "examples/09_tls_and_failover_circuit_breaker.cc": _SKIP_TRANSITIVE,
    "examples/10_high_throughput_telemetry_pipeline.cc": _SKIP_DIRECT,
}

_UNDECLARED_ANCHOR_WINDOW = 20
_DRIVER_ERROR_MARKERS = (
    "unknown argument:",
    "unsupported option",
    "invalid value",
    "no such file or directory",
    "unable to find compilation database",
    "Error while trying to load a compilation database",
    "unable to handle compilation",
    "no input files",
)
_REFLECTION_PROBE = "consteval bool Probe() { return ^^int == ^^int; }\nstatic_assert(Probe());\n"


@dataclass
class _DiagnosticBlock:
    file: str
    line: int
    message: str
    header: str
    lines: list[str] = field(default_factory=list)
    notes: set[str] = field(default_factory=set)
    note_locations: set[tuple[str, int]] = field(default_factory=set)

    def body(self) -> str:
        return "\n".join(LOCATION_PREFIX_RE.sub("", line) for line in self.lines)


def _has_direct_marker(text: str) -> bool:
    return any(marker in text for marker in REFLECTION_DIAGNOSTIC_MARKERS)


def _is_reflection_seed(text: str) -> bool:
    return any(marker in text for marker in _REFLECTION_SEED_MARKERS)


def _has_reflection_note(
    block: _DiagnosticBlock, rfiles: set[str] | None = None, rlocs: set[tuple[str, int]] | None = None
) -> bool:
    if (rfiles and (block.notes & rfiles)) or (rlocs and (block.note_locations & rlocs)):
        return True
    return any(": note:" in ln and (_has_direct_marker(ln) or _is_reflection_seed(ln)) for ln in block.lines[1:])


def _is_direct_reflection_block(block: _DiagnosticBlock) -> bool:
    return (
        _has_direct_marker(block.body())
        or block.message.strip().startswith("expanding ")
        or _has_reflection_note(block)
    )


def _parse_diagnostic_blocks(output: str) -> tuple[list[_DiagnosticBlock], list[str]]:
    blocks: list[_DiagnosticBlock] = []
    unlocated: list[str] = []
    current: _DiagnosticBlock | None = None
    for line in output.splitlines():
        if any(marker in line for marker in _DRIVER_ERROR_MARKERS):
            if current is not None:
                blocks.append(current)
                current = None
            unlocated.append(line)
            continue
        if m := LOCATION_DIAG_RE.match(line):
            if current is not None:
                blocks.append(current)
            chk, dfile = m.group(6), Path(m.group(1))
            ext_warn = m.group(4) == "warning" and dfile.is_absolute() and not dfile.is_relative_to(utils.PROJECT_ROOT)
            keep = chk.startswith("clang-diagnostic-") and not ext_warn
            current = _DiagnosticBlock(m.group(1), int(m.group(2)), m.group(5), line, [line]) if keep else None
            continue
        if bare := BARE_LOCATION_RE.match(line):
            if current is not None:
                blocks.append(current)
            current = _DiagnosticBlock(bare.group(1), int(bare.group(2)), bare.group(5), line, [line])
            continue
        if check := DIAGNOSTIC_CHECK_RE.search(line):
            if current is not None:
                blocks.append(current)
                current = None
            if check.group(1).startswith("clang-diagnostic-"):
                unlocated.append(line)
            continue
        if current is not None:
            current.lines.append(line)
            if note := NOTE_LOCATION_RE.match(line):
                current.notes.add(note.group(1))
                current.note_locations.add((note.group(1), int(note.group(2))))
    if current is not None:
        blocks.append(current)
    return blocks, unlocated


def _reflection_declared_names(filename: str) -> set[str]:
    """Only declaration evidence can explain a distant missing reflection-derived name."""
    try:
        text = Path(filename).read_text()
        plain = strip_reflection_annotations(text)
    except (OSError, UnicodeError, ValueError):
        return set()
    names: set[str] = set()
    for original, stripped in zip(text.splitlines(), plain.splitlines(), strict=True):
        if any(marker in original for marker in ("[[=", "[[ =", "[:", "template for")):
            names.update(re.findall(r"\b(?:struct|class|using|typename)\s+([A-Za-z_]\w*)", stripped))
            names.update(re.findall(r"\b([A-Za-z_]\w*)\s*(?:;|=)", stripped))
    reflection_types = names | {name for name in _REFLECTION_SEED_MARKERS if re.fullmatch(r"[A-Za-z_]\w*", name)}
    aliases = re.findall(r"\busing\s+([A-Za-z_]\w*)\s*=([^;]+);", plain)
    changed = True
    while changed:
        changed = False
        for alias, expression in aliases:
            if alias not in reflection_types and set(re.findall(r"[A-Za-z_]\w*", expression)) & reflection_types:
                names.add(alias)
                reflection_types.add(alias)
                changed = True
    for name in tuple(names):
        names.update(re.findall(r"\b" + re.escape(name) + r"(?:\s*<[^;{}]*>)?\s+([A-Za-z_]\w*)\s*(?:[({=;])", plain))
    return names


def _classify_clang_diagnostics(output: str) -> tuple[bool, list[str]]:
    if fatals := _actionable_fatal_lines(output):
        return False, fatals
    if any(marker in output for marker in _DRIVER_ERROR_MARKERS):
        return False, [line for line in output.splitlines() if line.strip()]
    blocks, unlocated = _parse_diagnostic_blocks(output)
    if unlocated or not blocks:
        return False, unlocated
    by_file: dict[str, list[_DiagnosticBlock]] = {}
    direct_lines: dict[str, list[int]] = {}
    rlocs: set[tuple[str, int]] = set()
    for block in blocks:
        by_file.setdefault(block.file, []).append(block)
        if _is_direct_reflection_block(block):
            direct_lines.setdefault(block.file, []).append(block.line)
            rlocs.add((block.file, block.line))
            rlocs.update(block.note_locations)
    if not direct_lines:
        return False, [block.header for block in blocks]
    declared = {filename: _reflection_declared_names(filename) for filename in by_file}
    declared_names = set().union(*declared.values())
    rfiles = set(direct_lines.keys()) | {filename for filename, names in declared.items() if names}
    rfiles.update(fp for fp, fb in by_file.items() if _is_reflection_seed(fb[0].body()))
    for filename in by_file:
        try:
            if any(
                marker in Path(filename).read_text()
                for marker in ("#include <meta>", "template for", "^^", "[:", "[[=")
            ):
                rfiles.add(filename)
        except (OSError, UnicodeError):
            continue
    changed = True
    while changed:
        changed = False
        for fp, fbs in by_file.items():
            if fp not in rfiles and any(_has_reflection_note(b, rfiles, rlocs) for b in fbs):
                rfiles.add(fp)
                changed = True
    unsuppressed: list[str] = []
    for block in blocks:
        loc = (block.file, block.line)
        if block.file not in rfiles and loc not in rlocs and not _has_reflection_note(block, rfiles, rlocs):
            unsuppressed.append(block.header)
            continue
        if "undeclared identifier" in block.message:
            ident = re.search(r"undeclared identifier '([A-Za-z_]\w*)'", block.message)
            evidenced = ident is not None and ident[1] in declared_names
            near = any(0 <= block.line - a <= _UNDECLARED_ANCHOR_WINDOW for a in direct_lines.get(block.file, []))
            if not (evidenced or loc in rlocs or near):
                unsuppressed.append(block.header)
    return bool(blocks and not unsuppressed), unsuppressed


def is_reflection_clang_tidy(clang_tidy: str, directory: Path | None = None) -> bool:
    """Return True when clang-tidy natively parses C++26 P2996 reflection syntax."""
    try:
        directory = directory if directory is not None else utils.PROJECT_ROOT / "build"
        directory.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="greptimedb-tidy-probe-", dir=directory) as temporary:
            probe = Path(temporary) / "probe.cc"
            probe.write_text(_REFLECTION_PROBE)
            for flag in ("-freflection", "-freflection-latest"):
                cmd = [str(clang_tidy), str(probe), "--", "-std=c++26", flag]
                res = subprocess.run(cmd, cwd=temporary, text=True, capture_output=True, check=False, timeout=15)
                if res.returncode == 0:
                    return True
    except (OSError, subprocess.SubprocessError):
        return False
    return False


def _extract_non_clang_diagnostics(output: str) -> list[str]:
    return [
        ln
        for ln in output.splitlines()
        if (m := DIAGNOSTIC_CHECK_RE.search(ln)) and not m[1].startswith("clang-diagnostic-")
    ]


def _unique_lines(lines: list[str]) -> list[str]:
    return list(dict.fromkeys(line for line in lines if line))


def _actionable_fatal_lines(output: str) -> list[str]:
    return [
        ln
        for ln in output.splitlines()
        if ("fatal error:" in ln or re.search(r"error:.*file not found", ln)) and "'meta' file not found" not in ln
    ]


def _tidy_failure(output: str, status: int | None = 1, capable: bool = False) -> tuple[bool, str]:
    if capable:
        return (status is not None and status != 0) or "fatal error:" in output, output
    filtered = _unique_lines(_extract_non_clang_diagnostics(output))
    fatals = _unique_lines(_actionable_fatal_lines(output))
    reflection_blocks, raw_unsuppressed = _classify_clang_diagnostics(output)
    unsuppressed = _unique_lines(raw_unsuppressed)
    has_refl = reflection_blocks or ("'meta' file not found" in output and not fatals and not unsuppressed)
    bad = (
        (status is not None and status < 0)
        or filtered
        or fatals
        or unsuppressed
        or ((status or 0) > 0 and not has_refl)
    )
    if bad:
        shown = _unique_lines([*filtered, *fatals, *unsuppressed])
        return True, "\n".join(shown) if shown else output
    return False, ""


_REFLECTION_ONLY_SKIP = (
    "Native -Werror -fsyntax-only verified P2996 reflection skip; "
    "stock clang-tidy reported only reflection parser diagnostics"
)


def _syntax_only_arguments(arguments: list[str]) -> list[str]:
    """Drop emit and dependency flags so the project compiler rechecks the TU as syntax only."""
    result: list[str] = []
    skip = False
    for argument in arguments:
        if skip:
            skip = False
            continue
        if argument in {"-o", "-MF", "-MT", "-MQ"}:
            skip = True
        elif argument not in {"-c", "-MD", "-MMD"}:
            result.append(argument)
    if result and Path(result[0]).name == "ccache":
        result.pop(0)
    return [*result, "-Werror", "-fsyntax-only"]


def _native_syntax_agrees(arguments: list[str], cwd: str) -> tuple[bool, str]:
    syntax = subprocess.run(arguments, cwd=cwd, capture_output=True, text=True, check=False)
    output = f"{syntax.stdout or ''}{syntax.stderr or ''}"
    agreed = syntax.returncode == 0 and "warning:" not in output and "error:" not in output
    return agreed, output


def analyzer_result(
    path: Path,
    output: str,
    status: int,
    capable: bool = False,
    compiler_verified: bool | None = None,
    native_output: str = "",
) -> tuple[bool, str, str]:
    """A reflection-only skip stands only after the native compiler accepts the TU."""
    failed, display = _tidy_failure(output, status, capable)
    if failed or capable:
        return failed, display, ""
    if "[clang-diagnostic-error]" not in output and "'meta' file not found" not in output:
        return False, "", ""
    if compiler_verified is not True:
        return True, native_output.strip() or output, ""
    relative = path.resolve().relative_to(utils.PROJECT_ROOT.resolve()).as_posix()
    reason = REFLECTION_ANALYZER_SKIP_TUS.get(relative, _REFLECTION_ONLY_SKIP)
    return False, "", reason


def clang_database(entries: list[dict], tool: str, *, capable: bool = False) -> list[dict]:
    """Translate GCC driver flags and strip reflection flags unsupported by clang-tidy."""
    translated, seen = [], set()
    stripped = {"-freflection-latest"} if capable else {"-freflection-latest", "-freflection", "-DFMT_HEADER_ONLY=1"}
    for entry in entries:
        source = (Path(entry["directory"]) / entry["file"]).resolve()
        if source in seen:
            continue
        seen.add(source)
        arguments = list(entry.get("arguments") or shlex.split(entry.get("command", "")))
        if not arguments:
            raise ValueError(f"Missing compiler command for {source}")
        if "-freflection" in arguments:
            home_tc = Path.home() / ".local/toolchain/clang-glibc"
            for root in (Path(tool).resolve().parent.parent, Path("/opt/clang-p2996"), home_tc):
                sysroot = root / "x86_64-unknown-linux-gnu/sysroot"
                if (root / "bin/clang++").is_file() and (sysroot / "usr/include/c++/v1/meta").is_file():
                    rest = [a for a in arguments[1:] if not a.startswith("--sysroot=")]
                    arguments = [str(root / "bin/clang++"), *rest, f"--sysroot={sysroot}", "-stdlib=libc++"]
                    break
                if (root / "bin/clang++").is_file() and (root / "include/c++/v1/meta").is_file():
                    arguments[0] = str(root / "bin/clang++")
                    arguments.extend(["-stdlib=libc++", "-stdlib++-isystem", str(root / "include/c++/v1")])
                    break
            else:
                if capable:
                    raise ValueError("GCC reflection analysis needs a clang-p2996 SDK with libc++ <meta>")
            arguments = ["-freflection-latest" if arg == "-freflection" else arg for arg in arguments]
        skip_prefixes = ("-fconcepts-diagnostics-depth=", "-fdiagnostics-show-template-tree")
        arguments = [arg for arg in arguments if arg not in stripped and not arg.startswith(skip_prefixes)]
        translated.append({"directory": entry["directory"], "file": str(source), "arguments": arguments})
    return translated


def tidy_checks(path: Path) -> str:
    checks = "--checks=clang-analyzer-*,bugprone-use-after-move"
    if path.resolve().is_relative_to(utils.PROJECT_ROOT / "tests"):
        checks += (
            ",-bugprone-use-after-move,-clang-analyzer-cplusplus.Move,-clang-analyzer-optin.core.EnumCastOutOfRange"
        )
    return checks


_ORIGINAL_RUN_COMMAND = Output.run_command


def run_tidy_commands(commands: list[list[str]], out: Output) -> list[Result]:
    """Keep reporting ordered and on the caller thread while independent analyzers run."""
    hooked = getattr(out.run_command, "__func__", None) is not _ORIGINAL_RUN_COMMAND
    if len(commands) < 2 or any("--fix" in c for c in commands) or hooked:
        return [out.run_command(c, label="clang-tidy", capture="--warnings-as-errors=*" not in c) for c in commands]
    workers = min(DEFAULT_JOBS, os.cpu_count() or DEFAULT_JOBS)
    out.info(f"Analyzing {len(commands)} translation units with {workers} workers")

    def execute(command: list[str]) -> Result:
        res = subprocess.run(command, cwd=utils.PROJECT_ROOT, capture_output=True, text=True)
        return Result(exited=res.returncode, stdout=res.stdout, stderr=res.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(execute, commands))
    for cmd, res in zip(commands, results, strict=True):
        if out.is_json:
            out.emit(
                "command",
                "clang-tidy",
                cmd=shlex.join(cmd),
                exit_code=res.return_code,
                stdout=res.stdout[-50_000:],
                stderr=res.stderr[-10_000:],
            )
        else:
            out.info("RUN " + shlex.join(cmd))
    return results


def tidy_cmd(
    ctx: typer.Context,
    paths: list[str] | None = typer.Argument(None),
    preset: str | None = typer.Option(None, "--preset"),
    compiler: str | None = typer.Option(None, "-c", "--compiler"),
    fix: bool = typer.Option(False, "--fix"),
) -> None:
    def action(out: Output) -> int:
        name, _ = selection(ctx, preset, compiler)
        tool = find_clang_tool("clang-tidy")
        directory = get_build_directory(name)
        database = directory / "compile_commands.json"
        if not database.exists() and (code := ensure_configured(name, out)):
            return code
        directory.mkdir(parents=True, exist_ok=True)
        capable = is_reflection_clang_tidy(tool, directory)
        entries = json.loads(database.read_text())
        compiled = {(Path(entry["directory"]) / entry["file"]).resolve() for entry in entries}
        files = [path for path in collect_cpp_files(paths) if path.resolve() in compiled]
        if not files:
            raise ValueError("No selected sources are present in compile_commands.json")
        selected = set(files)
        entries = [entry for entry in entries if (Path(entry["directory"]) / entry["file"]).resolve() in selected]
        native_commands = {
            (Path(entry["directory"]) / entry["file"]).resolve(): (
                _syntax_only_arguments(list(entry.get("arguments") or shlex.split(entry.get("command", "")))),
                entry["directory"],
            )
            for entry in entries
        }
        extra = (
            ["--extra-arg=-freflection", "--warnings-as-errors=*"]
            if capable
            else ["--extra-arg=-D__is_consteval_only(...)=false", "--extra-arg=-ferror-limit=0"]
        )
        with TemporaryDirectory(prefix="greptimedb-tidy-", dir=directory) as temporary:
            translated = clang_database(entries, tool, capable=capable)
            if not capable:
                if fix:
                    raise ValueError("--fix requires a reflection-capable clang-tidy")
                out.info("Stock analyzer staging bypasses uarch layout proofs; compiler tests validate real proofs.")
                translated = stage_sources(translated, Path(temporary))
            (Path(temporary) / "compile_commands.json").write_text(json.dumps(translated))
            exit_status, skipped, failures, coverage, commands = 0, 0, 0, [], []
            hfilter = (
                re.escape(str(utils.PROJECT_ROOT))
                if capable
                else re.escape(str(Path(temporary) / "source")) + r"/(src|tests|examples|benches)/"
            )
            for path in files:
                src_arg = (
                    str(path) if capable else str(Path(temporary) / "source" / path.relative_to(utils.PROJECT_ROOT))
                )
                hflag = f"--header-filter={hfilter}"
                commands.append(
                    [tool, "-p", temporary, src_arg, hflag, tidy_checks(path), *extra, *(["--fix"] if fix else [])]
                )
            results = run_tidy_commands(commands, out)
            rewrites = {} if capable else json.loads((Path(temporary) / "paths.json").read_text())
            sorted_rewrites = sorted(rewrites.items(), key=lambda pair: len(pair[1]), reverse=True)
            for path, result in zip(files, results, strict=True):
                combined = f"{result.stdout or ''}{result.stderr or ''}"
                for original, staged in sorted_rewrites:
                    combined = combined.replace(staged, original)
                compiler_verified = None
                native_output = ""
                parser_failed, _shown = _tidy_failure(combined, result.return_code, capable)
                reflection_only = not parser_failed and (
                    "[clang-diagnostic-error]" in combined or "'meta' file not found" in combined
                )
                if reflection_only and not capable:
                    spec = native_commands.get(path.resolve())
                    if spec is None:
                        compiler_verified = False
                    else:
                        compiler_verified, native_output = _native_syntax_agrees(*spec)
                failed, display, reason = analyzer_result(
                    path,
                    combined,
                    result.return_code,
                    capable,
                    compiler_verified=compiler_verified,
                    native_output=native_output,
                )
                rel_str, perrs = str(path.relative_to(utils.PROJECT_ROOT)), combined.count("[clang-diagnostic-error]")
                coverage.append({"file": rel_str, "failed": failed, "skip": reason, "parser_errors": perrs})
                if reason:
                    skipped += 1
                    out.info(f"Analyzer SKIPPED {rel_str}: {reason}")
                if failed:
                    failures += 1
                    logged = f"{combined}{native_output}" if native_output else combined
                    (directory / "tidy-failure.log").write_text(logged)
                    if display:
                        out.error(display)
                    exit_status = exit_status or result.return_code or 1
            (directory / "tidy-coverage.json").write_text(json.dumps(coverage, indent=2))
            out.info(
                f"Analyzer coverage: {len(files) - skipped - failures} checked, {skipped} reflection skips, {failures} failures"
            )
            return exit_status

    run_action(ctx, action)
