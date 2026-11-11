"""Shared execution, context selection, and bounded repository file operations."""

import os
import re
import shlex
import shutil
from collections.abc import Callable
from pathlib import Path

import typer
from invoke import Context, Result

from .config import CPP_EXTENSIONS, DEFAULT_JOBS, PROJECT_ROOT, get_build_directory
from .output import Output
from .preset import resolve_preset


def run_command(cmd: list[str], *, cwd: Path | None = None, hide: bool = False) -> Result:
    """Shell-escape argv elements before Invoke passes the command to the shell."""
    command = shlex.join(str(part) for part in cmd)
    command = f"cd {shlex.quote(str(cwd or PROJECT_ROOT))} && {command}"
    return Context().run(command, hide="both" if hide else False, warn=True, pty=False, in_stream=False)


def selection(ctx: typer.Context, preset: str | None, compiler: str | None, jobs: int | None = None) -> tuple[str, int]:
    """Combine global and command-local options, validating conflicting choices."""
    state = ctx.obj
    selected = resolve_preset(preset or state.preset, compiler or state.compiler)
    return selected, jobs or state.jobs or DEFAULT_JOBS


def run_action(ctx: typer.Context, action: Callable[[Output], int]) -> None:
    """Always flush one JSON outcome for operational errors and child failures."""
    out = ctx.obj.output
    try:
        code = action(out)
    except (OSError, ValueError, re.error) as error:
        out.error(str(error))
        code = 1
    raise typer.Exit(out.finish(code))


def clean_project(preset: str, out: Output) -> int:
    """Remove only a validated direct child of the repository's build directory."""
    resolve_preset(preset)
    directory = get_build_directory(preset)
    if directory.is_symlink() or directory.resolve().parent != (PROJECT_ROOT / "build"):
        raise ValueError(f"Refusing to remove a redirected build directory: {directory}")
    if directory.exists():
        shutil.rmtree(directory)
    out.info(f"Cleaned {directory}")
    return 0


def find_clang_tidy() -> str:
    """Locate clang-tidy, honoring environment overrides before toolchain and system binaries."""
    for env_var in ("GREPTIMEDB_CLANG_TIDY", "GRPCXX_CLANG_TIDY", "CLANG_TIDY"):
        if override := os.environ.get(env_var):
            if executable := shutil.which(override):
                return executable
            path = Path(override)
            if path.is_file() and os.access(path, os.X_OK):
                return str(path)
            raise ValueError(f"{env_var} is not executable: {override}")
    for directory in (Path("/opt/clang-p2996/bin"), Path.home() / ".local/toolchain/clang-glibc/bin"):
        if executable := shutil.which("clang-tidy", path=str(directory)):
            return executable
    for candidate in (
        "clang-tidy-23",
        "clang-tidy",
        "/opt/clang-tidy-23.1.6/clang-tidy-23",
        "/usr/bin/clang-tidy-23",
        *(f"clang-tidy-{version}" for version in range(22, 18, -1)),
    ):
        if executable := shutil.which(candidate):
            return executable
    raise ValueError("clang-tidy is not installed; add it to PATH")


def find_clang_tool(name: str) -> str:
    """Prefer versioned tools, reporting missing installations explicitly."""
    if name == "clang-tidy":
        return find_clang_tidy()
    candidates = (
        ("clang-format-18", name)
        if name == "clang-format"
        else (*(f"{name}-{version}" for version in range(23, 18, -1)), name)
    )
    for candidate in candidates:
        if executable := shutil.which(candidate):
            return executable
    raise ValueError(f"{name} is not installed; add it to PATH")


def collect_cpp_files(paths: list[str] | None) -> list[Path]:
    """Collect source files without traversing generated, dependency, or hidden trees."""
    result: set[Path] = set()
    for value in paths or ["src", "examples", "tests", "benches"]:
        path = (PROJECT_ROOT / value).resolve()
        if not path.is_relative_to(PROJECT_ROOT):
            raise ValueError(f"Not a repository path: {value}")
        if not path.exists():
            if paths:
                raise ValueError(f"Not a repository path: {value}")
            continue
        candidates = [path] if path.is_file() else []
        if path.is_dir():
            for directory, folders, files in os.walk(path, followlinks=False):
                folders[:] = [name for name in folders if name != "build" and not name.startswith(".")]
                candidates.extend(Path(directory) / name for name in files)
        for candidate in candidates:
            if candidate.suffix not in CPP_EXTENSIONS or not candidate.is_file():
                continue
            relative = candidate.relative_to(PROJECT_ROOT)
            if not any(
                part == "build" or part.startswith(".") for part in relative.parts
            ) and candidate.resolve().is_relative_to(PROJECT_ROOT):
                result.add(candidate)
    return sorted(result)


def tidy_bit_members(text: str) -> str:
    """Retain annotated bit descriptors when stock Clang cannot reflect their members."""
    import re

    annotation = r"\[\[=\s*(?:uarch::)?(?:bits::)?(?P<kind>Flag|Field)\{(?P<args>[^}]+)\}\s*\]\]"
    declaration = r"[\w:<>]+\s+(?P<name>\w+)"

    def member(match):
        low, *width = (part.strip() for part in match["args"].split(","))
        descriptor = f"{low}, {width[0] if width else '1'}, {'true' if match['kind'] == 'Flag' else 'false'}"
        name = match["name"]
        return (
            match[0]
            + f" static constexpr uarch::bits::StagedMember<decltype({name})> TidyMember_{name}{{{descriptor}}};"
        )

    for pattern in (annotation + r"\s+" + declaration + r"[^;]*;", declaration + r"\s*" + annotation + r"[^;]*;"):
        text = re.sub(pattern, member, text)
    return re.sub(r"\^\^([\w:]*Spec)::(\w+)", r"\1::TidyMember_\2", text)


def tidy_uarch_compat(relative: str, text: str) -> str:
    """Bypass reflection proofs only in analyzer copies; retain runtime bit operations."""
    import re

    layout = """#pragma once
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>
namespace uarch::layout {
struct register_leaf_tag {}; inline constexpr register_leaf_tag register_leaf{};
enum class AbiClass : std::uint8_t { None, Integer, Sse, Memory };
struct AbiShape { AbiClass lo{AbiClass::Integer}, hi{AbiClass::Integer}; std::uint8_t registers{2}; };
template<class T> consteval AbiShape sysv_classify() { return {}; }
template<class T> consteval AbiShape aapcs64_classify() { return {}; }
"""
    concepts = "NoPadding RegisterPassable RegisterLeaf ZeroRegisterCapability GprPair CacheLineDense CacheLineSized IsolatedSized CallTrivial"
    layout += "\n".join(f"template<class T, auto... A> concept {name} = true;" for name in concepts.split())
    functions = "assert_no_padding assert_register_pass assert_gpr_pair assert_layout assert_cache_line assert_isolated check_layout"
    layout += "\n" + "\n".join(
        f"template<class T, auto... A> consteval bool {name}() {{ return true; }}" for name in functions.split()
    )
    layout += "\ntemplate<class T, auto... A> consteval std::string_view layout_report() { return {}; }\n}\n"
    if relative == "core/layout/abi_class.hpp":
        return layout
    if relative in {f"core/layout/{name}.hpp" for name in ["assertions", "no_padding", "register_passable"]}:
        return "#pragma once\n#include <uarch/core/layout/abi_class.hpp>\n"
    if relative == "core/reflect.hpp":
        return "#pragma once\n"
    if relative == "core/layout/soa_columns.hpp":
        return "#pragma once\n#include <cstddef>\nnamespace uarch::layout { template<class T, auto... M> concept SoaMember = true; template<class T, std::size_t N> class SoaColumns; }\n"
    if relative == "core/flow/reduce_lanes.hpp":
        return text.replace(
            "template for (constexpr auto lane : detail::lane_indices<N>)",
            "for (std::size_t lane = 0; lane < N; ++lane)",
        )
    if relative == "tiers/tier_id.hpp":
        entries = re.findall(r"(\w+)\[\[=\s*(TierMeta\{[^}]+\})\]\]", text)
        if not entries:
            raise ValueError("Cannot stage uarch tier metadata: unsupported declaration layout")
        start, end = text.index("namespace detail {"), text.index("}  // namespace detail")
        metadata = "inline constexpr std::array kMetadata{" + ",".join(value for _, value in entries) + "};\n"
        ids = "inline constexpr std::array kTierIds{" + ",".join("TierId::" + name for name, _ in entries) + "};\n"
        return text[:start] + "namespace detail {\n" + metadata + ids + text[end:]
    if relative == "arch/tier.hpp":
        pattern = r"template for \(constexpr auto e[^\n]+\) \{\s*if \(self.id_ == \[:e:\]\)\s*return [^;]+;\s*\}"
        names = ["Scalar", "X86V3", "X86V3SlowPdep", "X86V4", "Aarch64Neon"]
        cases = (
            "switch (self.id_) {"
            + "".join(
                f"case tiers::TierId::{name}: return std::forward<Visitor>(visitor)(detail::TokenFactory::make<{name}>());"
                for name in names
            )
            + "}"
        )
        return re.sub(pattern, lambda _: cases, text)
    if relative in {"core/bits/bit_word.hpp", "core/bits/bit_pattern.hpp"}:
        if relative.endswith("bit_word.hpp"):
            begin, end = text.index("template <class Spec>"), text.index("class BitWord {")
            text = (
                text[:begin]
                + """template<class Spec> consteval std::string audit_spec() { return {}; }
template<class T> struct StagedMember { using Value = T; unsigned low, width; bool flag; constexpr bool operator==(const StagedMember&) const = default; };
namespace detail {
template<class Spec, auto M> concept OwnMember = true;
template<class Spec, auto M> consteval auto Descriptor() { return M; }
template<auto M> using MemberValue = typename decltype(M)::Value;
}
template<class Spec>
"""
                + text[end:]
            )
        text = text.replace("std::meta::info", "auto")
        text = text.replace(
            "static_assert(audit_spec<Spec>().empty(), reflect::freeze_string(audit_spec<Spec>()));",
            "static_assert(audit_spec<Spec>().empty());",
        )
        text = re.sub(
            r"reflect::annotation_count_of<(Flag|Field)>\((\w+)\)",
            lambda m: f"({m[2]}.flag ? {('1 : 0' if m[1] == 'Flag' else '0 : 1')})",
            text,
        )
    return text
