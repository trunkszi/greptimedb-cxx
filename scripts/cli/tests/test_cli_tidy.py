"""Discriminating CLI, preset, process-output, and launcher bootstrap tests."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from invoke import Result
from typer.testing import CliRunner

from cli import cmd_tidy, config, utils
from cli.__main__ import _with_default_command
from cli.app import app
from cli.output import Output

REPOSITORY = config.PROJECT_ROOT
runner = CliRunner()


def invoke(argv):
    return runner.invoke(app, _with_default_command(argv))


def test_reflection_annotation_staging_preserves_other_code():
    source = 'struct [[= Tags{values[0], "a]]b"}]] Row {\n int value [[= Key{}]];\n};\n'
    staged = cmd_tidy.strip_reflection_annotations(source)
    assert "struct " in staged and " Row {" in staged
    assert " int value " in staged and "[[" not in staged
    assert len(staged) == len(source)
    assert staged.count("\n") == source.count("\n")
    literal = 'const char* name = "[[=not_an_attribute]]"; // [[= comment ]]\n'
    assert cmd_tidy.strip_reflection_annotations(literal) == literal


@pytest.mark.parametrize(
    "syntax",
    [
        "no member named 'meta' in namespace 'std'",
        "blocks support disabled",
        "iterating expansion statements are not yet supported",
    ],
)
def test_reflection_parser_noise_is_classified(syntax):
    output = f"src/router.h:12:15: error: {syntax} [clang-diagnostic-error]\n"
    assert cmd_tidy._classify_clang_diagnostics(output) == (True, [])
    assert cmd_tidy._tidy_failure(output, 1) == (False, "")
    assert cmd_tidy._tidy_failure(output, -9)[0]


@pytest.mark.parametrize(
    "diagnostic",
    [
        "error: invalid value 'c++99' in '-std=c++99' [clang-diagnostic-error]",
        "error: no input files [clang-diagnostic-error]",
        "src/x.cc:3:1: error: use of undeclared identifier 'bogus_symbol' [clang-diagnostic-error]",
        "tests/x.cc:3:1: warning: moved [bugprone-use-after-move]",
        "benches/x.cc:3:1: warning: unused [clang-analyzer-deadcode.DeadStores]",
    ],
)
def test_genuine_failures_survive_reflection_noise(diagnostic):
    noise = "src/router.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    failed, shown = cmd_tidy._tidy_failure(noise + diagnostic, 1)
    assert failed and diagnostic in shown
    assert cmd_tidy._tidy_failure("", 1)[0]


def test_reflection_identifier_requires_declaration_evidence(tmp_path):
    source = tmp_path / "row.cc"
    source.write_text(
        'struct [[= table{"metrics"}]] Metric {};\nusing Batch = TypedColumnarBatch<Metric>;\nBatch batch{16};\n'
        + "\n" * 60
    )
    reflection = f"{source}:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    for name, expected in [("Metric", False), ("Batch", False), ("batch", False), ("bogus_symbol", True)]:
        output = reflection + f"{source}:60:1: error: use of undeclared identifier '{name}' [clang-diagnostic-error]\n"
        assert cmd_tidy._tidy_failure(output, 1)[0] is expected


@pytest.mark.parametrize("variable", ["GREPTIMEDB_CLANG_TIDY", "GRPCXX_CLANG_TIDY", "CLANG_TIDY"])
def test_tidy_override_is_explicit(monkeypatch, tmp_path, variable):
    for name in ["GREPTIMEDB_CLANG_TIDY", "GRPCXX_CLANG_TIDY", "CLANG_TIDY"]:
        monkeypatch.delenv(name, raising=False)
    tool = tmp_path / "clang-tidy"
    tool.write_text("#!/bin/sh\nexit 0\n")
    tool.chmod(0o755)
    monkeypatch.setenv(variable, str(tool))
    assert utils.find_clang_tidy() == str(tool)
    monkeypatch.setenv(variable, str(tool) + "-missing")
    with pytest.raises(ValueError):
        utils.find_clang_tidy()


def test_annotation_staging_handles_digit_separators_and_raw_strings():
    text = 'auto n = 10\'000; auto s = R"tag([[= fake "quoted"]])tag";\nstruct [[= Tag{}]] Row {};'
    staged = cmd_tidy.strip_reflection_annotations(text)
    assert staged.splitlines()[0] == text.splitlines()[0]
    assert "[[= Tag{}]]" not in staged


def test_external_deprecation_never_hides_project_errors(project):
    dependency = "/sdk/protobuf/arena.h:5:1: warning: old type [clang-diagnostic-deprecated-declarations]\n"
    assert cmd_tidy._tidy_failure(dependency, 0) == (False, "")
    error = f"{project}/src/row.cc:40:1: error: use of undeclared identifier 'bogus_symbol' [clang-diagnostic-error]\n"
    assert cmd_tidy._tidy_failure(dependency + error, 1)[0]


def test_clang_database_retains_native_flags_and_rejects_missing_commands(project):
    source = project / "src/x.cc"
    entry = {
        "directory": str(project),
        "file": str(source),
        "arguments": ["clang++", "-std=c++26", "-freflection-latest", "-DFMT_HEADER_ONLY=1", "-c", str(source)],
    }
    translated = cmd_tidy.clang_database([entry, entry], "clang-tidy", capable=False)
    assert len(translated) == 1
    assert "-freflection-latest" not in translated[0]["arguments"]
    assert "-DFMT_HEADER_ONLY=1" not in translated[0]["arguments"]
    native = cmd_tidy.clang_database([entry], "clang-tidy", capable=True)
    assert "-DFMT_HEADER_ONLY=1" in native[0]["arguments"]
    with pytest.raises(ValueError, match="Missing compiler command"):
        cmd_tidy.clang_database([{"directory": str(project), "file": str(source)}], "clang-tidy")


def test_tidy_reports_all_selected_translation_units(project, monkeypatch):
    directory = project / "build/gcc-glibc"
    directory.mkdir(parents=True)
    source = project / "src"
    source.mkdir()
    entries = []
    for name in ["one.cc", "two.cc"]:
        file = source / name
        file.write_text("int ready;\n")
        entries.append({"directory": str(project), "file": str(file), "arguments": ["clang++", "-c", str(file)]})
    (directory / "compile_commands.json").write_text(json.dumps(entries))
    monkeypatch.setattr(cmd_tidy, "find_clang_tool", lambda name: name)
    monkeypatch.setattr(cmd_tidy, "is_reflection_clang_tidy", lambda tool, directory=None: False)
    visited = []

    def error(self, args, **kwargs):
        visited.append(args[3])
        return Result(exited=1, stdout="error: unknown argument: -bad-driver-flag\n", stderr="")

    monkeypatch.setattr(Output, "run_command", error)
    assert invoke(["tidy", "src"]).exit_code == 1
    assert len(visited) == 2


@pytest.mark.parametrize(
    "filename", ["src/a.cc", "examples/a.cc", "benches/a.cc", "tests_elsewhere/a.cc", "tests/../src/a.cc"]
)
def test_production_tidy_preserves_every_checker(project, filename):
    assert cmd_tidy.tidy_checks(project / filename) == "--checks=clang-analyzer-*,bugprone-use-after-move"


def test_negative_fixture_tidy_excludes_only_three_checks(project):
    assert cmd_tidy.tidy_checks(project / "tests/unit/a.cc") == (
        "--checks=clang-analyzer-*,bugprone-use-after-move,-bugprone-use-after-move,"
        "-clang-analyzer-cplusplus.Move,-clang-analyzer-optin.core.EnumCastOutOfRange"
    )


@pytest.mark.parametrize("version", ["4.4.5", "4.5.0", "5.0.0"])
def test_launcher_preserves_cmake_from_path(tmp_path, version):
    home = tmp_path / "home"
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ["dirname", "mkdir"]:
        (tools / name).symlink_to(shutil.which(name))
    cmake = tools / "cmake"
    cmake.write_text(f'#!/bin/sh\necho "cmake version {version}"\n')
    cmake.chmod(0o755)
    uv = home / ".local/bin/uv"
    uv.parent.mkdir(parents=True)
    uv.write_text("#!/bin/sh\ncommand -v cmake\n")
    uv.chmod(0o755)
    env = {**os.environ, "HOME": str(home), "PATH": str(tools)}
    result = subprocess.run(
        ["/bin/bash", str(REPOSITORY / "build.py"), "--help"], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(cmake)


@pytest.mark.parametrize("filename", ["src/annotations.h", "tests/units/row/test_reflected_row.cc"])
@pytest.mark.parametrize("severity", ["fatal error", "error"])
def test_missing_headers_are_never_reflection_noise(filename, severity):
    output = f"{filename}:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    output += f"{filename}:8:1: {severity}: 'review_missing_header.h' file not found [clang-diagnostic-error]\n"
    assert cmd_tidy._classify_clang_diagnostics(output)[1]
    assert cmd_tidy._tidy_failure(output, 1)[0]


def test_staging_covers_external_and_generated_include_roots(project):
    source = project / "src/client.cc"
    source.parent.mkdir()
    source.write_text("int ready;\n")
    external = project.parent / (project.name + "-sdk") / "include"
    generated = project / "build/generated"
    for directory in [external, generated]:
        directory.mkdir(parents=True)
        (directory / "api.h").write_text("struct [[= Tag{}]] Api {};\n")
    entries = [
        {
            "directory": str(project),
            "file": str(source),
            "arguments": ["clang++", "-I" + str(generated), "-isystem", str(external), "-c", str(source)],
        }
    ]
    proof = external / "uarch/core/layout/abi_class.hpp"
    proof.parent.mkdir(parents=True)
    proof.write_text("// original proof\n")
    destination = project / "build/stage"
    staged = cmd_tidy.stage_sources(entries, destination)
    arguments = staged[0]["arguments"]
    roots = [Path(arguments[1][2:]), Path(arguments[3])]
    assert "concept NoPadding = true" in (roots[1] / "uarch/core/layout/abi_class.hpp").read_text()
    assert proof.read_text() == "// original proof\n"
    for original, mirror in zip([generated, external], roots, strict=True):
        assert mirror != original and mirror.is_relative_to(destination)
        assert "[[=" not in (mirror / "api.h").read_text()
        assert "[[=" in (original / "api.h").read_text()


def test_analyzer_residual_errors_require_explicit_tu_allowlist(project, monkeypatch):
    path = project / "src/reflected.cc"
    output = "src/annotations.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    default = cmd_tidy._REFLECTION_ONLY_SKIP
    assert cmd_tidy.analyzer_result(path, output, 1, compiler_verified=True) == (False, "", default)
    monkeypatch.setattr(cmd_tidy, "REFLECTION_ANALYZER_SKIP_TUS", {"src/reflected.cc": "Uses ^^"})
    assert cmd_tidy.analyzer_result(path, output, 1, compiler_verified=True) == (False, "", "Uses ^^")
    assert cmd_tidy.analyzer_result(path, "", 0) == (False, "", "")
    assert cmd_tidy.analyzer_result(project / "src/other.cc", output, 1, compiler_verified=True) == (
        False,
        "",
        default,
    )
    missing = "src/annotations.h:2:1: fatal error: 'missing.h' file not found\n"
    assert cmd_tidy.analyzer_result(path, output + missing, 1)[0]


def test_tidy_prints_explicit_analyzer_coverage(project, monkeypatch):
    directory = project / "build/gcc-glibc"
    directory.mkdir(parents=True)
    source = project / "src/reflected.cc"
    source.parent.mkdir()
    source.write_text("int ready;\n")
    entries = [{"directory": str(project), "file": str(source), "arguments": ["clang++", "-c", str(source)]}]
    (directory / "compile_commands.json").write_text(json.dumps(entries))
    monkeypatch.setattr(cmd_tidy, "find_clang_tool", lambda name: name)
    monkeypatch.setattr(cmd_tidy, "is_reflection_clang_tidy", lambda tool, directory=None: False)
    monkeypatch.setattr(cmd_tidy, "REFLECTION_ANALYZER_SKIP_TUS", {"src/reflected.cc": "Uses ^^"})
    noise = "src/annotations.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    monkeypatch.setattr(Output, "run_command", lambda *a, **kw: Result(exited=1, stdout=noise, stderr=""))
    monkeypatch.setattr(
        cmd_tidy.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 0, "", ""),
    )
    result = invoke(["tidy", "src"])
    assert result.exit_code == 0, result.output
    assert "Analyzer SKIPPED src/reflected.cc: Uses ^^" in result.output
    assert "0 checked, 1 reflection skips, 0 failures" in result.output
    assert json.loads((directory / "tidy-coverage.json").read_text())[0]["skip"] == "Uses ^^"
    monkeypatch.setattr(cmd_tidy, "REFLECTION_ANALYZER_SKIP_TUS", {})
    unlisted = invoke(["tidy", "src"])
    assert unlisted.exit_code == 0, unlisted.output
    assert cmd_tidy._REFLECTION_ONLY_SKIP in " ".join(unlisted.output.split())
    assert "0 checked, 1 reflection skips, 0 failures" in unlisted.output


@pytest.mark.parametrize("prefix", ["u8", "u", "U", "L"])
def test_prefixed_character_literals_do_not_hide_later_annotations(prefix):
    text = f"auto c = {prefix}'x'; struct [[= Tag{{}}]] Row {{}};"
    staged = cmd_tidy.strip_reflection_annotations(text)
    assert f"{prefix}'x'" in staged and "[[=" not in staged


def test_uarch_staging_keeps_bits_and_bypasses_only_layout_proofs():
    source = "struct MetaSpec { [[= uarch::Flag{3}]] bool ready{}; }; auto x = w.test<^^MetaSpec::ready>();"
    staged = utils.tidy_bit_members(source)
    assert "StagedMember<decltype(ready)> TidyMember_ready{3, 1, true}" in staged
    assert "w.test<MetaSpec::TidyMember_ready>()" in staged
    layout = utils.tidy_uarch_compat("core/layout/abi_class.hpp", "")
    assert "concept NoPadding = true" in layout and "consteval bool assert_layout()" in layout
    runtime = "int runtime() { return value + 1; }"
    assert utils.tidy_uarch_compat("core/arithmetic/fast_divisor_u64.hpp", runtime) == runtime
    with pytest.raises(ValueError, match="unsupported declaration"):
        utils.tidy_uarch_compat("tiers/tier_id.hpp", "enum class TierId {};")


def test_analyzer_concurrency_obeys_job_limit(monkeypatch):
    import threading
    import time
    from types import SimpleNamespace

    lock = threading.Lock()
    active = 0
    peak = 0

    def analyze(command, **_kwargs):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return SimpleNamespace(returncode=0, stdout=command[-1], stderr="")

    monkeypatch.setattr(cmd_tidy.os, "cpu_count", lambda: 64)
    monkeypatch.setattr(cmd_tidy.subprocess, "run", analyze)
    results = cmd_tidy.run_tidy_commands([["analyzer", str(i)] for i in range(8)], Output())
    assert [item.stdout for item in results] == [str(i) for i in range(8)]
    assert peak <= config.DEFAULT_JOBS


@pytest.mark.parametrize("kind,arguments,descriptor", [("Flag", "3", "3, 1, true"), ("Field", "0, 22", "0, 22, false")])
@pytest.mark.parametrize("placement", ["prefix", "postfix"])
@pytest.mark.parametrize("qualifier", ["uarch::", "uarch::bits::", ""])
def test_bit_descriptor_placements(kind, arguments, descriptor, placement, qualifier):
    annotation = f"[[= {qualifier}{kind}{{{arguments}}}]]"
    declaration = (
        f"{annotation} std::uint32_t slot{{}};" if placement == "prefix" else f"std::uint32_t slot{annotation}{{}};"
    )
    source = f"struct TokenSpec {{ {declaration} }}; auto x = ^^TokenSpec::slot;"
    staged = utils.tidy_bit_members(source)
    assert declaration in staged
    assert f"StagedMember<decltype(slot)> TidyMember_slot{{{descriptor}}};" in staged
    assert "auto x = TokenSpec::TidyMember_slot;" in staged


@pytest.mark.parametrize(
    "syntax",
    [
        "#include <meta>",
        "template for (auto x : members) {}",
        "auto x = ^^int;",
        "using T = [: info :];",
        "int x [[= Tag{}]];",
    ],
)
def test_header_recovery_requires_direct_reflection(tmp_path, syntax):
    header = tmp_path / "dispatcher.hpp"
    header.write_text(syntax)
    recovery = f"{header}:32:18: error: variable has incomplete type 'const void' [clang-diagnostic-error]\n"
    direct = "dependency.hpp:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    assert not cmd_tidy._classify_clang_diagnostics(recovery)[0]
    assert cmd_tidy._classify_clang_diagnostics(direct + recovery) == (True, [])
    plain = tmp_path / "plain.hpp"
    plain.write_text("int value;\n")
    genuine = recovery.replace(str(header), str(plain))
    assert not cmd_tidy._classify_clang_diagnostics(direct + genuine)[0]
    undeclared = f"{header}:80:1: error: use of undeclared identifier 'bogus_symbol' [clang-diagnostic-error]\n"
    assert not cmd_tidy._classify_clang_diagnostics(direct + undeclared)[0]
