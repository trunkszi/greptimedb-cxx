"""Parallel analyzer execution preserves command order and failures."""

import json
import subprocess
import threading

from invoke import Result
from typer.testing import CliRunner

from cli import cmd_tidy
from cli.__main__ import _with_default_command
from cli.app import app
from cli.output import Output

runner = CliRunner()


def invoke(argv):
    return runner.invoke(app, _with_default_command(argv))


def test_parallel_analyzers_keep_results_in_input_order(monkeypatch):
    second_started = threading.Event()

    def run(command, **kwargs):
        if command[-1] == "first":
            assert second_started.wait(5), "translation units ran sequentially"
        else:
            second_started.set()
        return subprocess.CompletedProcess(command, 3 if command[-1] == "second" else 0, command[-1], "")

    monkeypatch.setattr(cmd_tidy.subprocess, "run", run)
    results = cmd_tidy.run_tidy_commands([["tidy", "first"], ["tidy", "second"]], Output())
    assert [result.stdout for result in results] == ["first", "second"]
    assert [result.return_code for result in results] == [0, 3]


def test_reflection_only_parser_errors_skip_unlisted_units(project):
    path = project / "src/transitive.cc"
    output = "src/annotations.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    assert cmd_tidy.analyzer_result(path, output, 1)[0] is True
    failed, display, reason = cmd_tidy.analyzer_result(path, output, 1, compiler_verified=True)
    assert failed is False
    assert display == ""
    assert reason == cmd_tidy._REFLECTION_ONLY_SKIP


def test_non_reflection_diagnostics_remain_failures(project):
    path = project / "src/transitive.cc"
    error = "src/transitive.cc:1:1: error: use of undeclared identifier 'foo' [clang-diagnostic-error]\n"
    warning = "src/transitive.cc:4:1: warning: Use of memory after it is moved [clang-analyzer-cplusplus.Move]\n"
    reflection = "src/annotations.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"
    assert cmd_tidy.analyzer_result(path, error, 1)[0] is True
    assert cmd_tidy.analyzer_result(path, warning, 1)[0] is True
    assert cmd_tidy.analyzer_result(path, reflection + warning, 1)[0] is True


def test_capability_probe_stays_in_project_build(monkeypatch, tmp_path):
    monkeypatch.setattr(cmd_tidy.utils, "PROJECT_ROOT", tmp_path)
    observed = []

    def compile_probe(command, **kwargs):
        probe = cmd_tidy.Path(command[1])
        assert probe.is_relative_to(tmp_path / "build")
        assert "^^int" in probe.read_text()
        observed.append(probe)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(cmd_tidy.subprocess, "run", compile_probe)
    assert cmd_tidy.is_reflection_clang_tidy("clang-tidy")
    assert cmd_tidy.is_reflection_clang_tidy("clang-tidy", tmp_path / "build/selected")
    assert len(observed) == 2 and all(not probe.exists() for probe in observed)


_META = "src/annotations.h:1:1: error: no member named 'meta' in namespace 'std' [clang-diagnostic-error]\n"


def test_syntax_only_arguments_keep_the_compiler_and_sources():
    arguments = ["ccache", "clang++", "-c", "-MD", "src/a.cc", "-MF", "a.d", "-o", "a.o", "-MT", "a.o", "-MQ", "a.o"]
    assert cmd_tidy._syntax_only_arguments(arguments) == ["clang++", "src/a.cc", "-Werror", "-fsyntax-only"]


def _prepare_reflection_tidy(project, monkeypatch, tidy_output, syntax_code, syntax_stderr):
    directory = project / "build/gcc-glibc"
    directory.mkdir(parents=True)
    source = project / "src/reflected.cc"
    source.parent.mkdir(exist_ok=True)
    source.write_text("int ready;\n")
    arguments = ["ccache", "clang++", "-c", "-MD", str(source), "-MF", "a.d", "-o", "a.o", "-MT", "a.o", "-MQ", "a.o"]
    entries = [{"directory": str(project), "file": str(source), "arguments": arguments}]
    (directory / "compile_commands.json").write_text(json.dumps(entries))
    monkeypatch.setattr(cmd_tidy, "find_clang_tool", lambda name: name)
    monkeypatch.setattr(cmd_tidy, "is_reflection_clang_tidy", lambda tool, directory=None: False)
    monkeypatch.setattr(cmd_tidy, "REFLECTION_ANALYZER_SKIP_TUS", {})
    monkeypatch.setattr(Output, "run_command", lambda *args, **kwargs: Result(exited=1, stdout=tidy_output, stderr=""))
    seen = []

    def run(command, **kwargs):
        seen.append((command, kwargs.get("cwd")))
        return subprocess.CompletedProcess(command, syntax_code, "", syntax_stderr)

    monkeypatch.setattr(cmd_tidy.subprocess, "run", run)
    return seen


def test_unknown_type_in_reflection_header_fails_when_native_syntax_fails(project, monkeypatch):
    tidy_output = _META + "src/annotations.h:2:1: error: unknown type name 'Foo' [clang-diagnostic-error]\n"
    assert cmd_tidy._tidy_failure(tidy_output, 1)[0] is False
    native = "src/annotations.h:2:1: error: unknown type name 'Foo'\n"
    seen = _prepare_reflection_tidy(project, monkeypatch, tidy_output, 1, native)
    result = invoke(["tidy", "src"])
    command, cwd = seen[0]
    assert command == ["clang++", str(project / "src/reflected.cc"), "-Werror", "-fsyntax-only"]
    assert cwd == str(project)
    assert result.exit_code != 0
    assert "unknown type name 'Foo'" in result.output


def test_note_into_reflection_header_fails_when_native_syntax_fails(project, monkeypatch):
    tidy_output = (
        _META
        + "src/client.cc:8:3: error: no matching function for call to 'connect' [clang-diagnostic-error]\n"
        + "src/annotations.h:4:5: note: candidate function not viable: requires 2 arguments, but 1 was provided\n"
    )
    assert cmd_tidy._tidy_failure(tidy_output, 1)[0] is False
    native = "src/client.cc:8:3: error: no matching function for call to 'connect'\n"
    seen = _prepare_reflection_tidy(project, monkeypatch, tidy_output, 1, native)
    result = invoke(["tidy", "src"])
    assert seen[0][0][-2:] == ["-Werror", "-fsyntax-only"]
    assert "-c" not in seen[0][0]
    assert result.exit_code != 0
    assert "no matching function for call to 'connect'" in result.output


def test_valid_reflection_tu_skips_when_native_syntax_succeeds(project, monkeypatch):
    assert cmd_tidy._tidy_failure(_META, 1)[0] is False
    seen = _prepare_reflection_tidy(project, monkeypatch, _META, 0, "")
    result = invoke(["tidy", "src"])
    assert seen[0][0][-2:] == ["-Werror", "-fsyntax-only"]
    assert result.exit_code == 0, result.output
    assert cmd_tidy._REFLECTION_ONLY_SKIP in " ".join(result.output.split())
    assert "0 checked, 1 reflection skips, 0 failures" in result.output
