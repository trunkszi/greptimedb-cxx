"""Shared CLI command fixtures."""

import json

import pytest
from invoke import Result

from cli import cmd_test, config, preset, utils
from cli.output import Output

REPOSITORY = config.PROJECT_ROOT


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Keep command and cleanup tests independent of the actual checkout/build tree."""
    (tmp_path / "CMakePresets.json").write_text((REPOSITORY / "CMakePresets.json").read_text())
    for module in (config, preset, utils, cmd_test):
        monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    return tmp_path


@pytest.fixture
def commands(project, monkeypatch):
    """Record child argv; model CTest discovery without requiring a C++ build."""
    calls = []
    tests = [
        {"name": "Types.Ready", "command": [str(project / "bin/test_core_types")]},
        {"name": "Types.Pending", "command": [str(project / "bin/test_core_types")]},
        {"name": "Router.Dispatch", "command": [str(project / "bin/test_router")]},
    ]

    def fake_run(self, cmd, **kwargs):
        calls.append(cmd)
        stdout = json.dumps({"tests": tests}) if "--show-only=json-v1" in cmd else ""
        return Result(exited=0, stdout=stdout, stderr="")

    monkeypatch.setattr(Output, "run_command", fake_run)
    return calls
