"""Exercise source guardrails against isolated synthetic repository trees."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    script = tmp_path / ".github/scripts/check-source-contracts.sh"
    script.parent.mkdir(parents=True)
    repository = Path(__file__).resolve().parents[3]
    shutil.copy2(repository / ".github/scripts/check-source-contracts.sh", script)
    (tmp_path / "src").mkdir()
    (tmp_path / "vcpkg.json").write_text('{"dependencies": ["fmt"]}', encoding="utf-8")
    return tmp_path


def check(tree: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(tree / ".github/scripts/check-source-contracts.sh")],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "code",
    [
        "std::function<void()> callback;",
        "std /* comment */ ::\nthread worker;",
        "std::mutex lock;",
        "std::shared_mutex lock;",
        "std::shared_ptr<int> value;",
        "std::unordered_map<int, int> values;",
        "std::map<int, int> values;",
        "std::set<int> values;",
        "co_await ready;",
        "co_yield item;",
        "co_return;",
        "throw error;",
        "try\n{ work(); }",
        "catch\n(...) {}",
        "1+co_await ready;",
        "1+std::thread{};",
        "std\\\n::function<void()> callback;",
    ],
)
def test_banned_code_is_rejected(source_tree: Path, code: str) -> None:
    (source_tree / "src/check.cc").write_text(code + "\n", encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert "src/check.cc:1: banned production primitive" in result.stderr


def test_comments_literals_and_identifiers_are_allowed(source_tree: Path) -> None:
    source = r"""// std::thread, co_await, throw error
/* std::mutex; try { } catch (...) {} */
const char* ordinary = "std::function<void()>";
const char* raw = R"tag(std::shared_mutex " co_yield)tag";
const auto wide = LR"(co_return throw)";
const char quote = '\'';
auto digits = 1'234 + 0x1'FF;
int thread_counter;
std::thread::id owner = std::this_thread::get_id();
std /* comment */ :: thread :: id another_owner;
// continued comment \
std::thread hidden_in_comment;
"""
    (source_tree / "src/check.cc").write_text(source, encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("source", ['R"tag(unterminated', "/* unclosed comment"])
def test_unterminated_code_regions_fail_closed(source_tree: Path, source: str) -> None:
    (source_tree / "src/check.cc").write_text(source, encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert "unterminated C++" in result.stderr


@pytest.mark.parametrize("directory", ["src", "tests", "examples", "benches", "cmake", "scripts"])
@pytest.mark.parametrize("filename", ["check.h", "check.cc", "check.py", "CMakeLists.txt"])
def test_line_limit_boundaries(source_tree: Path, directory: str, filename: str) -> None:
    target = source_tree / directory / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n" * 599, encoding="utf-8")
    assert check(source_tree).returncode == 0
    target.write_text("\n" * 600, encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert f"{directory}/{filename}: 600 lines" in result.stderr


def test_root_cmake_and_github_scripts_use_the_line_limit(source_tree: Path) -> None:
    assert check(source_tree).returncode == 0
    (source_tree / "CMakeLists.txt").write_text("\n" * 600, encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert "CMakeLists.txt: 600 lines" in result.stderr
    (source_tree / "CMakeLists.txt").write_text("\n" * 599, encoding="utf-8")
    extra = source_tree / ".github/scripts/extra.sh"
    extra.write_text("\n" * 600, encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert ".github/scripts/extra.sh: 600 lines" in result.stderr


@pytest.mark.parametrize(
    "dependency",
    ["arrow", "grpc", "grpc++", "folly", "boost", "boost-asio", "openssl", "nghttp2", "c-ares"],
)
@pytest.mark.parametrize("as_object", [False, True])
@pytest.mark.parametrize("feature", [False, True])
def test_banned_dependencies(source_tree: Path, dependency: str, as_object: bool, feature: bool) -> None:
    entry = {"name": dependency, "platform": "linux"} if as_object else dependency
    data = {"features": {"test": {"dependencies": [entry]}}} if feature else {"dependencies": [entry]}
    (source_tree / "vcpkg.json").write_text(json.dumps(data), encoding="utf-8")
    result = check(source_tree)
    assert result.returncode == 1
    assert f"banned dependency {dependency}" in result.stderr


@pytest.mark.parametrize(
    "manifest",
    ["{", "[]", '{"dependencies": {}}', '{"dependencies": [{}]}', '{"features": []}'],
)
def test_invalid_manifests_fail_closed(source_tree: Path, manifest: str) -> None:
    (source_tree / "vcpkg.json").write_text(manifest, encoding="utf-8")
    assert check(source_tree).returncode == 1


def test_only_production_code_checks_primitives(source_tree: Path) -> None:
    target = source_tree / "tests/example.cc"
    target.parent.mkdir()
    target.write_text("std::thread test_worker;\n", encoding="utf-8")
    assert check(source_tree).returncode == 0


def test_local_python_environment_is_excluded(source_tree: Path) -> None:
    target = source_tree / "scripts/cli/.venv/third_party.py"
    target.parent.mkdir(parents=True)
    target.write_text("\n" * 600, encoding="utf-8")
    assert check(source_tree).returncode == 0


def test_greptime_proto_gen_preserves_unmanaged_user_directory(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[3]
    proto_gen = repository / "cmake/GreptimeProtoGen.cmake"
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "CMakeLists.txt").write_text(
        f'cmake_minimum_required(VERSION 4.0)\nproject(p NONE)\ninclude("{proto_gen}")\ngreptimedb_resolve_proto_root(OUT)\n',
        encoding="utf-8",
    )

    user_dir = tmp_path / "mywork"
    user_dir.mkdir()
    notes = user_dir / "notes.txt"
    notes.write_text("do not delete", encoding="utf-8")
    res = subprocess.run(
        ["cmake", "-S", str(proj), "-B", str(tmp_path / "b1"), f"-DGREPTIME_PROTO_DIR={user_dir}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode != 0
    assert "refusing to overwrite non-empty unmanaged directory" in " ".join(res.stderr.split())
    assert notes.read_text(encoding="utf-8") == "do not delete"

    corrupt_git = tmp_path / "corrupt_git"
    (corrupt_git / ".git").mkdir(parents=True)
    keepme = corrupt_git / "keepme.txt"
    keepme.write_text("keep", encoding="utf-8")
    res2 = subprocess.run(
        ["cmake", "-S", str(proj), "-B", str(tmp_path / "b2"), f"-DGREPTIME_PROTO_DIR={corrupt_git}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res2.returncode != 0
    assert "refusing to overwrite non-empty unmanaged directory" in " ".join(res2.stderr.split())
    assert keepme.read_text(encoding="utf-8") == "keep"


def test_greptime_proto_gen_recovers_managed_half_clone(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[3]
    proto_gen = repository / "cmake/GreptimeProtoGen.cmake"
    remote = tmp_path / "remote"
    (remote / "proto/greptime/v1").mkdir(parents=True)
    (remote / "proto/greptime/v1/common.proto").write_text('syntax = "proto3";\n', encoding="utf-8")
    subprocess.run(["git", "init", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(remote), "config", "user.email", "t@e.st"], check=True)
    subprocess.run(["git", "-C", str(remote), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(remote), "add", "."], check=True)
    subprocess.run(["git", "-C", str(remote), "commit", "-m", "init"], check=True, capture_output=True)
    rev = subprocess.run(
        ["git", "-C", str(remote), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()

    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "CMakeLists.txt").write_text(
        f'cmake_minimum_required(VERSION 4.0)\nproject(p NONE)\ninclude("{proto_gen}")\n'
        f'set(GREPTIME_PROTO_REVISION "{rev}")\n'
        f'set(GREPTIME_PROTO_GIT_REPOSITORY "file://{remote}")\n'
        "greptimedb_resolve_proto_root(OUT)\n",
        encoding="utf-8",
    )
    managed = tmp_path / "managed_cache"
    (managed / ".git").mkdir(parents=True)
    (managed / ".git/greptimedb-proto-managed").write_text("partial\n", encoding="utf-8")
    (managed / "stale.txt").write_text("stale", encoding="utf-8")
    res = subprocess.run(
        ["cmake", "-S", str(proj), "-B", str(tmp_path / "b3"), f"-DGREPTIME_PROTO_DIR={managed}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, res.stderr
    assert (managed / "proto/greptime/v1/common.proto").is_file()
    assert not (managed / "stale.txt").exists()
    assert (managed / ".git/greptimedb-proto-managed").is_file()
