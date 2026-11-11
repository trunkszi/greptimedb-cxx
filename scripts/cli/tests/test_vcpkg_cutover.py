"""Manifest contracts, native generator selection and overlay preservation."""

import json
import os
import subprocess
from pathlib import Path

import pytest

REPOSITORY = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("preset", "toolchain", "triplet"),
    [
        ("gcc-glibc", "gcc-glibc", "x64-linux-gcc-glibc"),
        ("clang-glibc", "clang-glibc", "x64-linux-clang-glibc"),
        ("gcc-musl", "gcc-musl", "x64-linux-gcc-musl"),
        ("clang-musl", "clang-musl", "x64-linux-clang-musl"),
        ("gcc-asan", "gcc-glibc", "x64-linux-gcc-glibc-asan"),
        ("clang-asan", "clang-glibc", "x64-linux-clang-glibc-asan"),
    ],
)
def test_manifest_preset_contract(preset, toolchain, triplet):
    manifest = json.loads((REPOSITORY / "CMakePresets.json").read_text())
    entry = next(p for p in manifest["configurePresets"] if p["name"] == preset)
    cache = entry["cacheVariables"]
    assert manifest["cmakeMinimumRequired"] == {"major": 4, "minor": 0, "patch": 0}
    assert cache["VCPKG_TARGET_TRIPLET"] == triplet
    assert cache["VCPKG_HOST_TRIPLET"] == "x64-linux-gcc-glibc"
    assert cache["VCPKG_CHAINLOAD_TOOLCHAIN_FILE"] == f"${{sourceDir}}/cmake/toolchain/{toolchain}.cmake"
    assert cache["CMAKE_TOOLCHAIN_FILE"].endswith("/.vcpkg/scripts/buildsystems/vcpkg.cmake")
    assert cache["VCPKG_OVERLAY_PORTS"] == "$penv{HOME}/.local/vcpkg"
    assert cache["VCPKG_INSTALLED_DIR"] == "${sourceDir}/build/${presetName}/vcpkg_installed"
    assert cache["CMAKE_MAP_IMPORTED_CONFIG_DEBUG"] == "Release;None;"
    assert cache["CMAKE_CXX_SCAN_FOR_MODULES"] == "OFF"
    assert "detect_container_overflow=0" not in (REPOSITORY / "CMakeLists.txt").read_text()
    if preset == "gcc-asan":
        for name in ("CMAKE_C_FLAGS_DEBUG", "CMAKE_CXX_FLAGS_DEBUG"):
            assert "-fsanitize=address,undefined -fno-sanitize=vptr" in cache[name]
        triplet_text = (REPOSITORY / "cmake/triplets/x64-linux-gcc-glibc-asan.cmake").read_text()
        for name in ("VCPKG_C_FLAGS", "VCPKG_CXX_FLAGS"):
            flags = next(line for line in triplet_text.splitlines() if line.startswith(f"set({name} "))
            assert "-fsanitize=address,undefined -fno-sanitize=vptr" in flags


@pytest.mark.parametrize("present", [True, False])
def test_host_generator_selection(tmp_path, present):
    installed = tmp_path / "installed with spaces"
    host = installed / "x64-linux-gcc-glibc/tools"
    if present:
        for tool in ("grpcxx/protoc-gen-grpcxx", "protobuf/protoc"):
            path = host / tool
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
    script = tmp_path / "host-tools.cmake"
    script.write_text(
        f'set(_VCPKG_INSTALLED_DIR "{installed}")\n'
        'set(VCPKG_HOST_TRIPLET "x64-linux-gcc-glibc")\n'
        f'include("{REPOSITORY}/cmake/GreptimeHostTools.cmake")\n'
        "greptimedb_find_host_tool(plugin tools/grpcxx/protoc-gen-grpcxx)\n"
        "greptimedb_find_host_tool(protoc tools/protobuf/protoc)\n"
        f'file(WRITE "{tmp_path}/result.txt" "${{plugin}}\\n${{protoc}}\\n")\n'
    )
    subprocess.run(["cmake", "-P", str(script)], check=True, capture_output=True, text=True)
    actual = (tmp_path / "result.txt").read_text().splitlines()
    assert actual == ([str(host / "grpcxx/protoc-gen-grpcxx"), str(host / "protobuf/protoc")] if present else ["", ""])


@pytest.mark.parametrize(
    "port", ["uarch", "uring", "c-ares", "cares", "aws-lc", "nexora", "abseil", "protobuf", "fory", "grpcxx"]
)
@pytest.mark.parametrize("partial", [False, True])
def test_overlay_directory_contract(tmp_path, port, partial):
    registry = tmp_path / "registry"
    source = registry / "ports" / port
    source.mkdir(parents=True)
    (source / "portfile.cmake").write_text("vcpkg_cmake_configure(OPTIONS -DCARES_BUILD_TESTS=OFF)\n")
    (source / "vcpkg.json").write_text(json.dumps({"name": port}))
    overlay = tmp_path / "overlay"
    target = overlay / port
    target.mkdir(parents=True)
    if partial:
        (target / "vcpkg.json").write_text(json.dumps({"name": port, "version-string": "customized"}))
    before = {p.name: p.read_bytes() for p in target.iterdir()}
    function = "ensure_c_ares_single_thread_port" if port == "c-ares" else f"ensure_{port.replace('-', '_')}_port"
    script = f'source "{REPOSITORY}/.github/scripts/setup-vcpkg-overlay.sh"\n'
    script += f'{function} "{overlay}"\n'
    subprocess.run(["bash", "-c", script], env={**os.environ, "VCPKG_ROOT": str(registry)}, check=True)
    if partial:
        assert {p.name: p.read_bytes() for p in target.iterdir()} == before
        assert not (target / "portfile.cmake").exists()
    else:
        assert (target / "portfile.cmake").is_file()
        assert json.loads((target / "vcpkg.json").read_text())["name"] == port
        assert not (target / port).exists()


def test_port_propagates_host_triplet(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "LICENSE").touch()
    script = tmp_path / "port-host.cmake"
    script.write_text(
        "cmake_minimum_required(VERSION 4.0)\n"
        'set(HOST_TRIPLET "x64-linux-gcc-glibc")\n'
        f'set(mock_source "{source}")\n'
        f'set(CURRENT_PACKAGES_DIR "{tmp_path / "packages"}")\n'
        'set(PORT "greptimedb")\n'
        "function(vcpkg_check_linkage)\nendfunction()\n"
        "function(vcpkg_from_git)\n"
        '  set(${ARGV1} "${mock_source}" PARENT_SCOPE)\n'
        "endfunction()\n"
        "function(vcpkg_cmake_configure)\n"
        '  if(NOT "-DVCPKG_HOST_TRIPLET=${HOST_TRIPLET}" IN_LIST ARGN)\n'
        '    message(FATAL_ERROR "Missing host triplet for protocol generators")\n'
        "  endif()\n"
        "endfunction()\n"
        "function(vcpkg_cmake_install)\nendfunction()\n"
        "function(vcpkg_cmake_config_fixup)\nendfunction()\n"
        "function(vcpkg_install_copyright)\nendfunction()\n"
        f'include("{REPOSITORY / "port/portfile.cmake.in"}")\n'
    )
    subprocess.run(["cmake", "-P", str(script)], check=True, capture_output=True, text=True)


def test_dependency_setup_creates_caches_before_toolchain(tmp_path):
    functions = (
        "ensure_uarch_port",
        "ensure_uring_port",
        "ensure_c_ares_single_thread_port",
        "ensure_cares_port",
        "ensure_aws_lc_port",
        "ensure_nexora_port",
        "ensure_abseil_port",
        "ensure_protobuf_port",
        "ensure_fory_port",
        "ensure_grpcxx_port",
    )
    script = f'source "{REPOSITORY}/.github/scripts/setup-ci-deps.sh"\n'
    script += 'install_one_toolchain() { test -d "$HOME/.cache/vcpkg/archives" && test -d "$HOME/.cache/ccache"; }\n'
    script += "\n".join(f"{name}() {{ :; }}" for name in functions)
    script += "\nmain\n"
    subprocess.run(["bash", "-c", script], env={**os.environ, "HOME": str(tmp_path)}, check=True)


@pytest.mark.parametrize("port", ["abseil", "protobuf"])
def test_registry_overlay_fills_empty_directory(tmp_path, port):
    registry = tmp_path / "registry"
    source = registry / "ports" / port
    source.mkdir(parents=True)
    (source / "portfile.cmake").write_text("vcpkg_cmake_configure(SOURCE_PATH source)\n")
    (source / "vcpkg.json").write_text(json.dumps({"name": port}))
    overlay = tmp_path / "overlay"
    target = overlay / port
    target.mkdir(parents=True)
    script = f'source "{REPOSITORY}/.github/scripts/setup-vcpkg-overlay.sh"\n'
    script += f'ensure_{port}_port "{overlay}"\n'
    env = {**os.environ, "VCPKG_ROOT": str(registry)}
    subprocess.run(["bash", "-c", script], env=env, check=True)
    assert (target / "portfile.cmake").is_file()
    assert not (target / port).exists()
    if port == "protobuf":
        assert "libprotoc" in json.loads((target / "vcpkg.json").read_text())["features"]
    else:
        assert "__builtin_is_constant_evaluated" in (target / "portfile.cmake").read_text()
    customized = (target / "portfile.cmake").read_text() + "# local customization\n"
    (target / "portfile.cmake").write_text(customized)
    subprocess.run(["bash", "-c", script], env=env, check=True)
    assert (target / "portfile.cmake").read_text() == customized


def test_dependency_setup_uses_overlay_override(tmp_path):
    overlay = tmp_path / "configured-overlay"
    home = tmp_path / "user"
    functions = (
        "ensure_uarch_port",
        "ensure_uring_port",
        "ensure_c_ares_single_thread_port",
        "ensure_cares_port",
        "ensure_aws_lc_port",
        "ensure_nexora_port",
        "ensure_abseil_port",
        "ensure_protobuf_port",
        "ensure_fory_port",
        "ensure_grpcxx_port",
    )
    script = f'source "{REPOSITORY}/.github/scripts/setup-ci-deps.sh"\n'
    script += "install_one_toolchain() { :; }\n"
    script += "\n".join(f'{name}() {{ [[ "$1" == "$VCPKG_OVERLAY_PORTS" ]]; }}' for name in functions)
    script += "\nmain\n"
    subprocess.run(
        ["bash", "-c", script],
        env={**os.environ, "HOME": str(home), "VCPKG_OVERLAY_PORTS": str(overlay)},
        check=True,
    )
    assert overlay.is_dir()
    assert not (home / ".local/vcpkg").exists()


def test_ci_apt_mirror_fallback_contract():
    import yaml

    text = (REPOSITORY / ".github/workflows/ci.yaml").read_text()
    workflow = yaml.safe_load(text)
    steps = {step["name"]: step["run"] for step in workflow["jobs"]["build-and-test"]["steps"] if "run" in step}
    provision = steps["Install toolchain and provision missing overlay ports"]
    for option in ('Acquire::Retries "3";', 'Acquire::http::Timeout "15";', 'Acquire::https::Timeout "15";'):
        assert option in provision
    assert provision.count("curl -fsSI --max-time 5") == 2
    assert "[[ -f /etc/apt/apt-mirrors.txt ]]" in provision
    for mirror, priority in [
        ("mirrors.edge.kernel.org", 1),
        ("ubuntu.osuosl.org", 2),
        ("mirror.csclub.uwaterloo.ca", 3),
    ]:
        assert f"https://{mirror}/ubuntu/\\tpriority:{priority}" in provision
    assert provision.index("/etc/apt/apt.conf.d/99ci-network") < provision.index("bash scripts/install-dependencies.sh")
    for forbidden in ("allow-unauthenticated", "AllowInsecureRepositories", "trusted=yes"):
        assert forbidden not in text
    tidy = steps["Install clang-tidy-23"]
    assert "command -v gpg >/dev/null ||" in tidy
    assert tidy.count("apt-get update") == 1
    assert tidy.index("/etc/apt/sources.list.d/llvm.list") < tidy.index("apt-get update")
    assert "apt-get install -y --no-install-recommends clang-tidy-23" in tidy
    assert "/etc/apt/trusted.gpg.d/apt.llvm.org.asc" in tidy
    for shell in steps.values():
        subprocess.run(["bash", "-n"], input=shell, text=True, check=True)
