"""Repository paths and the public compiler/benchmark registries."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PRESET = "gcc-glibc"
DEFAULT_JOBS = 8
COMPILERS = {"gcc": "gcc-glibc", "clang": "clang-glibc"}
BENCHMARK_TARGETS: dict[str, str] = {
    "": "bench_column_buf",
    "bench_column_buf": "bench_column_buf",
    "bench_reflected_row": "bench_reflected_row",
    "bench_arrow_ipc_codec": "bench_arrow_ipc_codec",
    "bench_streaming_loopback": "bench_streaming_loopback",
}
CPP_EXTENSIONS = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}


def get_build_directory(preset: str) -> Path:
    """Return greptimedb-cxx's preset build directory; callers validate the preset first."""
    return PROJECT_ROOT / "build" / preset
