"""Configure the workspace with host dependencies or the vcpkg manifest."""

import typer

from .cmd_build import ensure_configured
from .output import Output
from .utils import run_action, selection


def setup_cmd(
    ctx: typer.Context,
    vcpkg: bool = typer.Option(False, "--vcpkg", help="Use gcc-glibc-vcpkg, overriding global preset/compiler"),
) -> None:
    """Configure dependencies without compiling targets or modifying sibling sources."""
    def action(out: Output) -> int:
        preset = "gcc-glibc-vcpkg" if vcpkg else selection(ctx, None, None)[0]
        return ensure_configured(preset, out)

    run_action(ctx, action)
