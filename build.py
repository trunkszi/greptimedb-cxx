#!/usr/bin/env bash
# Launch the uv-managed greptimedb CLI from any working directory.
set -euo pipefail

GREPTIMEDB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="${GREPTIMEDB_ROOT}/scripts"
export PATH="$HOME/.local/bin:$PATH"

ensure_uv() {
    local local_bin="${UV_INSTALL_DIR:-$HOME/.local/bin}"
    mkdir -p "$local_bin"
    case ":$PATH:" in
        *":$local_bin:"*) ;;
        *) export PATH="$local_bin:$PATH" ;;
    esac
    if command -v uv >/dev/null 2>&1; then
        return 0
    fi
    echo "[INFO] uv not found; downloading uv (Astral) to ${local_bin}..." >&2
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$local_bin" sh
    hash -r
    if ! command -v uv >/dev/null 2>&1; then
        echo "Error: uv auto-installation failed." >&2
        exit 1
    fi
}
ensure_uv

unset RUST_LOG
export PYTHONPATH="${SCRIPTS_DIR}${PYTHONPATH:+:$PYTHONPATH}"
exec uv run --directory "${SCRIPTS_DIR}/cli" python -m cli "$@"
