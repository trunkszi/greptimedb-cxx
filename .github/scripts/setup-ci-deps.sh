#!/usr/bin/env bash
# Provision build toolchains and missing overlay ports without rebuilding packages.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/setup-vcpkg-overlay.sh"
main() {
    mkdir -p "$HOME/.cache/vcpkg/archives" "$HOME/.cache/ccache"
    CARES_COMMIT="${CARES_COMMIT:-b46378d1977aa73dabf6c52535f61419d0b48063}"
    NEXORA_COMMIT="${NEXORA_COMMIT:-f11700d5c9e8401bd5370871ea0a46c9146994b5}"
    GRPCXX_COMMIT="${GRPCXX_COMMIT:-e6df1b486a7291cb14740a1658c7dacd45b1b1bb}"
    local toolchain="${GREPTIMEDB_TOOLCHAIN:-${GRPCXX_TOOLCHAIN:-gcc-glibc}}"
    install_one_toolchain "$toolchain"
    if [[ "$toolchain" != gcc-glibc ]]; then install_one_toolchain gcc-glibc; fi
    local overlay="${VCPKG_OVERLAY_PORTS:-$HOME/.local/vcpkg}"
    mkdir -p "$overlay"
    ensure_uarch_port "$overlay"
    ensure_uring_port "$overlay"
    ensure_c_ares_single_thread_port "$overlay"
    ensure_cares_port "$overlay"
    ensure_aws_lc_port "$overlay"
    ensure_nexora_port "$overlay"
    ensure_abseil_port "$overlay"
    ensure_protobuf_port "$overlay"
    ensure_fory_port "$overlay"
    ensure_grpcxx_port "$overlay"
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
