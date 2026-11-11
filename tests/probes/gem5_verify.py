#!/usr/bin/env python3
"""Tier 1 leaf audit plus gem5 O3CPU and AMDuProf evidence for greptimedb probes."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

COUNTS = (512, 2560, 4608)
PROBES = ("null_mask", "overlaps", "column_append", "wire_row")
SYMBOLS = (
    "greptime_probe_null_mask_is_null",
    "greptime_probe_overlaps",
    "greptime_probe_column_append_i64",
    "greptime_probe_wire_row_null_check",
)
ALIASES = {
    "cycles": ("numCycles",),
    "idle": ("idleCycles",),
    "instructions": ("commitStats0.numInsts", "commit.instsCommitted::total", "committedInsts"),
    "ops": ("commitStats0.numOps", "commit.opsCommitted::total", "committedOps"),
    "renamed": ("rename.renamedInsts", "rename.RenamedInsts"),
    "rename_idle": ("rename.idleCycles", "rename.IdleCycles", "rename.status::Idle"),
    "squash": ("rename.squashCycles", "rename.SquashCycles", "rename.status::Squashing"),
    "branch_misses": ("commit.branchMispredicts", "branchPred.condIncorrect"),
    "memory_violations": ("iew.memOrderViolationEvents",),
}
LLVM = Path("/root/.local/toolchain/clang-glibc/bin")
CLANGXX = Path("/root/.local/toolchain/clang-glibc/bin/clang++")
GEM5 = Path("/opt/gem5/build/ALL/gem5.opt")
O3_CONFIG = Path("/root/uarch/scripts/gem5/o3_sim.py")
PROFILE = "o3-w8-rob448"
CORE = "2"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(command, *, cwd, env, timeout, log: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {command[0]} log={log}")
    return result


def split_symbols(text: str) -> dict[str, str]:
    parts = re.split(r"\n(?=[0-9a-f]+ <[^>]+>:)", text)
    found = {}
    for part in parts:
        match = re.match(r"[0-9a-f]+ <([^>]+)>:", part)
        if match:
            found[match.group(1)] = part
    return found


def instruction_lines(body: str) -> list[tuple[int, str]]:
    entries = []
    for line in body.splitlines():
        match = re.match(r"\s+([0-9a-f]+):\s+(.*)$", line)
        if not match:
            continue
        inst = match.group(2).split("#", 1)[0].strip()
        inst = re.sub(r"\s+<[^>]*>", "", inst).strip()
        if not inst or inst.startswith("R_") or "R_X86_64" in inst:
            continue
        entries.append((int(match.group(1), 16), inst))
        if inst == "ret" or inst.startswith("ret "):
            break
    return entries


def mca_assembly(entries: list[tuple[int, str]]) -> str:
    labels = {addr: f".L{addr:x}" for addr, _inst in entries}
    lines = [".intel_syntax noprefix"]
    for addr, inst in entries:
        lines.append(f"{labels[addr]}:")
        branch = re.match(r"((?:j[a-z]+)|jmp)\s+(0x[0-9a-f]+)\b", inst)
        if branch:
            target = int(branch.group(2), 16)
            if target in labels:
                inst = f"{branch.group(1)} {labels[target]}"
        lines.append(inst)
    return "\n".join(lines) + "\n"


def leaf_faults(body: str) -> list[str]:
    faults = []
    for line in body.splitlines():
        if re.search(r"\b(?:call|callq)\b", line):
            faults.append("call " + line.strip())
        if re.search(r"\bjmp\b", line):
            faults.append("jmp " + line.strip())
        if re.search(r"\b(?:div|idiv)\b", line):
            faults.append("div " + line.strip())
        if re.search(r"\bsub\s+rsp\b", line) or re.search(r"\b(?:push|pop)\b", line):
            faults.append("frame " + line.strip())
    return faults


def audit_archive(archive: Path, out_dir: Path, compiler: str) -> dict:
    dump = subprocess.check_output(
        [str(LLVM / "llvm-objdump"), "-d", "-r", "--no-show-raw-insn", "-M", "intel", str(archive)],
        text=True,
    )
    sections = subprocess.check_output([str(LLVM / "llvm-readobj"), "--sections", str(archive)], text=True)
    symbols = subprocess.check_output(
        [str(LLVM / "llvm-nm"), "-S", "--defined-only", "--format=posix", str(archive)],
        text=True,
    )
    (out_dir / f"{compiler}-objdump.txt").write_text(dump)
    (out_dir / f"{compiler}-readobj.txt").write_text(sections)
    (out_dir / f"{compiler}-nm.txt").write_text(symbols)
    if (
        ".gcc_except_table" in sections
        or ".eh_frame" in sections
        or ".init_array" in sections
        or "__gxx_personality" in dump
        or "__cxa_atexit" in symbols
        or "_ZTI" in symbols
        or "__cxa_throw" in dump
        or "__gxx_personality" in symbols
    ):
        raise RuntimeError(f"{compiler} archive keeps forbidden EH/unwind/init_array section or symbol")
    blobs = split_symbols(dump)
    report = {}
    for name in SYMBOLS:
        body = blobs.get(name)
        if body is None:
            raise RuntimeError(f"{compiler} archive is missing {name}")
        faults = leaf_faults(body)
        if faults:
            raise RuntimeError(f"{compiler} {name} leaf fault: {faults[0]}")
        asm = mca_assembly(instruction_lines(body))
        asm_path = out_dir / f"{compiler}-{name}.s"
        asm_path.write_text(asm)
        mca = subprocess.run(
            [
                str(LLVM / "llvm-mca"),
                "--bottleneck-analysis",
                "-mtriple=x86_64-unknown-linux-gnu",
                "-mcpu=znver5",
                str(asm_path),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        (out_dir / f"{compiler}-{name}.mca").write_text(mca.stdout)
        if mca.returncode != 0:
            raise RuntimeError(f"llvm-mca failed for {compiler} {name}")
        match = re.search(r"Block RThroughput:\s+([0-9.]+)", mca.stdout)
        if match is None:
            raise RuntimeError(f"llvm-mca omitted throughput for {compiler} {name}")
        throughput = float(match.group(1))
        if throughput > 16.0:
            raise RuntimeError(f"{compiler} {name} RThroughput {throughput} exceeds 16")
        report[name] = {
            "sha256": hashlib.sha256(body.encode()).hexdigest(),
            "block_rthroughput": throughput,
            "instructions": len(instruction_lines(body)),
        }
    return {"sha256": sha256_file(archive), "symbols": report}


def roi_stats(text: str) -> dict[str, float]:
    epochs = re.findall(r"Begin Simulation Statistics.*?End Simulation Statistics", text, re.DOTALL)
    if len(epochs) != 2:
        raise ValueError(f"expected two gem5 epochs, found {len(epochs)}")
    raw = {}
    for line in epochs[0].splitlines():
        words = line.split()
        if len(words) < 2 or not words[0].startswith("system.cpu."):
            continue
        key = words[0][len("system.cpu.") :]
        raw[key] = words[1]
    result = {}
    for name, aliases in ALIASES.items():
        value = next((raw[alias] for alias in aliases if alias in raw), None)
        if value is None and name in ("idle", "rename_idle", "squash"):
            value = "0"
        if value is None:
            raise ValueError(f"missing O3 counter {name}")
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0:
            raise ValueError(f"invalid O3 counter {name}")
        result[name] = numeric
    return result


def linear_metrics(samples: list[dict[str, float]], width: int) -> dict[str, float]:
    first = {name: samples[1][name] - samples[0][name] for name in ALIASES}
    second = {name: samples[2][name] - samples[1][name] for name in ALIASES}
    if any(value < 0 for value in first.values()) or any(value < 0 for value in second.values()):
        raise ValueError("non-monotonic O3 counter")
    slopes = (first["cycles"] / 2048.0, second["cycles"] / 2048.0)
    drift = abs(slopes[1] - slopes[0]) / slopes[0]
    xs = [float(count) for count in COUNTS]
    ys = [sample["cycles"] for sample in samples]
    mean_x = sum(xs) / 3.0
    mean_y = sum(ys) / 3.0
    denom = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom
    intercept = mean_y - slope * mean_x
    residual = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    total = sum((y - mean_y) ** 2 for y in ys)
    r_squared = 1.0 if total == 0 else 1.0 - residual / total
    if drift >= 0.02 or r_squared < 0.9999:
        raise ValueError(f"linearity failed drift={drift} r2={r_squared}")
    slots = width * (second["cycles"] - second["idle"])
    if slots <= 0:
        raise ValueError("empty rename-slot interval")
    retiring = 100.0 * second["ops"] / slots
    bad = 100.0 * (max(0.0, second["renamed"] - second["ops"]) + width * second["squash"]) / slots
    frontend = 100.0 * width * second["rename_idle"] / slots
    attributed = retiring + bad + frontend
    if attributed > 105.0:
        raise ValueError(f"rename-slot attribution {attributed}")
    branch = second["branch_misses"] / 2048.0
    violations = [sample["memory_violations"] for sample in samples]
    if branch > 0.005 or any(item != 0 for item in violations) or first["memory_violations"] != 0:
        raise ValueError(f"branch or memory-order gate failed misses={branch} violations={violations}")
    return {
        "delta1_cycles_per_op": slopes[0],
        "delta2_cycles_per_op": slopes[1],
        "linearity_drift": drift,
        "r_squared": r_squared,
        "ipc": second["instructions"] / second["cycles"],
        "branch_misses_per_op": branch,
        "memory_order_violations": second["memory_violations"],
        "retiring_pct": retiring,
        "bad_speculation_pct": bad,
        "frontend_bound_pct": frontend,
        "backend_bound_pct": max(0.0, 100.0 - attributed),
        "attribution_sum_pct": attributed,
    }


def compile_guest(source: Path, archive: Path, output: Path, roi: bool) -> None:
    build_dir = next(parent for parent in archive.parents if (parent / "CMakeCache.txt").is_file())
    includes = build_dir / "vcpkg_installed/x64-linux-clang-glibc/include"
    if not includes.is_dir():
        raise RuntimeError(f"Missing manifest-installed headers: {includes}")
    command = [
        str(CLANGXX),
        "-stdlib=libc++",
        "-std=c++26",
        "-freflection-latest",
        "-fno-exceptions",
        "-fno-rtti",
        "-O3",
        "-g",
        "-static",
        "-fno-lto",
        "-DNDEBUG",
        f"-I{source.parents[1] / 'src'}",
        "-isystem",
        str(includes),
        str(source),
        str(archive),
        "-o",
        str(output),
    ]
    if roi:
        command.insert(8, "-DGREPTIME_GEM5_ROI")
    env = os.environ.copy()
    env["TMPDIR"] = str(output.parent / "tmp")
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False, env=env)
    (output.parent / f"{output.name}.link.log").write_text(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"guest link failed: {output}\n{result.stdout[-2000:]}")


def host_checksum(binary: Path, probe: str, count: int) -> int:
    result = subprocess.run(
        [str(binary), "--probe", probe, "--iterations", f"{count:010d}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"host oracle failed {probe} {count}: {result.stdout[-500:]}")
    match = re.search(rf"^probe={probe} iterations={count:010d} checksum=(\d+)$", result.stdout, re.MULTILINE)
    if match is None:
        raise RuntimeError(f"host oracle output missing checksum: {result.stdout}")
    return int(match.group(1))


def simulate(binary: Path, probe: str, count: int, run_dir: Path, width_holder: dict) -> tuple[int, dict[str, float]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    command = [
        str(GEM5),
        f"--outdir={run_dir}",
        str(O3_CONFIG),
        "--cpu",
        PROFILE,
        "--binary",
        str(binary),
        "--probe",
        probe,
        "--iterations",
        f"{count:010d}",
    ]
    env = os.environ.copy()
    env["GLIBC_TUNABLES"] = "glibc.pthread.rseq=0"
    env["TMPDIR"] = str(run_dir.parent / "tmp")
    result = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
        check=False,
        env=env,
    )
    (run_dir / "console.log").write_text(result.stdout)
    (run_dir / "command.json").write_text(json.dumps(command))
    if result.returncode != 0:
        raise RuntimeError(f"gem5 failed {probe} {count}: {run_dir}")
    match = re.search(rf"^probe={probe} iterations={count:010d} checksum=(\d+)$", result.stdout, re.MULTILINE)
    if match is None:
        raise RuntimeError(f"guest checksum missing: {run_dir}")
    stats = (run_dir / "stats.txt").read_text()
    width_holder["seen"] = True
    return int(match.group(1)), roi_stats(stats)


def parse_percent_table(text: str) -> dict[str, float]:
    rows = {
        "IPC (Sys + User)": "ipc",
        "Retiring": "retiring_pct",
        "Bad_Speculation": "bad_speculation_pct",
        "Frontend_Bound": "frontend_bound_pct",
        "Backend_Bound": "backend_bound_pct",
    }
    found = {}
    for line in text.splitlines():
        name, separator, rest = line.partition(",")
        if separator and name in rows:
            found[rows[name]] = float(rest.split(",")[0])
    return found


def collect_pmu(oracle: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    lock = Path("/tmp/amduprof.lock")
    report = {}
    for probe in PROBES:
        iterations = "000600000000"
        pcm_dir = output / f"pcm-{probe}"
        cli_dir = output / f"cli-{probe}"
        if pcm_dir.exists():
            shutil.rmtree(pcm_dir)
        if cli_dir.exists():
            shutil.rmtree(cli_dir)
        pcm_cmd = (
            f"taskset -c {CORE} flock -w 900 {lock} /usr/local/bin/AMDuProfPcm profile "
            f"-m ipc,pipeline_util,l1,l2,tlb,dc,cache_miss -c core={CORE} -O {pcm_dir} -- "
            f"{oracle} --probe {probe} --iterations {iterations}"
        )
        wrapper = output / f"run-{probe}.sh"
        wrapper.write_text(f"#!/bin/sh\nexec {oracle} --probe {probe} --iterations {iterations}\n")
        wrapper.chmod(0o755)
        cli_cmd = (
            f"taskset -c {CORE} flock -w 900 {lock} /usr/local/bin/AMDuProfCLI collect --config assess_ext "
            f"-o {cli_dir} {wrapper}"
        )
        run(["bash", "-lc", pcm_cmd], cwd=output, env=os.environ.copy(), timeout=900, log=output / f"{probe}-pcm.log")
        run(["bash", "-lc", cli_cmd], cwd=output, env=os.environ.copy(), timeout=900, log=output / f"{probe}-cli.log")
        sessions = sorted(path for path in cli_dir.glob("*") if path.is_dir())
        if not sessions:
            raise RuntimeError(f"AMDuProfCLI produced no session for {probe}")
        reported = subprocess.run(
            ["flock", "-w", "900", str(lock), "/usr/local/bin/AMDuProfCLI", "report", "-i", str(sessions[-1])],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=180,
        )
        (output / f"{probe}-cli-report.txt").write_text(reported.stdout)
        pcm_text = "\n".join(
            path.read_text(errors="replace")
            for path in pcm_dir.rglob("*")
            if path.suffix in {".csv", ".txt", ".html"} and path.stat().st_size < 2_000_000
        )
        (output / f"{probe}-pcm-extract.txt").write_text(pcm_text[:200000])
        metrics = parse_percent_table(pcm_text)
        required = ("ipc", "retiring_pct", "bad_speculation_pct", "frontend_bound_pct", "backend_bound_pct")
        missing = [name for name in required if name not in metrics]
        if missing:
            raise RuntimeError(f"AMDuProf metrics missing for {probe}: {missing}")
        report[probe] = metrics
    return report


def write_markdown(path: Path, payload: dict) -> None:
    lines = [
        "# greptimedb probes Tier 2 TMA",
        "",
        f"- Worktree: `{payload['worktree']}`",
        f"- Commit: `{payload['commit']}` (`dirty_count={payload['dirty_count']}`)",
        f"- Clang archive SHA256: `{payload['tier1']['clang']['sha256']}`",
        f"- GCC archive SHA256: `{payload['tier1']['gcc']['sha256']}`",
        f"- ROI guest SHA256: `{payload['guest_sha256']}`",
        f"- Oracle guest SHA256: `{payload['oracle_sha256']}`",
        "",
        "## Tier 1 Block RThroughput",
        "",
        "| Compiler | Symbol | Instructions | RThroughput | Disassembly SHA256 |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for compiler in ("clang", "gcc"):
        for name, item in payload["tier1"][compiler]["symbols"].items():
            lines.append(
                f"| {compiler} | `{name}` | {item['instructions']} | {item['block_rthroughput']} | `{item['sha256']}` |"
            )
    lines.extend(["", "## gem5 O3CPU", "", "| Probe | N | Checksum | Cycles | Instructions | uOps | Branch misses | Memory violations |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for probe, item in payload["gem5"].items():
        for sample in item["samples"]:
            lines.append(
                f"| `{probe}` | {sample['iterations']} | {sample['checksum']} | {int(sample['stats']['cycles'])} | "
                f"{int(sample['stats']['instructions'])} | {int(sample['stats']['ops'])} | "
                f"{int(sample['stats']['branch_misses'])} | {int(sample['stats']['memory_violations'])} |"
            )
    lines.extend(["", "| Probe | Drift | R² | IPC | Retiring % | Bad speculation % | Frontend % | Backend % |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for probe, item in payload["gem5"].items():
        metrics = item["metrics"]
        lines.append(
            f"| `{probe}` | {metrics['linearity_drift']:.6f} | {metrics['r_squared']:.6f} | {metrics['ipc']:.3f} | "
            f"{metrics['retiring_pct']:.2f} | {metrics['bad_speculation_pct']:.2f} | "
            f"{metrics['frontend_bound_pct']:.2f} | {metrics['backend_bound_pct']:.2f} |"
        )
    lines.extend(["", "## AMDuProf", ""])
    for probe, metrics in payload["pmu"].items():
        lines.append(
            f"- `{probe}`: IPC {metrics['ipc']}, Retiring {metrics['retiring_pct']}%, "
            f"Bad Speculation {metrics['bad_speculation_pct']}%, Frontend Bound {metrics['frontend_bound_pct']}%, "
            f"Backend Bound {metrics['backend_bound_pct']}%"
        )
    lines.append("")
    path.write_text("\n".join(lines))


def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clang-archive", type=Path, required=True)
    parser.add_argument("--gcc-archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--skip-pmu", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    if Path("/tmp") in output.parents or output == Path("/tmp"):
        parser.error("audit output must stay in the worktree build tree")
    output.mkdir(parents=True, exist_ok=True)
    (output / "tmp").mkdir(exist_ok=True)
    started = time.time()
    tier1_dir = output / "tier1"
    tier1_dir.mkdir(exist_ok=True)
    tier1 = {
        "clang": audit_archive(args.clang_archive.resolve(), tier1_dir, "clang"),
        "gcc": audit_archive(args.gcc_archive.resolve(), tier1_dir, "gcc"),
    }
    source = repo / "benches" / "bench_hot_probes.cc"
    oracle = output / "probe_oracle"
    guest = output / "probe_guest"
    compile_guest(source, args.clang_archive.resolve(), oracle, roi=False)
    compile_guest(source, args.clang_archive.resolve(), guest, roi=True)
    profile = json.loads(subprocess.check_output(["python3", str(O3_CONFIG), "--dump-profile-json", PROFILE], text=True))
    width = int(profile["slot_width"])
    gem5 = {}
    for probe in PROBES:
        samples = []
        for count in COUNTS:
            expected = host_checksum(oracle, probe, count)
            guest_sum, stats = simulate(guest, probe, count, output / "gem5" / f"{probe}-{count:010d}", {})
            if guest_sum != expected:
                raise RuntimeError(f"checksum mismatch {probe} {count}: host {expected} guest {guest_sum}")
            samples.append({"iterations": count, "checksum": guest_sum, "stats": stats})
        gem5[probe] = {"samples": samples, "metrics": linear_metrics([item["stats"] for item in samples], width)}
        print(f"gem5 {probe} drift={gem5[probe]['metrics']['linearity_drift']:.6f}", flush=True)
    pmu = {} if args.skip_pmu else collect_pmu(oracle, output / "uprof")
    commit_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    dirty_lines = [
        line
        for line in subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).splitlines()
        if line.strip()
    ]
    payload = {
        "worktree": str(repo),
        "commit": commit_sha,
        "execution_commit": commit_sha,
        "dirty_count": len(dirty_lines),
        "profile": profile,
        "tier1": tier1,
        "guest_sha256": sha256_file(guest),
        "oracle_sha256": sha256_file(oracle),
        "gem5": gem5,
        "pmu": pmu,
        "elapsed_sec": round(time.time() - started, 3),
    }
    result_path = output / "result.json"
    result_path.write_text(json.dumps(payload, indent=2) + "\n")
    shutil.copyfile(result_path, Path("/tmp/greptimedb_tier2_result.json"))
    write_markdown(Path("/tmp/greptimedb_tier2_tma.md"), payload)
    print("PASS tier1 gem5" + ("" if args.skip_pmu else " pmu"), flush=True)


if __name__ == "__main__":
    main()
