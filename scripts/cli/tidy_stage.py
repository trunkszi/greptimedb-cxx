"""Stage sources so stock clang-tidy can parse around P2996 annotations."""

import json
import os
import re
from pathlib import Path

from . import utils


def strip_reflection_annotations(text: str) -> str:
    """Keep source positions stable while removing only P2996 annotation attributes."""
    result, index, size = list(text), 0, len(text)
    while index < size:
        if token := re.match(r'//[^\n]*|/\*[\s\S]*?\*/|(?:u8|u|U|L)?R"([^ ()\\\t\r\n]*)\(', text[index:]):
            if token.group(1) is not None:
                end = text.find(")" + token.group(1) + '"', index + token.end())
                index = size if end < 0 else end + len(token.group(1)) + 2
            else:
                index += token.end()
            continue
        prefix = re.match(r"(?:u8|u|U|L)(?=['\"])", text[index:])
        if prefix:
            index += prefix.end()
        is_sep = not prefix and text[index] == "'" and 0 < index < size - 1
        if is_sep and text[index - 1].isalnum() and text[index + 1].isalnum():
            index += 1
            continue
        if text[index] in "\"'":
            quote, index = text[index], index + 1
            while index < size:
                if text[index] == quote:
                    index += 1
                    break
                index += 2 if text[index] == "\\" else 1
            continue
        if not (start := re.match(r"\[\[\s*=", text[index:])):
            index += 1
            continue
        begin, index, depth = index, index + start.end(), 0
        while index < size:
            if token := re.match(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|//[^\n]*|/\*[\s\S]*?\*/', text[index:]):
                index += token.end()
                continue
            if depth == 0 and text.startswith("]]", index):
                index += 2
                for pos in range(begin, index):
                    if result[pos] not in "\r\n":
                        result[pos] = " "
                break
            depth += (text[index] == "[") - (text[index] == "]")
            index += 1
        else:
            raise ValueError("Unterminated reflection annotation")
    return "".join(result)


def _include_roots(entries: list[dict]) -> set[Path]:
    roots: set[Path] = set()
    for entry in entries:
        arguments = iter(entry["arguments"])
        for arg in arguments:
            for flag in ("-isystem", "-iquote", "-I"):
                if arg == flag or arg.startswith(flag):
                    value = next(arguments, "") if arg == flag else arg[len(flag) :]
                    candidate = (Path(entry["directory"]) / value).resolve()
                    if value and candidate.is_dir():
                        roots.add(candidate)
                    break
    return roots


def _mirror_files(root: Path, destination: Path):
    for current, dirs, names in os.walk(root, followlinks=True):
        cpath = Path(current)
        if cpath.resolve().is_relative_to(destination.resolve()):
            dirs.clear()
            continue
        anc = {p.resolve() for p in cpath.parents} | {cpath.resolve()}
        dirs[:] = [
            n
            for n in dirs
            if not n.startswith(".") and n not in {"build", "__pycache__"} and (cpath / n).resolve() not in anc
        ]
        for name in names:
            yield cpath / name


def stage_sources(entries: list[dict], destination: Path) -> list[dict]:
    """Mirror annotation-bearing include trees as well as selected project sources."""
    root, mirror = utils.PROJECT_ROOT.resolve(), destination / "source"
    roots = {root / name for name in ("src", "tests", "examples", "benches") if (root / name).is_dir()}
    suffixes = {".h", ".hpp", ".hh", ".hxx", ".cc", ".cpp", ".inc", ".ipp", ".tpp"}
    for d in _include_roots(entries) - roots:
        if any(f.suffix in suffixes and re.search(rb"\[\[\s*=", f.read_bytes()) for f in _mirror_files(d, destination)):
            roots.add(d)
    rewrites: dict[Path, Path] = {}
    for number, original in enumerate(sorted(roots)):
        in_repo = original.is_relative_to(root)
        target = mirror / original.relative_to(root) if in_repo else destination / "includes" / str(number)
        rewrites[original] = target
        for file in _mirror_files(original, destination):
            output = target / file.relative_to(original)
            output.parent.mkdir(parents=True, exist_ok=True)
            data = file.read_bytes()
            if file.suffix in suffixes:
                text = data.decode()
                if "uarch" in file.parts:
                    text = utils.tidy_uarch_compat(file.as_posix().rsplit("/uarch/", 1)[1], text)
                text = utils.tidy_bit_members(text)
                if re.search(r"\[\[\s*=", text):
                    text = strip_reflection_annotations(text)
                data = text.encode()
            output.write_bytes(data)
    (destination / "paths.json").write_text(json.dumps({str(k): str(v) for k, v in rewrites.items()}))

    def remap(value: str, directory: str) -> str:
        absolute = (Path(directory) / value).resolve()
        for original in sorted(rewrites, key=lambda p: len(p.parts), reverse=True):
            if absolute.is_relative_to(original):
                return str(rewrites[original] / absolute.relative_to(original))
        return value

    translated = []
    for entry in entries:
        arguments = iter(entry["arguments"])
        result: list[str] = []
        for arg in arguments:
            for flag in ("-isystem", "-iquote", "-I"):
                if arg == flag:
                    result.extend([flag, remap(next(arguments), entry["directory"])])
                elif arg.startswith(flag):
                    result.append(flag + remap(arg[len(flag) :], entry["directory"]))
                else:
                    continue
                break
            else:
                result.append(remap(arg, entry["directory"]) if arg == entry["file"] else arg)
        translated.append({**entry, "file": remap(entry["file"], entry["directory"]), "arguments": result})
    return translated
