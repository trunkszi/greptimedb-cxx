#!/usr/bin/env bash
# Verify source limits, dependency boundaries, and single-threaded primitives.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$script_dir/../.." && pwd)

python3 - "$root" <<'PY'
import json
import os
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
errors = []
source_dirs = ("src", "tests", "examples", "benches", "cmake", "scripts")
source_suffixes = {".h", ".hh", ".hpp", ".hxx", ".c", ".cc", ".cpp", ".cxx", ".ipp", ".tpp"}
ignored_dirs = {".git", ".venv", "__pycache__"}
banned_packages = {"arrow", "grpc", "folly", "boost-asio"}


def report(path, message, line=None):
    location = str(path.relative_to(root))
    if line is not None:
        location += f":{line}"
    errors.append(f"error: {location}: {message}")


def cpp_tokens(text):
    """Yield code tokens with original line numbers, skipping comments/literals."""
    # Translation-phase line splicing precedes comment and token recognition.
    offsets = []
    chars = []
    index = 0
    line = 1
    while index < len(text):
        if text.startswith("\\\n", index):
            index += 2
            line += 1
            continue
        if text.startswith("\\\r\n", index):
            index += 3
            line += 1
            continue
        chars.append(text[index])
        offsets.append(line)
        line += text[index] == "\n"
        index += 1
    text = "".join(chars)
    size = len(text)
    index = 0
    while index < size:
        if text[index].isspace():
            index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index + 2)
            index = size if end < 0 else end
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise ValueError(f"unterminated C++ block comment at line {offsets[index]}")
            index = end + 2
            continue
        raw = re.match(r'(?:u8|u|U|L)?R"([^ ()\\\t\r\n]{0,16})\(', text[index:])
        if raw:
            end_marker = ")" + raw.group(1) + '"'
            end = text.find(end_marker, index + raw.end())
            if end < 0:
                raise ValueError(f"unterminated C++ raw string at line {offsets[index]}")
            index = end + len(end_marker)
            continue
        literal = re.match(r"(?:u8|u|U|L)?([\"'])", text[index:])
        if literal:
            quote = literal.group(1)
            index += literal.end()
            while index < size:
                if text[index] == "\\":
                    index += 2
                elif text[index] == quote:
                    index += 1
                    break
                else:
                    index += 1
            continue
        # Apostrophes within preprocessing numbers are digit separators.
        number = re.match(r"(?:[0-9]|\.[0-9])[A-Za-z0-9_.']*", text[index:])
        if number:
            index += number.end()
            continue
        identifier = re.match(r"[A-Za-z_][A-Za-z_0-9]*", text[index:])
        if identifier:
            token = identifier.group()
        elif text.startswith("::", index):
            token = "::"
        else:
            token = text[index]
        yield token, offsets[index]
        index += len(token)


def check_primitives(path, text):
    tokens = list(cpp_tokens(text))
    for index, (token, line) in enumerate(tokens):
        next_token = tokens[index + 1][0] if index + 1 < len(tokens) else None
        if token in {"co_await", "co_yield", "co_return", "throw"}:
            report(path, f"banned production primitive {token}", line)
        elif token == "try" and next_token == "{":
            report(path, "banned production primitive try {", line)
        elif token == "catch" and next_token == "(":
            report(path, "banned production primitive catch (", line)
        elif token == "std" and index + 2 < len(tokens):
            scope, name = tokens[index + 1][0], tokens[index + 2][0]
            identity_type = name == "thread" and [item[0] for item in tokens[index + 3:index + 5]] == ["::", "id"]
            if scope == "::" and name in {"function", "thread", "mutex", "shared_mutex"} and not identity_type:
                report(path, f"banned production primitive std::{name}", line)


for directory in source_dirs:
    base = root / directory
    if not base.exists():
        continue
    for current, dirs, names in os.walk(base, followlinks=False):
        dirs[:] = sorted(name for name in dirs if name not in ignored_dirs)
        for name in sorted(names):
            path = pathlib.Path(current) / name
            limited = path.suffix in {".h", ".cc", ".py"} or name == "CMakeLists.txt"
            production = directory == "src" and path.suffix in source_suffixes
            if not (limited or production):
                continue
            try:
                content = path.read_bytes()
                lines = content.count(b"\n")
                if limited and lines > 800:
                    report(path, f"{lines} lines exceeds the 800-line limit")
                if production:
                    check_primitives(path, content.decode("utf-8"))
            except (OSError, UnicodeError, ValueError) as error:
                report(path, f"cannot inspect source: {error}")


def check_dependencies(path, entries, section):
    if not isinstance(entries, list):
        report(path, f"{section} must be an array")
        return
    for entry in entries:
        name = entry.get("name") if isinstance(entry, dict) else entry
        if not isinstance(name, str):
            report(path, f"invalid dependency entry in {section}")
        elif name in banned_packages:
            report(path, f"banned dependency {name} in {section}")


manifest = root / "vcpkg.json"
try:
    data = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("manifest must be an object")
    check_dependencies(manifest, data.get("dependencies", []), "dependencies")
    features = data.get("features", {})
    if not isinstance(features, dict):
        raise ValueError("features must be an object")
    for name, feature in features.items():
        if not isinstance(feature, dict):
            raise ValueError(f"feature {name} must be an object")
        check_dependencies(manifest, feature.get("dependencies", []), f"features.{name}")
except (OSError, UnicodeError, ValueError) as error:
    report(manifest, f"cannot inspect dependency manifest: {error}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print("source contracts ok")
PY
