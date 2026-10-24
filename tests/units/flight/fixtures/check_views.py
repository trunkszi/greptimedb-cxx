"""Verify C++ View frames using an independent PyArrow reader."""
import argparse
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.ipc as ipc


def frame(header, body=b""):
    metadata = header + bytes((-len(header)) % 8)
    return struct.pack("<II", 0xFFFFFFFF, len(metadata)) + metadata + body


def read_batch(directory, codec):
    schema = (directory / "views.schema.bin").read_bytes()
    header = (directory / ("views-" + codec + ".header.bin")).read_bytes()
    body = (directory / ("views-" + codec + ".body.bin")).read_bytes()
    stream = frame(schema) + frame(header, body) + struct.pack("<II", 0xFFFFFFFF, 0)
    return ipc.open_stream(pa.py_buffer(stream)).read_next_batch()


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path, nargs="?",
                    default=Path("build/gcc-glibc/tests/units/flight/interop"))
args = parser.parse_args()
for codec in ("none", "lz4", "zstd"):
    expected = read_batch(Path(__file__).parent, codec)
    actual = read_batch(args.output, codec)
    assert actual.equals(expected, check_metadata=True), codec + " View batch mismatch"
    print(codec + ": C++ Utf8View/BinaryView -> PyArrow values and metadata match")
