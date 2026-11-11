"""Validate C++ encoder frames with the independent PyArrow fixture oracle."""
import argparse
from pathlib import Path
import struct
import pyarrow as pa
import pyarrow.ipc as ipc


def frame(header, body=b''):
    metadata = header + bytes((-len(header)) % 8)
    return struct.pack('<II', 0xFFFFFFFF, len(metadata)) + metadata + body


def read_batch(directory, codec):
    schema = (directory / 'schema.bin').read_bytes()
    header = (directory / (codec + '.header.bin')).read_bytes()
    body = (directory / (codec + '.body.bin')).read_bytes()
    stream = frame(schema) + frame(header, body) + struct.pack('<II', 0xFFFFFFFF, 0)
    reader = ipc.open_stream(pa.py_buffer(stream))
    return reader.read_next_batch()


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('output', type=Path, nargs='?',
                    default=Path('build/gcc-glibc/tests/units/flight/interop'))
args = parser.parse_args()
reference = Path(__file__).resolve().parent
for codec in ('none', 'lz4', 'zstd'):
    expected = read_batch(reference, codec)
    actual = read_batch(args.output, codec)
    assert actual.equals(expected, check_metadata=True), codec + ' Arrow batch mismatch'
    print(codec + ': C++ encoder -> PyArrow matches all 24 fields, values, and metadata')
