"""Generate independent Arrow View IPC frames with PyArrow 21.0.0."""
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

root = Path(__file__).parent
fields = [
    pa.field("text", pa.string_view()),
    pa.field("binary", pa.binary_view()),
    pa.field("json", pa.string_view(), metadata={b"greptime:type": b"Json"}),
]
schema = pa.schema(fields)
batch = pa.RecordBatch.from_arrays([
    pa.array(["", None, "123456789012", "external view payload 0123456789", "λ🙂"], type=pa.string_view()),
    pa.array([b"", None, b"123456789012", b"long \x00\xff binary payload", b"\x00\xff"], type=pa.binary_view()),
    pa.array(["{}", None, "null", '{"long_document":"external payload"}', "[]"], type=pa.string_view()),
], schema=schema)
for codec in (None, "lz4", "zstd"):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema, options=ipc.IpcWriteOptions(compression=codec)) as writer:
        writer.write_batch(batch)
    messages = list(ipc.MessageReader.open_stream(sink.getvalue()))
    if codec is None:
        (root / "views.schema.bin").write_bytes(messages[0].metadata.to_pybytes())
    name = "views-" + (codec or "none")
    (root / (name + ".header.bin")).write_bytes(messages[1].metadata.to_pybytes())
    (root / (name + ".body.bin")).write_bytes(messages[1].body.to_pybytes())
print("Generated independent Arrow View frames with PyArrow", pa.__version__)
