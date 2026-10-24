# Arrow interoperability fixtures

These bare IPC schema, record-batch headers, and bodies were generated with
Apache PyArrow 21.0.0 using `generate.py`. They cover 24 fields: the 19 primitive
GreptimeDB types, Decimal128, JSON-tagged Binary, LargeUtf8, LargeBinary, and Null.
The timestamp time index uses the live-server `greptime:time_index` marker.
Null masks are deliberately nontrivial, and the batch contains negative hours,
integer limits, Unicode, empty values, decimal signs, opaque binary JSON payloads,
and table metadata. The Date64 field omits its default unit in the FlatBuffer,
exercising Arrow's millisecond default independently of our writer.
An independent dictionary schema/header pair checks dictionary-batch detection
and explicit rejection of dictionary-encoded fields.

The C++ tests read these independent uncompressed, LZ4 Frame, and Zstandard
frames without linking Arrow. They also write their own frames into the build
directory for optional validation with PyArrow. PyArrow is a fixture-generation
and interoperability oracle only; it is not a project build dependency.

Regenerate from the repository root with an environment containing
`pyarrow==21.0.0`:

```sh
python tests/units/flight/fixtures/generate.py
```

After `./build.sh test`, check the exported C++ frames using that same optional
Python environment:

```sh
python tests/units/flight/fixtures/check.py
```

`generate_views.py` generates separate `views-*` fixtures with PyArrow 21.0.0
for Utf8View, BinaryView, JSON metadata, inline and external payloads, nulls,
and all three compression choices. The C++ view tests also export their encoded
frames under the build directory's `interop/` directory.
Run `python tests/units/flight/fixtures/generate_views.py` to regenerate them,
and `python tests/units/flight/fixtures/check_views.py` after the C++ tests to
validate the exported View frames with the independent PyArrow reader.

`live-matrix-schema.bin` is a schema captured from a GreptimeDB Docker query
against a 42-column type matrix. The `required_document` and `nullable_document`
JSON string projections use Utf8View (Arrow type 24) without JSON metadata.
It exercises live server compatibility independently of either SDK writer.
