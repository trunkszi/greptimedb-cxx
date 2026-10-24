from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.ipc as ipc

root = Path('tests/units/flight/fixtures')
fields, arrays = [], []
def add(name, datatype, values, metadata=None, nullable=True):
    fields.append(pa.field(name, datatype, nullable, metadata=metadata or {b'greptime:semantic_type': b'field'}))
    arrays.append(pa.array(values, type=datatype))
add('time', pa.timestamp('s'), [-3601, -1, 0, 3599, 3600], {b'greptime:time_index': b'true'}, False)
add('bool', pa.bool_(), [True, None, False, True, None])
for bits in (8,16,32,64):
    add('i'+str(bits), getattr(pa,'int'+str(bits))(), [-(2**(bits-1)), None, 0, 2**(bits-1)-1, None])
    add('u'+str(bits), getattr(pa,'uint'+str(bits))(), [0, None, 2**bits-1, 1, None])
for bits in (32,64):
    add('f'+str(bits), getattr(pa,'float'+str(bits))(), [-1.25,None,0.0,2.5,None])
add('bytes',pa.binary(),[b'\x00\xff',None,b'',b'abc',None])
add('text',pa.string(),['alpha',None,'','\u03bb\U0001f600',None],{b'greptime:semantic_type':b'tag'})
add('date',pa.date32(),[-100,None,0,100,None])
add('datetime',pa.date64(),[-86400000,None,0,86400000,None])
for unit in ('ms','us','ns'):
    add('ts_'+unit,pa.timestamp(unit),[-123,None,0,123,None])
add('decimal',pa.decimal128(20,4),[Decimal('-1234567890123456.7890'),None,Decimal('0.0000'),Decimal('1234567890123456.7890'),None])
add('json',pa.binary(),[b'{"x":1}',None,b'null',b'\x00\xff',None],{b'greptime:semantic_type':b'field',b'greptime:type':b'Json'})
add('large_text',pa.large_string(),['large',None,'','\u4e2d',None])
add('large_bytes',pa.large_binary(),[b'large',None,b'',b'\x00\xff',None])
add('nulls',pa.null(),[None]*5)
schema=pa.schema(fields,metadata={b'greptime:table':b'ipc_reference',b'custom':b'preserved'})
batch=pa.RecordBatch.from_arrays(arrays,schema=schema)
for compression in (None,'lz4','zstd'):
    sink=pa.BufferOutputStream()
    with ipc.new_stream(sink,schema,options=ipc.IpcWriteOptions(compression=compression)) as writer:
        writer.write_batch(batch)
    messages=list(ipc.MessageReader.open_stream(sink.getvalue()))
    assert len(messages)==2
    if compression is None:
        (root/'schema.bin').write_bytes(messages[0].metadata.to_pybytes())
    label=compression or 'none'
    (root/(label+'.header.bin')).write_bytes(messages[1].metadata.to_pybytes())
    (root/(label+'.body.bin')).write_bytes(messages[1].body.to_pybytes())
print('Created PyArrow',pa.__version__,'fixtures:',len(fields),'fields,',len(batch),'rows')
dictionary_batch = pa.record_batch([pa.array(['red','blue','red']).dictionary_encode()], names=['dictionary'])
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, dictionary_batch.schema) as writer:
    writer.write_batch(dictionary_batch)
messages = list(ipc.MessageReader.open_stream(sink.getvalue()))
assert len(messages) == 3 and messages[1].type == 'dictionary'
(root/'dictionary.schema.bin').write_bytes(messages[0].metadata.to_pybytes())
(root/'dictionary.header.bin').write_bytes(messages[1].metadata.to_pybytes())
