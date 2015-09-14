import bz2
import struct

meta_struct = struct.Struct('6IQiII')
len_struct = struct.Struct('I')
def write_str(fobj, s):
    fobj.write(len_struct.pack(len(s)))
    fobj.write(s.encode('ascii'))

def make_block(data):
    comp = bz2.compress(data)
    return len_struct.pack(len(comp)) + comp

with open('test.stream', 'wb') as out:
    block = make_block(b'o' * 100)
    out.write(meta_struct.pack(144, 1, 2, 3, 4, 24 + len(block), 1, 1, 1, 1))
    write_str(out, 'L2-BZIP2/KFTG/20150908215946/494/1/S/V06/0')
    write_str(out, 'foobar')
    out.write(b'1' * 24)
    out.write(block)

    block = make_block(b'p' * 50)
    out.write(meta_struct.pack(144, 1, 2, 3, 4, len(block), 1, 1, 1, 2))
    write_str(out, 'L2-BZIP2/KFTG/20150908215946/494/2/E/V06/0')
    write_str(out, 'foobar')
    out.write(block)
