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


def write_chunk(out, radar, vol, chunk, typ):
    block = make_block(b'p' * 50)
    if typ == 'S':
        block = b'1' * 24 + block
    prod_id = 'L2-BZIP2/%s/20150908215946/%d/%d/%s/V06/0' % (radar, vol, chunk, typ)
    origin = 'foobar'
    out.write(meta_struct.pack(meta_struct.size + 2 * len_struct.size + len(prod_id) + len(origin),
                               1, 2, 3, 4, len(block), 1, 1, 1, 2))
    write_str(out, prod_id)
    write_str(out, origin)
    out.write(block)


if __name__ == '__main__':
    import subprocess
    import sys
    radar = 'KFTG'
    vol = 494
    proc = subprocess.Popen(['./l2assemble.py', '-vv', '-d', '.', '-t', '2', radar, str(vol)],
                            stdout=sys.stdout, stdin=subprocess.PIPE, universal_newlines=False)

    write_chunk(proc.stdin, radar, vol, 1, 'S')
    for chunk in range(2, 10):
        write_chunk(proc.stdin, radar, vol, chunk, 'I')
    else:
        write_chunk(proc.stdin, radar, vol, chunk + 1, 'E')
