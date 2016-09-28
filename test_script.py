#!/usr/bin/env python
# Copyright (c) 2015-2016 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

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
    out.flush()


if __name__ == '__main__':
    import subprocess
    import sys
    import time

    timeout = False
    sig_hup = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'timeout':
            timeout = True
        elif sys.argv[1] == 'hup':
            sig_hup = True

    radar = 'KFTG'
    vol = 494
    time_val = 10
    proc = subprocess.Popen(['./l2assemble.py', '-vv', '-d', '.',
                             '-t', str(time_val)],
                            stdout=sys.stdout, stdin=subprocess.PIPE, universal_newlines=False,
                            bufsize=40)

    write_chunk(proc.stdin, radar, vol, 1, 'S')
    for chunk in range(2, 3):
        write_chunk(proc.stdin, radar, vol, chunk, 'I')
        time.sleep(0.25)
    else:
        if not (timeout or sig_hup):
            write_chunk(proc.stdin, radar, vol, chunk + 1, 'E')

    proc.stdin.flush()
    if timeout:
        time.sleep(timeout + 15)

    proc.stdin.close()
    proc.wait()
