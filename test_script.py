#!/usr/bin/env python
# Copyright (c) 2015-2016 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import bz2
from datetime import datetime
import functools
from itertools import cycle, islice
import random
import struct


meta_struct = struct.Struct('6IQiII')
len_struct = struct.Struct('I')


def write_str(fobj, s):
    fobj.write(len_struct.pack(len(s)))
    fobj.write(s.encode('ascii'))


def make_block(data):
    comp = bz2.compress(data)
    return len_struct.pack(len(comp)) + comp


@functools.lru_cache(1024)
def get_vol_time(radar, vol):
    return datetime.utcnow()


def write_chunk(out, radar, vol, chunk, typ):
    block = make_block(random.randbytes(100 * 1024))
    if typ == 'S':
        block = b'1' * 24 + block
    dt = get_vol_time(radar, vol)
    prod_id = f'L2-BZIP2/{radar}/{dt:%Y%m%d%H%M%S}/{vol}/{chunk}/{typ}/V06/0'
    origin = 'foobar'
    out.write(meta_struct.pack(meta_struct.size + 2 * len_struct.size + len(prod_id) + len(origin),
                               1, 2, 3, 4, len(block), 1, 1, 1, 2))
    write_str(out, prod_id)
    write_str(out, origin)
    out.write(block)
    out.flush()


# From official itertools 3.9 docs
def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    # Recipe credited to George Sakkis
    num_active = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while num_active:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            # Remove the iterator we just exhausted from the cycle.
            num_active -= 1
            nexts = cycle(islice(nexts, num_active))


def vols(radar, n):
    start = random.randint(0, 999 - n)
    for vol in range(start, start + n):
        num_chunks = random.randint(30, 100)
        for chunk in range(1, num_chunks + 1):
            code = 'S' if chunk == 1 else 'E' if chunk == num_chunks else 'I'
            yield radar, vol, chunk, code


if __name__ == '__main__':
    import argparse
    import string
    import subprocess
    import sys
    import time

    parser = argparse.ArgumentParser(description='Test l2assemble.')
    parser.add_argument('stop', type=str, nargs='?', help='Optional stop method.')
    parser.add_argument('--radars', '-r', type=int, default=10,
                        help='Number of radars to simulate')
    parser.add_argument('--volumes', '-v', type=int, default=5,
                        help='Number of volumes to generate per radar')
    args = parser.parse_args()

    timeout = args.stop == 'timeout'
    sig_hup = args.stop == 'hup'
    spacing = 6
    num_radars = args.radars
    radars = ('K' + ''.join(random.choice(string.ascii_uppercase) for _ in range(3))
              for _ in range(num_radars))
    time_val = 5 * spacing
    proc = subprocess.Popen(['./l2assemble.py', '-vv', '-d', 'data',
                             '-t', str(time_val)],
                            stdout=sys.stdout, stdin=subprocess.PIPE, universal_newlines=False,
                            bufsize=40)

    for radar, vol, chunk, code in roundrobin(*(vols(r, args.volumes) for r in radars)):
        if radar is None:
            continue
        if not (timeout or sig_hup and code == 'E'):
            write_chunk(proc.stdin, radar, vol, chunk, code)
            time.sleep(random.gauss(spacing / num_radars, 0.2 * spacing / num_radars))
        else:
            break

    proc.stdin.flush()
    if timeout:
        time.sleep(time_val + 15)

    proc.stdin.close()
    proc.wait()
