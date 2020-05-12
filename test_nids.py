#!/usr/bin/env python
# Copyright (c) 2015-2016 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

from datetime import datetime
import struct

import boto3

meta_struct = struct.Struct('6IQiII')
len_struct = struct.Struct('I')


def write_str(fobj, s):
    fobj.write(len_struct.pack(len(s)))
    fobj.write(s.encode('ascii'))


def write_product(out):
    bucket = boto3.session.Session().resource('s3').Bucket('unidata-nexrad-level3')
    obj = bucket.Object('RAX_NST_2020_04_12_22_01_33')
    block = obj.get()['Body'].read()
    prod_id = 'SDUS55 KBOU 280701 /pNCRFTG'
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

    proc = subprocess.Popen(['./upload_nids.py', '-vv', '-b', 'foo'],
                            stdout=sys.stdout, stdin=subprocess.PIPE, universal_newlines=False,
                            bufsize=40)

    write_product(proc.stdin)
    for chunk in range(2, 3):
        write_product(proc.stdin)
        time.sleep(0.25)

    proc.stdin.flush()
    if timeout:
        time.sleep(timeout + 15)

    proc.stdin.close()
    proc.wait()
