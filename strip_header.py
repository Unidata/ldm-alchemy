#!/usr/bin/env python
# Copyright (c) 2015 Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import re
import sys


# Find WMO header and remove
def remove_header(block):
    data = block[:64].decode('utf-8', 'ignore')
    match = re.search('\x01\r\r\n[\w\d\s]{4}\r\r\n\w{4}\d{2} \w{4} \d{6}[\s\w\d]*\r\r\n', data)
    if match:
        return block[match.end():]
    else:
        return block


# Remove WMO end of transmission block
def remove_footer(block):
    if block.endswith('\r\r\n\x03'):
        return block[:-4]
    else:
        return block

# Read first block and remove header (no guarantee read() gets all data)
block = remove_header(sys.stdin.read())
blocks = []

# While we keep getting data, try to remove the footer. Easier than trying to remove footer
# only for last block.
while block:
    blocks.append(remove_footer(block))
    block = sys.stdin.read()

# Write the data out
with open(sys.argv[1], 'wb') as f:
    for b in blocks:
        f.write(b)
