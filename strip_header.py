#!/usr/bin/env python
# Copyright (c) 2015 Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import os
import os.path
import re
import sys


def init_logger():
    import logging
    import logging.handlers
    import socket

    # Set the global logger
    global logger
    logger = logging.getLogger('HeaderStripHandler')

    # Send logs to LDM's log if possible, otherwise send to stderr.
    try:
        handler = logging.handlers.SysLogHandler(address='/dev/log', facility='local0')
    except socket.error:
        handler = logging.StreamHandler()

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


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

try:
    init_logger()
    logger.debug('Started script.')

    # Read first block and remove header (no guarantee read() gets all data)
    block = remove_header(sys.stdin.read())
    blocks = []

    # While we keep getting data, try to remove the footer. Easier than trying to remove footer
    # only for last block.
    while block:
        blocks.append(remove_footer(block))
        block = sys.stdin.read()

    # Make sure directory exists
    target_file = sys.argv[1]
    target_dir = os.path.dirname(target_file)
    logger.debug('Writing to %s in %s', target_file, target_dir)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Write the data out
    with open(target_file, 'wb') as f:
        for b in blocks:
            f.write(b)
    logger.info('Successfully wrote to: %s', target_file)
except Exception as e:
    logger.exception("Exception!")
