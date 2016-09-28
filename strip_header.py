#!/usr/bin/env python
# Copyright (c) 2015-2016 University Corporation for Atmospheric Research/Unidata
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


#
# Argument parsing
#
def setup_arg_parser():
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Read NEXRAD Level2 LDM compressed blocks'
                                     ' and assemble when they are done arriving.')
    parser.add_argument('-d', '--decompress', help='Decompress file', action='store_true')
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('filename', help='Output filename', nargs=1)

    return parser


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
    if block.endswith(b'\r\r\n\x03'):
        return block[:-4]
    else:
        return block

try:
    init_logger()
    parser = setup_arg_parser()
    args = parser.parse_args()

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    logger.debug('Started script.')

    # Read first block and remove header (no guarantee read() gets all data)
    block = remove_header(sys.stdin.buffer.read())
    blocks = []

    # While we keep getting data, try to remove the footer. Easier than trying to remove footer
    # only for last block.
    while block:
        blocks.append(remove_footer(block))
        block = sys.stdin.buffer.read()

    # Make sure directory exists
    target_file = args.filename[0]
    target_dir = os.path.dirname(target_file)
    logger.debug('Writing to %s in %s', target_file, target_dir)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Decompress file if necessary
    if args.decompress:
        import gzip
        from io import BytesIO
        reader = gzip.GzipFile(mode='rb', fileobj=BytesIO(b''.join(blocks)))
        blocks = [reader.read()]

    # Write the data out
    with open(target_file, 'wb') as f:
        for b in blocks:
            f.write(b)
    logger.info('Successfully wrote to: %s', target_file)
except Exception as e:
    logger.exception("Exception!")
