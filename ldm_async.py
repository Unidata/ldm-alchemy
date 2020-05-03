# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import asyncio
from collections import namedtuple
import logging
from pathlib import Path
import struct
import sys

from async_base import Main

logger = logging.getLogger('alchemy')

def set_log_file(filename, when='midnight', backupCount=14):
    """Set up logging to a file.

    Given a log file name, sets up the logger to write to this filename on a rotating basis
    within the appropriate logging location for the LDM logs.

    Parameters
    ----------
    filename : str
        Filename to which the logfile should be written.
    when : str
        When to rotate the log
    backupCount : int
        Number of days of logs to keep

    Returns
    -------
        The LDM logger
    """
    import logging.handlers

    # Send to our own rotating log in LDM's log directory
    log_dir = Path.home() / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(log_dir / filename, when=when,
                                                        backupCount=backupCount)
    handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s [%(funcName)s]: '
                                           '%(message)s'))

    logger.addHandler(handler)
    return logger

def set_log_level(args):
    """Set appropriate logging level based on command line args."""
    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10
    logger.debug('Logging initialized.')

def setup_arg_parser(description):
    """Set up command line argument parsing."""
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--threads', help='Specify number of threads to use.', default=20,
                        type=int)
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('other', help='Other arguments for LDM identification', type=str,
                        nargs='*')
    return parser


#
# Structures for decoding LDM binary metadata
#
len_struct = struct.Struct('I')
meta_struct = struct.Struct('6IQiII')
ldm_meta = namedtuple('LDMMeta', 'meta_length md5_1 md5_2 md5_3 md5_4 prod_len creation_secs '
                                 'creation_micro feed_type seq_num')
_Product = namedtuple('product', 'prod_id data')

class Product(_Product):
    def __str__(self):
        return self.prod_id


async def check_read(stream, num_bytes):
    """Read bytes from an async stream and check for EOF.

    This detects EOF by getting back 0 bytes from the read, which Python defines as EOF.

    Parameters
    ----------
    stream : file-like object, async compatible
        Stream to read from
    num_bytes : int
        Number of bytes to read
    """
    data = await stream.readexactly(num_bytes)
    if data:
        return data
    raise EOFError('Got 0 byte read.')


async def read_ascii_string(stream):
    """Read an ascii string from stream and return it.

    Works by reading the count of bytes in the string, then reads that many bytes.

    Parameters
    ----------
    stream : file-like object, async compatible

    Returns
    -------
        ascii string of read in bytes
    """
    data = await check_read(stream, len_struct.size)
    slen, = len_struct.unpack(data)
    s = await check_read(stream, slen)
    return s.decode('ascii')


async def read_ldm_metadata(stream):
    """Read an product from the LDM input stream.

    Needs to be supplied an async-compatible input stream that contains LDM metadata
    in-between products. Reads in binary metadata structure as well as the product identifier
    and returns them.

    Parameters
    ----------
    stream : file-like object, async compatible

    Returns
    -------
        Tuple(LDMMeta, str)
    """
    data = await check_read(stream, meta_struct.size)
    meta = ldm_meta(*meta_struct.unpack(data))
    logger.debug('LDM metadata: %s', meta)
    prod_ident = await read_ascii_string(stream)
    logger.debug('Got prod_id: %s', prod_ident)
    prod_origin = await read_ascii_string(stream)
    logger.debug('Got origin: %s', prod_origin)
    return meta, prod_ident


async def read_product(stream):
    """Read a product from the LDM input stream.

    Needs to be supplied an async-compatible input stream that contains LDM metadata
    in-between products. This uses the binary metadata to determine how large the
    subsequent product is.

    Parameters
    ----------
    stream : file-like object, async compatible

    Returns
    -------
        Product
    """
    metadata, prod_id = await read_ldm_metadata(stream)
    logger.debug('Reading product {}'.format(prod_id))
    data = await stream.readexactly(metadata.prod_len)
    logger.debug('Read product. (%d bytes)', len(data))
    return Product(prod_id, data)


#
# Handling of input
#

async def product_generator(loop, stream):
    stream_reader = asyncio.StreamReader(loop=loop)
    transport, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(stream_reader), stream)

    try:
        while True:
            product = await read_product(stream_reader)
            yield product
    except EOFError:
        logger.warning('Finishing due to EOF.')
        transport.close()


class LDMReader(Main):
    def __aiter__(self):
        return product_generator(self.loop, sys.stdin.buffer)
