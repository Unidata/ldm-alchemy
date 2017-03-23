# Copyright (c) 2017 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import asyncio
from collections import namedtuple
import logging
import re
import struct


# Find WMO header and remove
def remove_header(block):
    r"""Find and remove WMO message header bytes."""
    data = block[:64].decode('utf-8', 'ignore')
    match = remove_header.header_regex.search(data)
    if match:
        return block[match.end():]
    else:
        return block
remove_header.header_regex = re.compile(r'\x01\r\r\n[\w\d\s]{4}\r\r\n'
                                        r'\w{4}\d{2} \w{4} \d{6}[\s\w\d]*\r\r\n')


# Remove WMO end of transmission block
def remove_footer(block):
    r"""Find and remove WMO end of transmission bytes."""
    if block.endswith(b'\r\r\n\x03'):
        return block[:-4]
    else:
        return block

#
# LDM processing stuff
#
len_struct = struct.Struct('I')
meta_struct = struct.Struct('6IQiII')
ldm_meta = namedtuple('LDMMeta', 'meta_length md5_1 md5_2 md5_3 md5_4 prod_len creation_secs '
                                 'creation_micro feed_type seq_num')


def init_logger(formatter=None, stream=None):
    import logging.handlers

    # Set the global logger
    logger = logging.getLogger('LDMHandler')
    handler = logging.StreamHandler(stream)

    if formatter:
        handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = logging.getLogger('LDMHandler')


# Raises an EOFError if we get a 0 byte read, which is by definition an EOF in Python
async def check_read(fobj, num_bytes):
    r"""Read data from file object, """
    data = await fobj.readexactly(num_bytes)
    if data:
        return data
    raise EOFError('Got 0 byte read.')


async def read_byte_string(fobj):
    r"""Read in an ascii string.
    
    First reads in the string length.
    """
    data = await check_read(fobj, len_struct.size)
    slen, = len_struct.unpack(data)
    s = await check_read(fobj, slen)
    return s.decode('ascii')


# Stuff for parsing LDM metadata
async def read_metadata(fobj):
    r"""Read in and parse LDM metadata."""
    data = await check_read(fobj, meta_struct.size)
    meta = ldm_meta(*meta_struct.unpack(data))
    logger.debug('LDM metadata: %s', meta)
    prod_ident = await read_byte_string(fobj)
    logger.debug('Got prod_id: %s', prod_ident)
    prod_origin = await read_byte_string(fobj)
    logger.debug('Got origin: %s', prod_origin)
    return prod_ident, meta.prod_len

#
# Handling of input
#
async def read_product(stream):
    r"""Read in a product from the LDM stream.
    
    Reads metadata from LDM for prod id and product size, then reads in the appropriate
    amount of data.
    """
    prod_id, prod_length = await read_metadata(stream)
    logger.debug('Reading product: %s', prod_id)
    data = await stream.readexactly(prod_length)
    logger.debug('Read product. (%d bytes)', len(data))
    return prod_id, data


async def read_stream(loop, file, sinks, cleanup=None, timeout=None):
    r"""Asynchronously read products from a stream.
    
    Dispatches products to one or more sinks. Also cleans up when done and optionally
    times out.
    """
    stream_reader = asyncio.StreamReader(loop=loop)
    transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stream_reader), file)
    while True:
        try:
            product = await asyncio.wait_for(read_product(stream_reader), timeout)
            for sink in sinks:
                await sink.put(product)
        except (EOFError, asyncio.TimeoutError):
            break
    # If we get an EOF, flush out the queues top down, then save remaining
    # chunks to disk for reloading later.
    logger.info('Closing out processing.')
    for sink in sinks:
        logger.debug('Flushing product sinks.')
        await sink.join()
    if cleanup:
        await cleanup()
    await asyncio.sleep(0.01)  # Just enough to let other things close out
    transport.close()
