# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import asyncio
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import struct
import sys


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

    logger.addHandler(handler)
    return logger

logger = logging.getLogger('LDM')

#
# Structures for decoding LDM binary metadata
#
len_struct = struct.Struct('I')
meta_struct = struct.Struct('6IQiII')
ldm_meta = namedtuple('LDMMeta', 'meta_length md5_1 md5_2 md5_3 md5_4 prod_len creation_secs '
                                 'creation_micro feed_type seq_num')
Product = namedtuple('product', 'prod_id data')


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

class LDMReader:
    def __init__(self, *, nthreads=20):
        self.nthreads = nthreads
        self.sinks = []
        self.tasks = []
        self.queues = []

    def process(self, stream=sys.stdin.buffer):
        try:
            self.loop = asyncio.get_event_loop()
            self.loop.set_default_executor(ThreadPoolExecutor(self.nthreads))

            self.tasks.append(asyncio.ensure_future(self.log_counts()))

            # Add callback for stdin and start loop
            logger.info('Starting main event loop.')
            self.loop.run_until_complete(self.read_stream(stream))
            self.loop.close()
        except Exception as e:
            logger.exception('Exception raised!', exc_info=e)

    async def read_stream(self, stream):
        stream_reader = asyncio.StreamReader(loop=self.loop)
        transport, _ = await self.loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(stream_reader), stream)
        try:
            while True:
                product = await read_product(stream_reader)
                for sink in self.sinks:
                    await sink.put(product)
        except EOFError:
            # If we get an EOF, flush out the queues top down, then save remaining
            # products to disk for reloading later.
            logger.warning('Finishing due to EOF.')
            for sink in self.sinks:
                logger.debug('Flushing product queue.')
                await sink.join()
            for t in self.tasks:
                t.cancel()
            await asyncio.sleep(0.01)  # Just enough to let other things close out
            transport.close()

    async def log_counts(self):
        while True:
            await asyncio.sleep(5)
            logger.info('Tasks: %d', len(asyncio.Task.all_tasks(self.loop)))

