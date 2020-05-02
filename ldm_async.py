# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import asyncio
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
from pathlib import Path
import struct
import sys

logger = logging.getLogger('LDM')

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

class LDMReader:
    def __init__(self, *, nthreads=20):
        self.nthreads = nthreads
        self.sinks = []
        self.tasks = []

    def add_standalone_task(self, async_func):
        task = asyncio.ensure_future(async_func())
        self.tasks.append(task)

    def connect(self, dest):
        task = asyncio.ensure_future(dest.mainloop())
        self.tasks.append(task)
        self.sinks.append(dest.queue)

    def process(self, stream=sys.stdin.buffer):
        try:
            # Get the default loop and add an executor for running synchronous code
            self.loop = asyncio.get_event_loop()
            self.loop.set_default_executor(ThreadPoolExecutor(self.nthreads))

            # Add a debugging task that shows the current task count
            self.add_standalone_task(self.log_tasks)

            # Run our main task until it finishes
            logger.info('Starting main event loop.')
            self.loop.run_until_complete(self.read_stream(stream))

            # Close up
            self.loop.close()
        except Exception as e:
            logger.exception('Exception raised!', exc_info=e)

    async def read_stream(self, stream):
        # Set up an async way to read data from our stream
        stream_reader = asyncio.StreamReader(loop=self.loop)
        transport, _ = await self.loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(stream_reader), stream)

        # Continuously loop, reading products and sending them to connected tasks
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
            await asyncio.sleep(0.05)  # Just enough to let other things close out
            transport.close()

    async def log_tasks(self):
        while True:
            await asyncio.sleep(60)
            logger.info('Tasks: %d', len(asyncio.Task.all_tasks(self.loop)))


class Job:
    def __init__(self, name):
        self.name = name
        self.queue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()

    async def mainloop(self):
        while True:
            item = await self.queue.get()
            try:
                fut = self.loop.run_in_executor(None, self.run, item)
                fut.add_done_callback(functools.partial(self.done_callback, item))
            except Exception:
                logger.exception(f'Exception executing task {self.name}:',
                                 exc_info=sys.exc_info())

    def done_callback(self, item, future):
        try:
            res = future.result()
            self.finish(item, res)
        except IOError:
            logger.warning('Failed to process %s. Queuing for retry...', item)
            self.loop.call_later(15, self.queue.put_nowait, item)
        except Exception:
            logger.exception('Exception on finishing item %s:', item, exc_info=sys.exc_info())
        finally:
            self.queue.task_done()

    def run(self, item):
        logger.debug('Processing %s...', item)

    def finish(self, item, result):
        logger.debug('Finished %s.', item)
