#!/usr/bin/env python
# Copyright (c) 2015-2016 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import asyncio
import functools
import glob
import json
import logging
import os
import os.path
import shutil
import struct
import sys
import threading

from collections import namedtuple, defaultdict
from contextlib import contextmanager
from datetime import datetime


#
# Set up logging
#
class ProdInfoAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'extra' in kwargs:
            try:
                kwargs['extra'] = kwargs['extra']._asdict()
            except AttributeError:
                kwargs['extra'] = dict(zip(['site', 'volume_id'], kwargs['extra']))
        else:
            kwargs['extra'] = self.extra
        return msg, kwargs

    @classmethod
    def from_logger(cls, logger):
        return cls(logger, {'site': '----', 'volume_id': 0})


def init_logger(formatter=None):
    import logging.handlers
    import socket

    # Set the global logger
    logger = logging.getLogger('LDMHandler')

    # Send logs to LDM's log if possible, otherwise send to stderr.
    try:
        handler = logging.handlers.SysLogHandler(address='/dev/log', facility='local0')
    except (FileNotFoundError, socket.error):
        handler = logging.StreamHandler()

    if formatter:
        handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def init_lv2_logger():
    import faulthandler

    # Set up some kind of logging for crashes
    os.makedirs('logs', exist_ok=True)
    faulthandler.enable(open('logs/l2assemble-crash.log', 'a'))

    fmt = '%(filename)s [%(funcName)s]: [%(site)s %(volume_id)03d] %(message)s'
    return init_logger(logging.Formatter(fmt=fmt))


def log_rmtree_error(func, path, exc):
    logger.error('Error removing (%s %s)', func, path)

logger = ProdInfoAdapter.from_logger(logging.getLogger('LDMHandler'))


#
# Keeping stats
#
class ChunkStats(object):
    def __init__(self, fname):
        import sqlite3
        self._conn = sqlite3.connect(fname)
        try:
            self._conn.execute('CREATE TABLE volumes '
                               '(site text, date timestamp, volume integer, count integer, '
                               'missing_start integer, missing_chunks text)')
        except sqlite3.OperationalError:
            pass

    def log_volume(self, prod_info, num_chunks, missing_start, missing):
        self._conn.execute('INSERT INTO volumes VALUES(?,?,?,?,?,?)',
                           (prod_info.site, prod_info.dt, prod_info.volume_id, num_chunks,
                            int(missing_start), ','.join(missing)))
        self._conn.commit()

    def __del__(self):
        self._conn.close()

#
# LDM processing stuff
#
hdr_struct = struct.Struct('>12s2L4s')  # Structure of volume header
len_struct = struct.Struct('I')
meta_struct = struct.Struct('6IQiII')
ldm_meta = namedtuple('LDMMeta', 'meta_length md5_1 md5_2 md5_3 md5_4 prod_len creation_secs '
                                 'creation_micro feed_type seq_num')
_ProdInfo = namedtuple('ProdInfo',
                       'format site dt volume_id chunk_id chunk_type version unused')


class ProdInfo(_ProdInfo):
    __slots__ = _ProdInfo.__slots__  # To fix _asdict(). See Python #249358

    def __str__(self):
        mod = self._replace(dt=self.dt.strftime('%Y%m%d%H%M%S'), chunk_id=str(self.chunk_id),
                            volume_id=str(self.volume_id))
        return '_'.join(mod)

    def __hash__(self):
        return hash(self.to_key())

    def __eq__(self, other):
        return self.site == other.site and self.volume_id == other.volume_id

    def to_key(self):
        return self.site, self.volume_id

    @classmethod
    def fromstring(cls, s):
        c = cls(*s.split('_'))
        return c._replace(dt=datetime.strptime(c.dt, '%Y%m%d%H%M%S'), chunk_id=int(c.chunk_id),
                          volume_id=int(c.volume_id))

    # Turn the string 'L2-BZIP2/KFTG/20150908215946/494/43/I/V06/0' into useful information
    @classmethod
    def from_ldm_string(cls, s):
        return cls.fromstring(s.replace('/', '_'))

    def as_vol_hdr(self):
        version = 'AR2V00' + self.version[1:] + '.' + str(self.volume_id)
        timestamp = (self.dt - datetime(1970, 1, 1)).total_seconds()
        date = int(timestamp // 86400)
        time = int(timestamp - date * 86400)
        return hdr_struct.pack(version.encode('ascii'), date + 1, time * 1000,
                               self.site.encode('ascii'))

    def to_sns_filter_attrs(self):
        """Turn into a set of appropriate filterable values for SNS."""
        return {'SiteID': {'DataType': 'String', 'StringValue': self.site},
                'DateTime': {'DataTYpe': 'String', 'StringValue': self.dt.isoformat()},
                'VolumeID': {'DataType': 'Number', 'StringValue': str(self.volume_id)},
                'ChunkID': {'DataType': 'Number', 'StringValue': str(self.chunk_id)},
                'ChunkType': {'DataType': 'String', 'StringValue': self.chunk_type},
                'L2Version': {'DataType': 'String', 'StringValue': self.version}}

    def to_sns_message_dict(self):
        return {'SiteID': self.site, 'DateTime': self.dt.isoformat(),
                'VolumeID': self.volume_id, 'ChunkID': self.chunk_id,
                'ChunkType': self.chunk_type, 'L2Version': self.version}


# Raises an EOFError if we get a 0 byte read, which is by definition an EOF in Python
async def check_read(fobj, num_bytes):
    data = await fobj.readexactly(num_bytes)
    if data:
        return data
    raise EOFError('Got 0 byte read.')


async def read_byte_string(fobj):
    data = await check_read(fobj, len_struct.size)
    slen, = len_struct.unpack(data)
    s = await check_read(fobj, slen)
    return s.decode('ascii')


# Stuff for parsing LDM metadata
async def read_metadata(fobj):
    data = await check_read(fobj, meta_struct.size)
    meta = ldm_meta(*meta_struct.unpack(data))
    logger.debug('LDM metadata: %s', meta)
    prod_ident = await read_byte_string(fobj)
    logger.debug('Got prod_id: %s', prod_ident)
    prod_origin = await read_byte_string(fobj)
    logger.debug('Got origin: %s', prod_origin)
    return prod_ident, meta.prod_len


#
# Caching and storage of chunks
#
# Overriding defaultdict--essentially (at first) just to pass key to factory
class VolumeStore(defaultdict):
    RELOAD_FILE = '.vols_restart'

    def __init__(self, loop, cache_dir, gen_header, s3=None, s3_path_format=''):
        super(defaultdict, self).__init__()
        self._loop = loop
        self._cache_dir = cache_dir
        self._gen_header = gen_header
        self._s3_buckets = S3BucketPool(s3) if s3 else None
        self._s3_path = s3_path_format

    def _reload_vols(self):
        if os.path.exists(self.RELOAD_FILE):
            with open(self.RELOAD_FILE, 'r') as reload:
                for line in reload:
                    line = line.rstrip()  # Pop off newline
                    if line:
                        logger.info('Reloading: %s', line)
                        pi = ProdInfo.fromstring(line)
                        self.__missing__(pi)  # Trigger creation
            os.remove(self.RELOAD_FILE)

    def __missing__(self, key):
        new = self._create_store(key)
        self[key] = new
        return new

    def _create_store(self, prod_info):
        logger.debug('Creating store.', extra=prod_info)

        store = ChunkStore()

        # Try to load any data for this volume from the cache
        if self._s3_buckets:
            fut = loop.run_in_executor(None, store.loadfroms3, self._s3_buckets,
                                       self._s3_path.format(prod_info), prod_info)
            fut.add_done_callback(lambda f: store.ready.set())
        else:
            cache = self.cache_dir(prod_info.to_key())
            if os.path.exists(cache):
                logger.debug('Loading previously stored chunks from: %s', cache,
                             extra=prod_info)
                fut = loop.run_in_executor(None, store.loadfromdir, cache)
                fut.add_done_callback(lambda f: store.ready.set())
                fut.add_done_callback(lambda f: shutil.rmtree(cache,
                                                              onerror=log_rmtree_error))
            else:
                store.ready.set()

            # Remove any old cache directories
            self.clear_old_caches(prod_info.to_key())

        # Pass in call-back to call when done. We don't use the standard future callback
        # because it will end up queued--we need to run immediately.
        store.task = asyncio.ensure_future(
            store.wait_for_chunks(self.timeout,
                                  functools.partial(self.chunk_store_done, key=prod_info)))
        store.ensure_header(self._gen_header)

        return store

    def cache_dir(self, key):
        site, vol_num = key
        return os.path.join(self._cache_dir, '.' + site, '%03d' % vol_num)

    def clear_old_caches(self, key):
        logger.debug('Checking for old caches...', extra=key)
        # List all old cache directories for this site
        site, cur_vol = key
        for fname in glob.glob(os.path.join(self._cache_dir, '.' + site, '[0-9][0-9][0-9]')):
            if os.path.isdir(fname):  # Minor sanity check that this is ours
                logger.debug('Found: %s', fname, extra=key)

                # Use this volume number as a proxy for time
                num = int(os.path.basename(fname))

                # Find the difference, account for the wrap 999->0
                diff = cur_vol - num
                if diff < 0:
                    diff += 1000

                # If the one we found is more than 30 past, delete it
                if diff > 30:
                    logger.info('Deleting old cache: %s', fname, extra=key)
                    shutil.rmtree(fname, onerror=log_rmtree_error)

    def save(self):
        if not self._s3_buckets:
            for key, chunks in self.items():
                cache = self.cache_dir(key.to_key())
                logger.warning('Caching chunks to: %s', cache, extra=key)
                chunks.savetodir(cache)

    async def finish(self):
        # Need to iterate over copy of keys because items could be removed during iteration
        for key in list(self.keys()):
            logger.debug('Flushing chunk store queue.', extra=key)
            store = self[key]
            await store.finish()
            store.task.cancel()

        # Mark volumes that do not finish for reload in case we don't get any more chunks
        if self:
            with open(self.RELOAD_FILE, 'w') as reload:
                for key in self:
                    txt = str(key)
                    logger.info('Marking for reload: %s', txt, extra=key)
                    reload.write('%s\n' % txt)

        logger.debug('Flushing volumes queue')
        await self.vol_dest.join()

    async def wait_for_chunks(self, src, vol_dest, timeout):
        self.vol_dest = vol_dest
        self.timeout = timeout
        self._reload_vols()
        while True:
            # Get the next chunk when available
            chunk = await src.get()

            # Find the appropriate store (will be created if necessary)
            await self[chunk.prod_info].enqueue(chunk)
            src.task_done()

    def chunk_store_done(self, key):
        logger.debug('Chunk store finished.', extra=key)
        store = self.pop(key)
        self.vol_dest.put_nowait(store)


Chunk = namedtuple('Chunk', 'prod_info data')


class ChunkStore(object):
    def __init__(self):
        self._store = dict()
        self.first = self.last = -1
        self._vol_hdr = b''
        self._add_header = False
        self._queue = asyncio.Queue()
        self.ready = asyncio.Event()

    def loadfromdir(self, path):
        # Go find all the appropriately named files in the directory and load them
        for fname in sorted(glob.glob(os.path.join(path, 'L2-BZIP2_*')),
                            key=lambda f: ProdInfo.fromstring(os.path.basename(f)).chunk_id):
            name = os.path.basename(fname)
            self.add(Chunk(prod_info=ProdInfo.fromstring(name), data=open(fname, 'rb').read()))
        logger.info('Loaded %d chunks from cache %s', len(self), path)

    def savetodir(self, path):
        # Create the directory if necessary
        if not os.path.exists(path):
            os.makedirs(path)

        # Write the chunks
        logger.warning('Saving %d chunks: [%s]', len(self),
                       ' '.join(map(str, self._store.keys())),
                       extra=self.first_chunk().prod_info)
        for chunk in self:
            with open(os.path.join(path, str(chunk.prod_info)), 'wb') as outf:
                if chunk.prod_info.chunk_id == self.first:
                    outf.write(self.vol_hdr)
                outf.write(chunk.data)

    def loadfroms3(self, bucket_pool, key, prod_info):
        loaded = False
        with bucket_pool.use() as bucket:
            prefix = '-'.join(key.split('-')[:-2])
            for obj in bucket.objects.filter(Prefix=prefix):
                name = os.path.basename(obj.key)
                date, time, chunk, chunk_type = name.split('-')
                pi = prod_info._replace(chunk_id=int(chunk), chunk_type=chunk_type)

                # When loading from cache, make sure we don't already have a chunk before
                # adding it. If it matches our original one, queue it up instead of adding
                # it directly.
                chunk = Chunk(prod_info=pi, data=obj.get()['Body'].read())
                if pi.chunk_id == prod_info.chunk_id:
                    self.enqueue_nowait(chunk)
                elif pi.chunk_id not in self._store:
                    loaded = True
                    self.add(chunk)

        if loaded:
            logger.info('Loaded %d chunks from S3 cache %s', len(self), prefix,
                        extra=prod_info)

    def __len__(self):
        return len(self._store)

    def need_more(self):
        return len(self) != self.last

    def min_id(self):
        return min(self._store.keys()) if self._store else 0

    def max_id(self):
        return max(self._store.keys()) if self._store else 0

    def first_chunk(self):
        return next(iter(self._store.values()))

    # Iterate in the order of the keys, but only return the value
    def __iter__(self):
        return iter(i[1] for i in sorted(self._store.items()))

    async def finish(self):
        await self._queue.join()

    async def enqueue(self, chunk):
        await self._queue.put(chunk)

    async def wait_for_chunks(self, timeout, when_done):
        chunk = None
        await self.ready.wait()
        while self.need_more() or not self._queue.empty():
            try:
                chunk = await asyncio.wait_for(self._queue.get(), timeout)
                await self.add_wait(chunk)
                self._queue.task_done()
            except asyncio.TimeoutError:
                kwargs = {'extra': chunk.prod_info} if chunk else {}
                logger.warning('Finishing due to timeout.', **kwargs)
                break

        when_done()

    async def add_wait(self, chunk):
        self.add(chunk)
        # If we think we're done but we didn't just handle the end of volume,
        # wait to make sure more chunks don't arrive.
        if not self.need_more() and chunk.prod_info.chunk_type != 'E':
            await asyncio.sleep(30)

    def enqueue_nowait(self, chunk):
        self._queue.put_nowait(chunk)

    # Add a chunk to our store. If this was the start or end, note that as well.
    def add(self, chunk):
        max_id = self.max_id()
        chunk_id = chunk.prod_info.chunk_id
        if chunk_id != max_id + 1:
            if chunk_id in self._store:
                if chunk_id > 1:
                    logger.warning('Duplicate chunk: %d', chunk_id, extra=chunk.prod_info)
            else:
                logger.warning('Chunks out of order--Got: %d Max: %d', chunk_id, max_id,
                               extra=chunk.prod_info)
        logger.debug('Added chunk: %d', chunk_id, extra=chunk.prod_info)

        # Not only do we need to note the first block, we need to pop off the header bytes
        chunk_type = chunk.prod_info.chunk_type
        if chunk_type == 'S':
            self.first = chunk_id
            self.vol_hdr = chunk.data[:hdr_struct.size]
            chunk = chunk._replace(data=chunk.data[hdr_struct.size:])
        elif chunk_type == 'E':
            self.last = chunk_id

        self._store[chunk_id] = chunk

    def ensure_header(self, f):
        self._add_header = f

    # Reconstruct a level 2 volume header if we miss the first block
    @property
    def vol_hdr(self):
        if not self._vol_hdr and self._add_header:
            pi = self.first_chunk().prod_info
            hdr = pi.as_vol_hdr()
            logger.warning('Created volume header for first chunk: %s', hdr, extra=pi)
            self._vol_hdr = hdr
        return self._vol_hdr

    @vol_hdr.setter
    def vol_hdr(self, hdr):
        self._vol_hdr = hdr

    # List any blocks we missed
    def missing(self):
        return map(str, set(range(1, self.max_id() + 1)) - set(self._store.keys()))


#
# Handling of writing chunks to a variety of destinations in a bunch of formats
#
class ChunkWriter(object):
    def __init__(self, fobj, fmt):
        self.fobj = fobj
        self.needclose = False
        if fmt == 'raw':
            self._process_chunk = lambda chunk: chunk
        else:
            import bz2
            if fmt == 'gz':
                import gzip
                self.fobj = gzip.GzipFile(filename=self.fobj.filename, fileobj=fobj, mode='wb')
                self.needclose = True
            elif fmt == 'bz2':
                self.fobj = bz2.BZ2File(fobj, mode='wb')
                self.needclose = True
            self._process_chunk = lambda chunk: bz2.decompress(chunk[4:])

    def write(self, data):
        self.fobj.write(data)

    def write_chunk(self, chunk):
        self.write(self._process_chunk(chunk.data))

    def write_chunks(self, chunks):
        # Write the volume header if we have one
        if chunks.vol_hdr:
            self.write(chunks.vol_hdr)
        else:
            logger.error('Missing volume header for: %s', self.fobj.filename,
                         extra=chunks.first_chunk().prod_info)

        # Write the data chunks
        for num, chunk in enumerate(chunks):
            try:
                self.write_chunk(chunk)
            except (OSError, IOError):
                logger.error('Error writing chunk: %d', num, extra=chunk.prod_info)

        if self.needclose:
            self.fobj.close()


class DiskFile(object):
    def __init__(self, base_dir, path, name, fallback_num):
        # Create the output dir if necessary
        out_dir = os.path.join(base_dir, path)
        if not os.path.exists(out_dir):
            logger.debug('Creating dir: %s', out_dir)
            os.makedirs(out_dir)

        # Check to make sure the file doesn't already exist
        outname = os.path.join(out_dir, name)
        if os.path.exists(outname):
            outname = self.fallback(outname, fallback_num)

        self.filename = name
        self._fobj = open(outname, 'wb')

    @staticmethod
    def fallback(outname, fallback_num):
        newname = outname + '.%03d' % fallback_num
        logger.error('%s already exists!. Falling back to %s.', outname, newname)
        return newname

    def write(self, data):
        return self._fobj.write(data)

    def close(self):
        self._fobj.close()


class S3File(DiskFile):
    def __init__(self, bucket_pool, path, name, fallback_num, s3_kwargs):
        from io import BytesIO

        logger.debug('Writing to S3 bucket: %s', bucket_pool.bucket_name)
        self.filename = name
        self._bucket_pool = bucket_pool
        self._key = path + '/' + name
        self._fobj = BytesIO()
        self._fallback_num = fallback_num
        self._s3_kwargs = s3_kwargs

    @staticmethod
    def _exists(obj):
        import botocore.exceptions
        try:
            obj.version_id
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
            raise IOError from e
        return True

    @staticmethod
    def put_checked(bucket, key, data, **kwargs):
        import botocore.exceptions
        import hashlib
        import base64
        try:
            # Calculate MD5 checksum for integrity
            digest = base64.b64encode(hashlib.md5(data).digest()).decode('ascii')

            # Write to S3
            logger.debug('Uploading to S3 under key: %s (md5: %s)', key, digest)
            bucket.put_object(Key=key, Body=data, ContentMD5=digest, **kwargs)
        except botocore.exceptions.ClientError as e:
            logger.error(str(e))
            raise IOError from e

    def close(self):
        data = self._fobj.getvalue()

        # Get the object and try to make sure it doesn't exist
        with self._bucket_pool.use() as bucket:
            obj = bucket.Object(self._key)
            if self._exists(obj):
                # If the key already exists, only fallback if the new data are smaller.
                # Otherwise, overwrite.
                hdr = obj.get()
                if len(data) < hdr['ContentLength']:
                    obj = bucket.Object(self.fallback(self._key, self._fallback_num))

            # Upload to S3
            self.put_checked(bucket, obj.key, data, **self._s3_kwargs)
        super(S3File, self).close()


#
# Coroutines for handling S3 and saving volumes
#
def when_item_done(loop, queue, name, item, future):
    try:
        future.result()
        logger.debug('Finished %s.', name)
    except IOError:
        logger.warning('Failed to process %s. Queuing for retry...', name)
        loop.call_later(15, queue.put_nowait, item)
    except Exception:
        logger.exception('Item done exception:', exc_info=sys.exc_info())
    finally:
        queue.task_done()


async def write_chunks_s3(loop, queue, bucket_pool, sns_pool):
    while True:
        chunk = await queue.get()
        try:
            key = args.key.format(chunk.prod_info)
            logger.debug('Writing chunk to %s on S3 %s', key, bucket_pool.bucket_name,
                         extra=chunk.prod_info)
            fut = loop.run_in_executor(None, upload_chunk_s3, bucket_pool, key, chunk, sns_pool)
            fut.add_done_callback(functools.partial(when_item_done, loop, queue, key, chunk))
        except Exception:
            logger.exception('write_chunks_s3 exception:', exc_info=sys.exc_info())


def upload_chunk_s3(s3_pool, key, chunk, sns_pool):
    with s3_pool.use() as bucket, sns_pool.use() as topic:
        S3File.put_checked(bucket, key, chunk.data)
        prod_info = chunk.prod_info
        message = {'S3Bucket': s3_pool.bucket_name, 'Key': key}
        message.update(prod_info.to_sns_message_dict())
        topic.publish(Message=json.dumps(message),
                      MessageAttributes=prod_info.to_sns_filter_attrs())


async def save_volume(loop, queue, file_factory, fmt, statsfile):
    if statsfile:
        stats = ChunkStats(statsfile)

    while True:
        chunks = await queue.get()

        try:
            # Determine file name
            prod_info = chunks.first_chunk().prod_info
            fname = args.filename.format(prod_info)
            if fmt != 'raw':
                fname += '.' + fmt

            path = args.path.format(prod_info)

            # Decide how to log
            missing = list(chunks.missing())
            status = 'incomplete' if missing or chunks.last < 1 else 'complete'
            logger.info('%s %s %d %d %d [%s]', fname, status, chunks.first,
                        chunks.last, len(chunks), ' '.join(missing), extra=prod_info)
            if statsfile:
                stats.log_volume(prod_info, len(chunks), chunks.last < 0, chunks.missing())

            # Set up and write file in another thread
            file = file_factory(path, fname, chunks.min_id())
            fut = loop.run_in_executor(None, write_file, file, fmt, chunks)
            fut.add_done_callback(functools.partial(when_item_done, loop, queue, fname,
                                                    chunks))
        except Exception:
            logger.exception('Save volume exception:', exc_info=sys.exc_info())


def write_file(file, fmt, chunks):
    cw = ChunkWriter(file, fmt)
    cw.write_chunks(chunks)
    file.close()


#
# Handling of input
#
async def read_chunk(stream):
    # Read metadata from LDM for prod id and product size, then read in the appropriate
    # amount of data.
    prod_id, prod_length = await read_metadata(stream)
    prod_info = ProdInfo.from_ldm_string(prod_id)
    logger.debug('Reading chunk {0.chunk_id} ({0.chunk_type}) for {0.site} '
                 '{0.volume_id} {0.dt}'.format(prod_info), extra=prod_info)
    data = await stream.readexactly(prod_length)
    logger.debug('Read chunk. (%d bytes)', len(data), extra=prod_info)
    return Chunk(prod_info, data)


async def read_stream(loop, file, vols, sinks, tasks):
    stream_reader = asyncio.StreamReader(loop=loop)
    transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stream_reader), file)
    try:
        while True:
            chunk = await read_chunk(stream_reader)
            for sink in sinks:
                await sink.put(chunk)
    except EOFError:
        # If we get an EOF, flush out the queues top down, then save remaining
        # chunks to disk for reloading later.
        logger.warning('Finishing due to EOF.')
        for sink in sinks:
            logger.debug('Flushing chunk queue.')
            await sink.join()
        await vols.finish()
        vols.save()
        for t in tasks:
            t.cancel()
        await asyncio.sleep(0.01)  # Just enough to let other things close out
        transport.close()


async def log_counts(loop, vols):
    while True:
        await asyncio.sleep(300)
        logger.info('Volumes: %d Tasks: %d', len(vols), len(asyncio.Task.all_tasks(loop)))


#
# Pools for AWS access objects
#
class SharedObjectPool(object):
    """A shared pool of managed objects

    Objects are created while a lock is held, then are doled out and returned using
    a context manager.
    """
    _create_lock = threading.Lock()  # We want one lock among all subclasses

    def __init__(self):
        import queue
        self._queue = queue.Queue()

    def _create_new(self):
        pass

    def borrow(self):
        if self._queue.empty():
            with self._create_lock:
                return self._create_new()

        return self._queue.get()

    def put(self, item):
        self._queue.put(item)

    @contextmanager
    def use(self):
        obj = self.borrow()
        try:
            yield obj
        finally:
            self.put(obj)


class S3BucketPool(SharedObjectPool):
    def __init__(self, bucket):
        super().__init__()
        self.bucket_name = bucket

    def _create_new(self):
        import boto3
        return boto3.session.Session().resource('s3').Bucket(self.bucket_name)


class SNSBucketPool(SharedObjectPool):
    def __init__(self, name):
        import boto3
        super().__init__()
        self.sns_arn = boto3.client('sns').create_topic(Name=name)['TopicArn']

    def _create_new(self):
        import boto3
        return boto3.session.Session().resource('sns').Topic(self.sns_arn)


#
# Argument parsing
#
def setup_arg_parser():
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Read NEXRAD Level2 LDM compressed blocks'
                                     ' and assemble when they are done arriving.')
    parser.add_argument('-d', '--data_dir', help='Base output directory', type=str,
                        default='/data/ldm/pub/native/radar/level2')
    parser.add_argument('-s', '--s3', help='Write to specified S3 bucket rather than disk.',
                        type=str)
    parser.add_argument('-c', '--save-chunks', help='Write chunks to this S3 bucket.',
                        type=str)
    parser.add_argument('--sns', help='When saving chunks, publish to this SNS topic',
                        type=str, default='NewNEXRADLevel2ObjectFilterable')
    parser.add_argument('--s3-volume-args', help='Additional arguments to pass when sending '
                        ' volumes to S3. This is useful for passing along access controls.',
                        type=str)
    parser.add_argument('-k', '--key', help='Key format string when storing chunks. Uses '
                        'Python string format specification',
                        default='{0.site}/{0.volume_id}/{0.dt:%Y%m%d-%H%M%S}-'
                                '{0.chunk_id:03d}-{0.chunk_type}')
    parser.add_argument('-f', '--format', help='Format for output', type=str,
                        choices=('raw', 'bz2', 'gz'), default='raw')
    parser.add_argument('-g', '--generate_header', help='Generate volume header if missing',
                        action='store_true')
    parser.add_argument('-t', '--timeout', help='Timeout in seconds for waiting for data',
                        default=600, type=int)
    parser.add_argument('--threads', help='Specify number of threads to use.', default=20,
                        type=int)
    parser.add_argument('-a', '--stats', help='Enable stats saving. Specifies name of '
                        'sqlite3 file.', type=str)
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-p', '--path', help='Path format string. Uses Python '
                        'string format specification', default='{0.site}/{0.dt:%Y%m%d}')
    parser.add_argument('-n', '--filename', help='Filename format string. Uses Python '
                        'string format specification',
                        default='Level2_{0.site}_{0.dt:%Y%m%d_%H%M%S}.ar2v')
    parser.add_argument('other', help='Other arguments for LDM identification', type=str,
                        nargs='*')
    return parser


if __name__ == '__main__':
    from ast import literal_eval
    from concurrent.futures import ThreadPoolExecutor

    init_lv2_logger()
    args = setup_arg_parser().parse_args()

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10
    logger.debug('Logging initialized.')

    try:
        # Read directly from standard in buffer
        read_in = sys.stdin.buffer

        # Set up event loop
        loop = asyncio.get_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(args.threads))

        # Setup queue for saving volumes
        vol_queue = asyncio.Queue()
        if args.s3:
            # Parse the additional arguments if given
            if args.s3_volume_args:
                try:
                    args.s3_volume_args = literal_eval(args.s3_volume_args)
                except SyntaxError:
                    logger.warning('Error parsing args: %s', args.s3_volume_args)
                    args.s3_volume_args = dict()
            else:
                args.s3_volume_args = dict()
            logger.debug('Additional S3 volume args: %s', str(args.s3_volume_args))

            # Binding the bucket as a default keeps from having problems if variable is re-used
            def factory(path, fname, fallback, pool=S3BucketPool(args.s3)):
                return S3File(pool, path, fname, fallback, args.s3_volume_args)
        else:
            def factory(path, fname, fallback):
                return DiskFile(args.data_dir, path, fname, fallback)

        tasks = [asyncio.ensure_future(save_volume(loop, vol_queue, factory, args.format,
                                                   args.stats))]

        # Set up storing chunks internally
        chunk_queue = asyncio.Queue()
        volumes = VolumeStore(loop=loop, cache_dir=args.data_dir, gen_header=args.generate_header,
                              s3=args.save_chunks, s3_path_format=args.key)
        tasks.append(asyncio.ensure_future(volumes.wait_for_chunks(chunk_queue, vol_queue,
                                                                   args.timeout)))
        queues = [chunk_queue]

        # Add task to periodically dump internal usage stats
        tasks.append(asyncio.ensure_future(log_counts(loop, volumes)))

        # If we need to save the chunks to s3, set that up as well
        if args.save_chunks:
            import boto3
            s3_queue = asyncio.Queue()
            bucket_pool = S3BucketPool(args.save_chunks)
            sns_pool = SNSBucketPool(args.sns)
            tasks.append(asyncio.ensure_future(write_chunks_s3(loop, s3_queue, bucket_pool,
                                                               sns_pool)))
            queues.append(s3_queue)

        # Add callback for stdin and start loop
        loop.run_until_complete(read_stream(loop, read_in, volumes, queues, tasks))
        loop.close()
    except Exception as e:
        logger.exception('Exception raised!', exc_info=e)
