#!/usr/bin/env python
import asyncio
import functools
import glob
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


def init_logger():
    import faulthandler
    import logging.handlers
    import socket

    # Set up some kind of logging for crashes
    os.makedirs('logs', exist_ok=True)
    faulthandler.enable(open('logs/l2assemble-crash.log', 'a'))

    # Set the global logger
    global logger
    logger = logging.getLogger('Level2Handler')

    # Send logs to LDM's log if possible, otherwise send to stderr.
    try:
        handler = logging.handlers.SysLogHandler(address='/dev/log', facility='local0')
    except (FileNotFoundError, socket.error):
        handler = logging.StreamHandler()

    fmt = '%(filename)s [%(funcName)s]: [%(site)s %(volume_id)03d] %(message)s'
    handler.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(handler)
    logger = ProdInfoAdapter(logger, {'site': '----', 'volume_id': 0})


def log_rmtree_error(func, path, exc):
    logger.error('Error removing (%s %s)', func, path)


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
    def __init__(self, cache_dir, gen_header, s3=None, s3_path_format=''):
        super(defaultdict, self).__init__()
        self._cache_dir = cache_dir
        self._gen_header = gen_header
        self._s3_buckets = S3BucketPool(s3) if s3 else None
        self._s3_path = s3_path_format

    def __missing__(self, key):
        new = self._create_store(key)
        self[key] = new
        return new

    def _create_store(self, prod_info):
        logger.debug('Creating store.', extra=prod_info)

        # Check to see if we have previously written part to disk:
        if self._s3_buckets and prod_info.chunk_id > 1:
            store = ChunkStore.loadfroms3(self._s3_buckets, self._s3_path.format(prod_info),
                                          prod_info)
        else:
            cache = self.cache_dir(prod_info.to_key())
            if os.path.exists(cache):
                logger.debug('Loading previously stored chunks from: %s', cache,
                             extra=prod_info)
                store = ChunkStore.loadfromdir(cache)
                shutil.rmtree(cache, onerror=log_rmtree_error)
            else:
                store = ChunkStore()

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
        logger.debug('Flushing volumes queue')
        await self.vol_dest.join()

    async def wait_for_chunks(self, src, vol_dest, timeout):
        self.vol_dest = vol_dest
        self.timeout = timeout
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

    @classmethod
    def loadfromdir(cls, path):
        # Go find all the appropriately named files in the directory and load them
        cs = cls()
        for fname in sorted(glob.glob(os.path.join(path, 'L2-BZIP2_*')),
                            key=lambda f: ProdInfo.fromstring(os.path.basename(f)).chunk_id):
            name = os.path.basename(fname)
            cs.add(Chunk(prod_info=ProdInfo.fromstring(name), data=open(fname, 'rb').read()))
        logger.warning('Loaded %d chunks from cache %s', len(cs), path)
        return cs

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

    @classmethod
    def loadfroms3(cls, bucket_pool, key, prod_info):
        with bucket_pool.use() as bucket:
            cs = cls()
            prefix = '-'.join(key.split('-')[:-2])
            for obj in bucket.objects.filter(Prefix=prefix):
                name = os.path.basename(obj.key)
                date, time, chunk, chunk_type = name.split('-')
                pi = prod_info._replace(chunk_id=int(chunk), chunk_type=chunk_type)
                cs.add(Chunk(prod_info=pi, data=obj.get()['Body'].read()))
        logger.warning('Loaded %d chunks from S3 cache %s', len(cs), prefix, extra=prod_info)
        return cs

    def __len__(self):
        return len(self._store)

    def min_id(self):
        return min(self._store.keys()) if self._store else 0

    def max_id(self):
        return max(self._store.keys()) if self._store else 0

    def first_chunk(self):
        return list(self._store.values())[0]

    # Iterate in the order of the keys, but only return the value
    def __iter__(self):
        return iter(i[1] for i in sorted(self._store.items()))

    async def finish(self):
        await self._queue.join()

    async def enqueue(self, chunk):
        await self._queue.put(chunk)

    async def wait_for_chunks(self, timeout, when_done):
        need_more = True
        while need_more:
            try:
                chunk = await asyncio.wait_for(self._queue.get(), timeout)
                need_more = self.add(chunk)
                self._queue.task_done()
            except asyncio.TimeoutError:
                logger.warning('Finishing due to timeout.', extra=chunk.prod_info)
                need_more = False

        when_done()

    # Add a chunk to our store. If this was the start or end, note that as well.
    def add(self, chunk):
        max_id = self.max_id()
        chunk_id = chunk.prod_info.chunk_id
        if chunk_id != max_id + 1:
            if chunk_id in self._store:
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

        # Return whether we need more
        return len(self) != self.last

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
    def __init__(self, bucket_pool, path, name, fallback_num):
        from io import BytesIO

        logger.debug('Writing to S3 bucket: %s', bucket_pool.bucket_name)
        self.filename = name
        self._bucket_pool = bucket_pool
        self._key = path + '/' + name
        self._fobj = BytesIO()
        self._fallback_num = fallback_num

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
    def put_checked(bucket, key, data):
        import botocore.exceptions
        import hashlib
        import base64
        try:

            # Calculate MD5 checksum for integrity
            digest = base64.b64encode(hashlib.md5(data).digest()).decode('ascii')

            # Write to S3
            logger.debug('Uploading to S3 under key: %s (md5: %s)', key, digest)
            bucket.put_object(Key=key, Body=data, ContentMD5=digest)
        except botocore.exceptions.ClientError as e:
            logger.error(str(e))
            raise IOError from e

    def close(self):
        data = self._fobj.getvalue()

        # Get the object and try to make sure it doesn't exist
        with self._bucket_pool.use() as bucket:
            obj = bucket.Object(self._key)
            if self._exists(obj):
                obj = bucket.Object(self.fallback(self._key, self._fallback_num))

            # Upload to S3
            self.put_checked(bucket, obj.key, data)
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


async def write_chunks_s3(loop, queue, bucket_pool):
    while True:
        chunk = await queue.get()
        try:
            key = args.key.format(chunk.prod_info)
            logger.debug('Writing chunk to %s on S3 %s', key, bucket_pool.bucket_name,
                         extra=chunk.prod_info)
            fut = loop.run_in_executor(None, upload_chunk_s3, bucket_pool, key, chunk)
            fut.add_done_callback(functools.partial(when_item_done, loop, queue, key, chunk))
        except Exception:
            logger.exception('write_chunks_s3 exception:', exc_info=sys.exc_info())


def upload_chunk_s3(pool, key, chunk):
    with pool.use() as bucket:
        S3File.put_checked(bucket, key, chunk.data)


async def save_volume(loop, queue, File, base, fmt, statsfile):
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
            file = File(base, path, fname, chunks.min_id())
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


#
# Pool for S3 access objects
#
class S3BucketPool(object):
    def __init__(self, bucket):
        import queue
        self._queue = queue.Queue()
        self._create_lock = threading.Lock()
        self.bucket_name = bucket

    def borrow(self):
        if self._queue.empty():
            with self._create_lock:
                import boto3
                return boto3.session.Session().resource('s3').Bucket(self.bucket_name)

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
    from concurrent.futures import ThreadPoolExecutor

    init_logger()
    args = setup_arg_parser().parse_args()

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    # Read directly from standard in buffer
    read_in = sys.stdin.buffer

    # Set up event loop
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(args.threads))

    # Setup queue for saving volumes
    vol_queue = asyncio.Queue()
    FileClass, base = (S3File, S3BucketPool(args.s3)) if args.s3 else (DiskFile, args.data_dir)
    tasks = [asyncio.ensure_future(save_volume(loop, vol_queue, FileClass, base, args.format,
                                               args.stats))]

    # Set up storing chunks internally
    chunk_queue = asyncio.Queue()
    volumes = VolumeStore(cache_dir=args.data_dir, gen_header=args.generate_header,
                          s3=args.save_chunks, s3_path_format=args.key)
    tasks.append(asyncio.ensure_future(volumes.wait_for_chunks(chunk_queue, vol_queue,
                                                               args.timeout)))
    queues = [chunk_queue]

    # If we need to save the chunks to s3, set that up as well
    if args.save_chunks:
        s3_queue = asyncio.Queue()
        bucket_pool = S3BucketPool(args.save_chunks)
        tasks.append(asyncio.ensure_future(write_chunks_s3(loop, s3_queue, bucket_pool)))
        queues.append(s3_queue)

    # Add callback for stdin and start loop
    loop.run_until_complete(read_stream(loop, read_in, volumes, queues, tasks))
    loop.close()
