#!/usr/bin/env python
import bz2
import glob
import gzip
import logging
import logging.handlers
import os
import os.path
import shutil
import socket
import struct
import sys
import traceback

from collections import namedtuple
from contextlib import closing
from datetime import datetime


# Fix some name problems
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = Exception


# Set up logging
def init_logger(site, volnum):
    # Set the global logger
    global logger
    logger = logging.getLogger('Level2Handler')
    logger.setLevel(logging.DEBUG)

    # Send logs to LDM's log if possible, otherwise send to stderr.
    try:
        handler = logging.handlers.SysLogHandler(address='/dev/log', facility='local0')
    except (FileNotFoundError, socket.error):
        handler = logging.StreamHandler()

    fmt = '%(filename)s [%(process)d]: ' + '[%s %03d]' % (site, volnum) + ' %(message)s'
    handler.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(handler)

    # Set up a system exception hook for logging exceptions
    def log_uncaught_exceptions(ex_cls, ex, tb):
        global logger
        logger.critical(''.join(traceback.format_tb(tb)))
        logger.critical('{0}: {1}'.format(ex_cls, ex))
    sys.excepthook = log_uncaught_exceptions


def log_rmtree_error(func, path, exc):
    logger.error('Error removing (%s %s)', func, path)


# Raises an EOFError if we get a 0 byte read, which is by definition an EOF in Python
def check_read(fobj, num_bytes):
    data = fobj.read(num_bytes)
    if data:
        return data
    raise EOFError('Got 0 byte read.')


len_struct = struct.Struct('I')
def read_byte_string(fobj):
    slen, = len_struct.unpack(check_read(fobj, len_struct.size))
    return check_read(fobj, slen)


# Stuff for parsing LDM metadata
meta_struct = struct.Struct('6IQiII')
ldm_meta = namedtuple('LDMMeta', 'meta_length md5_1 md5_2 md5_3 md5_4 prod_len creation_secs '
                                 'creation_micro feed_type seq_num')
def read_metadata(fobj):
    meta = ldm_meta(*meta_struct.unpack(check_read(fobj, meta_struct.size)))
    logger.debug('LDM metadata: %s', meta)
    prod_ident = read_byte_string(fobj)
    logger.debug('Got prod_id: %s', prod_ident)
    read_byte_string(fobj)  # prod_origin
    return prod_ident, meta.prod_len


# Turn the string 'L2-BZIP2/KFTG/20150908215946/494/43/I/V06/0' into useful information
ProdInfo = namedtuple('ProdInfo',
                      'format site dt volume_id chunk_id chunk_type version unused')
def parse_prod_info(s):
    pi = ProdInfo(*s.split('/'))
    return pi._replace(dt=datetime.strptime(pi.dt, '%Y%m%d%H%M%S'),
                       chunk_id=int(pi.chunk_id))


def cache_dir(base_dir, site, vol_num):
    return os.path.join(base_dir, '.' + site, '.%03d' % vol_num)


def clear_old_caches(base_dir, site, cur_vol):
    logger.debug('Checking for old caches...')
    # List all old cache directories for this site
    for fname in glob.glob(os.path.join(base_dir, '.' + site, '.[0-9][0-9][0-9]')):
        if os.path.isdir(fname):  # Minor sanity check that this is ours
            logger.debug('Found: %s', fname)

            # Use this volume number as a proxy for time
            num = int(os.path.basename(fname)[1:])

            # Find the difference, account for the wrap 999->0
            diff = cur_vol - num
            if diff < 0:
                diff += 1000

            # If the one we found is more than 30 past, delete it
            if diff > 30:
                logger.info('Deleting old cache: %s', fname)
                shutil.rmtree(fname, onerror=log_rmtree_error)


hdr_struct = struct.Struct('>12s2L4s')
class ChunkStore(object):
    def __init__(self):
        self._store = dict()
        self.first = self.last = -1
        self.vol_hdr = None

    @classmethod
    def loadfromdir(cls, path):
        # Go find all the appropriately named files in the directory and load them
        cs = ChunkStore()
        for fname in sorted(glob.glob(os.path.join(path, '[0-9][0-9][0-9][EIS]'))):
            name = os.path.basename(fname)
            chunk_id = int(name[:3])
            chunk_type = name[-1]
            cs.add(chunk_id, chunk_type, open(fname, 'rb').read())
        logger.warn('Loaded %d chunks from cache %s', len(cs), path)
        return cs

    def savetodir(self, path):
        # Get all the chunks
        chunk_ids = list(self._store.keys())

        # Create the directory if necessary
        if not os.path.exists(path):
            os.makedirs(path)

        # Handle first and last separately
        if self.first != -1:
            logger.warn('Saving first chunk: %d', self.first)
            chunk_ids.remove(self.first)
            with open(os.path.join(path, '%03dS' % self.first), 'wb') as outf:
                outf.write(self.vol_hdr)
                outf.write(self._store[self.first])

        if self.last != -1:
            logger.warn('Saving last chunk: %d', self.last)
            chunk_ids.remove(self.last)
            with open(os.path.join(path, '%03dE' % self.last), 'wb') as outf:
                outf.write(self._store[self.last])

        # Write the rest
        logger.warn('Saving remaining %d (of %d) chunks: [%s]', len(chunk_ids),
                    len(self._store), ' '.join(map(str, chunk_ids)))
        for chunk_id in chunk_ids:
            with open(os.path.join(path, '%03dI' % chunk_id), 'wb') as outf:
                outf.write(self._store[chunk_id])

    def __len__(self):
        return len(self._store)

    def min_id(self):
        return min(self._store.keys()) if self._store else 0

    def max_id(self):
        return max(self._store.keys()) if self._store else 0

    # Iterate in the order of the keys, but only return the value
    def __iter__(self):
        return iter(i[1] for i in sorted(self._store.items()))

    # Add a chunk to our store. If this was the start or end, note that as well.
    def add(self, chunk_id, chunk_type, chunk):
        max_id = self.max_id()
        if chunk_id != max_id + 1:
            if chunk_id in self._store:
                logger.warn('Duplicate chunk: %d', chunk_id)
            else:
                logger.warn('Chunks out of order--Got: %d Max: %d', chunk_id, max_id)

        # Not only do we need to note the first block, we need to pop off the header
        # bytes
        if chunk_type == 'S':
            self.first = chunk_id
            self.vol_hdr = chunk[:hdr_struct.size]
            chunk = chunk[hdr_struct.size:]
        elif chunk_type == 'E':
            self.last = chunk_id

        self._store[chunk_id] = chunk

        # Return whether we need more chunks
        return len(self) != self.last

    # Reconstruct a level 2 volume header if we miss the first block
    def add_header(self, info):
        if not self.vol_hdr:
            version = b'AR2V00' + info.version[1:] + b'.' + info.volume_id
            timestamp = (info.dt - datetime(1970, 1, 1)).total_seconds()
            date = int(timestamp // 86400)
            time = int(timestamp - date * 86400)
            logger.warn('Creating volume header for first chunk: %s %d %d %s', version,
                        date + 1, time * 1000, info.site)
            self.vol_hdr = hdr_struct.pack(version, date + 1, time * 1000, info.site)

    # List any blocks we missed
    def missing(self):
        return map(str, set(range(1, self.last + 1)) - set(self._store.keys()))


class ChunkWriter(object):
    def __init__(self, fobj, fmt):
        self.fobj = fobj
        if fmt == 'raw':
            self._process_chunk = lambda chunk: chunk
        else:
            if fmt == 'gz':
                self.fobj = gzip.GzipFile(fileobj=fobj, mode='wb')
            elif fmt == 'bz2':
                self.fobj = bz2.BZ2File(fobj, mode='wb')
            self._process_chunk = lambda chunk: bz2.decompress(chunk[4:])

    def write(self, data):
        self.fobj.write(data)

    def write_chunk(self, chunk):
        self.write(self._process_chunk(chunk))


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

        self._fobj = open(outname, 'wb')

    @staticmethod
    def fallback(outname, fallback_num):
        newname = outname + '.%03d' % fallback_num
        logger.error('%s already exists!. Falling back to %s.', outname, newname)
        return newname

    def writable(self):
        return True

    def write(self, data):
        return self._fobj.write(data)

    def close(self):
        self._fobj.close()


class S3File(DiskFile):
    def __init__(self, bucket_name, path, name, fallback_num):
        import boto3
        from io import BytesIO

        logger.debug('Writing to S3 bucket: %s', bucket_name)
        s3 = boto3.resource('s3')
        self._bucket = s3.Bucket(bucket_name)
        self._key = path + '/' + name
        self._fobj = BytesIO()
        self._fallback_num = fallback_num

    def _exists(self, obj):
        import botocore
        try:
            obj.version_id
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
        return True

    @staticmethod
    def put_checked(bucket, key, data):
        try:
            import botocore
            import hashlib

            # Calculate MD5 checksum for integrity
            digest = hashlib.md5(data).digest().encode('base64').rstrip()

            # Write to S3
            logger.debug('Uploading to S3 under key: %s (md5: %s)', key, digest)
            bucket.put_object(Key=key, Body=data, ContentMD5=digest)
        except botocore.exceptions.ClientError as e:
            logger.error(str(e))

    def close(self):
        data = self._fobj.getvalue()

        # Get the object and try to make sure it doesn't exist
        obj = self._bucket.Object(self._key)
        if self._exists(obj):
            obj = self._bucket.Object(self.fallback(self._key, self._fallback_num))

        # Upload to S3
        self.put_checked(self._bucket, obj.key, data)
        super(S3File, self).close()


def write_file(writer, chunks):
    # Write the volume header if we have one
    if chunks.vol_hdr:
        writer.write(chunks.vol_hdr)
    else:
        logger.error('Missing volume header for: %s', fname)

    # Write the data chunks
    for num, chunk in enumerate(chunks):
        try:
            writer.write_chunk(chunk)
        except (OSError, IOError):
            logger.error('Error writing chunk: %d', num)


def read_chunk(stream):
    # Read metadata from LDM for prod id and product size, then read in the appropriate
    # amount of data.
    prod_id, prod_length = read_metadata(stream)
    prod_info = parse_prod_info(prod_id)
    logger.debug('Handling chunk {0.chunk_id} ({0.chunk_type}) for {0.site} '
                 '{0.volume_id} {0.dt}'.format(prod_info))
    data = check_read(stream, prod_length)
    logger.debug('Read chunk.')
    return prod_info, data


def timed_poll(poll, time_sec):
    # Time out after given time
    info = poll.poll(time_sec * 1000)

    # Return whether data are ready
    if not info:
        logger.warn('Finishing due to time out.')
        return False

    return True


def setup_arg_parser():
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
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-p', '--path', help='Path format string. Uses Python '
                        'string format specification', default='{0.site}/{0.dt:%Y%m%d}')
    parser.add_argument('-n', '--filename', help='Filename format string. Uses Python '
                        'string format specification',
                        default='Level2_{0.site}_{0.dt:%Y%m%d_%H%M%S}.ar2v')
    parser.add_argument('site', help='Site ID for volume', type=str)
    parser.add_argument('volume_number', help='Volume number for file', type=int)
    return parser

if __name__ == '__main__':
    import argparse
    import select

    parser = setup_arg_parser()
    args = parser.parse_args()
    init_logger(args.site, args.volume_number)

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    # Reading from standard in. Need to re-open for binary access
    read_in = os.fdopen(sys.stdin.fileno(), 'rb')

    # Need to go ahead and read the chunk first to clear the pipe
    prod_info, deferred_chunk = read_chunk(read_in)
    logger.debug('Chunk on startup: %d', prod_info.chunk_id)

    # Set up a poll so we can timeout waiting for data
    poll_in = select.poll()
    poll_in.register(read_in, select.POLLIN | select.POLLHUP)

    # Check to see if we have previously written part to disk:
    cache = cache_dir(args.data_dir, args.site, args.volume_number)
    if os.path.exists(cache):
        logger.debug('Loading previously stored chunks from: %s', cache)
        chunks = ChunkStore.loadfromdir(cache)

        # Clear it out after we've finished
        shutil.rmtree(cache, onerror=log_rmtree_error)
    else:
        chunks = ChunkStore()

    # Remove any old cache directories
    clear_old_caches(args.data_dir, args.site, args.volume_number)

    # If we need to save the chunks, grab the bucket
    if args.save_chunks:
        import boto3
        s3 = boto3.resource('s3')
        save_bucket = s3.Bucket(args.save_chunks)

    # Main loop
    need_more = True
    need_save = True
    while need_more:
        # If we already have a chunk, use it, otherwise try to get another
        if deferred_chunk:
            data, deferred_chunk = deferred_chunk, None
        else:
            try:
                if not timed_poll(poll_in, args.timeout):
                    break
                logger.debug('Reading....')
                prod_info, data = read_chunk(read_in)
            except EOFError:
                # If we get an EOF, save our chunks to disk for reloading later
                cache = cache_dir(args.data_dir, args.site, args.volume_number)
                logger.warn('Finishing due to EOF. Saving to: %s', cache)
                chunks.savetodir(cache)
                need_save = False
                break

        # Store chunk if necessary
        if args.save_chunks:
            S3File.put_checked(save_bucket, args.key.format(prod_info), data)

        # Add the chunk, let it control whether we continue
        need_more = chunks.add(prod_info.chunk_id, prod_info.chunk_type, data)

    # When we kick out without needing more, write the data if we have some
    if chunks and prod_info:
        if need_save:
            # Determine file name
            fname = args.filename.format(prod_info)
            if args.format != 'raw':
                fname += '.' + args.format

            path = args.path.format(prod_info)
            logger.info('File: %s (S:%d E:%d N:%d M:[%s])', fname, chunks.first,
                        chunks.last, len(chunks), ' '.join(chunks.missing()))

            # Add header if necessary
            if args.generate_header:
                chunks.add_header(prod_info)

            # Set up end place file will be written to
            if args.s3:
                file = S3File(args.s3, path, fname, chunks.min_id())
            else:
                file = DiskFile(args.data_dir, path, fname, chunks.min_id())

            with closing(file):
                cw = ChunkWriter(file, args.format)
                write_file(cw, chunks)
    else:
        logger.error('Exiting without doing anything!')
