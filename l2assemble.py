#!/usr/bin/env python
import bz2
import glob
import gzip
import logging
import logging.handlers
import os
import os.path
import shutil
import struct
import sys
import traceback

from collections import namedtuple
from datetime import datetime, timedelta


# Set up logging
def init_logger(site, volnum):
    # Set the global logger
    global logger
    logger = logging.getLogger('Level2Handler')
    logger.setLevel(logging.DEBUG)

    # Set up a system exception hook for logging exceptions
    def log_uncaught_exceptions(ex_cls, ex, tb):
        logger.critical(''.join(traceback.format_tb(tb)))
        logger.critical('{0}: {1}'.format(ex_cls, ex))
    sys.excepthook = log_uncaught_exceptions

    # Send logs to LDM's log if possible, otherwise send to stderr.
    try:
        handler = logging.handlers.SysLogHandler(address='/dev/log', facility='local0')
    except FileNotFoundError:
        handler = logging.StreamHandler()

    fmt = '%(filename)s [%(process)d]: ' + '[%s %03d]' % (site, volnum) + ' %(message)s'
    handler.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(handler)


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
    return os.path.join(base_dir, site, '.%03d' % vol_num)


def clear_old_caches(base_dir, site, cur_vol):
    logger.debug('Checking for old caches...')
    # List all old cache directories for this site
    for fname in glob.glob(os.path.join(base_dir, site, '.[0-9][0-9][0-9]')):
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
        chunk_ids = self._store.keys()

        # Create the directory if necessary
        if not os.path.exists(path):
            os.makedirs(path)

        # Handle first and last separately
        if self.first != -1:
            chunk_ids.remove(self.first)
            with open(os.path.join(path, '%03dS' % self.first), 'wb') as outf:
                outf.write(self.vol_hdr)
                outf.write(self._store[self.first])

        if self.last != -1:
            chunk_ids.remove(self.last)
            with open(os.path.join(path, '%03dE' % self.last), 'wb') as outf:
                outf.write(self._store[self.last])

        # Write the rest
        for chunk_id in chunk_ids:
            with open(os.path.join(path, '%03dI' % chunk_id), 'wb') as outf:
                outf.write(self._store[chunk_id])

    def __len__(self):
        return len(self._store)

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


def bz2_decomp(chunk):
    return bz2.decompress(chunk[4:])


# Write the file in the specified format. For gz/bz2 we decompress the blocks, then
# compress the file. For raw, no decompression, write block as is.
data_handlers = dict(gz=(gzip.open, bz2_decomp, '.gz'), bz2=(bz2.BZ2File, bz2_decomp, '.bz2'),
                     raw=(open, lambda c: c, ''))
def write_file(fname, chunks, format):
    writer, decomp, ext = data_handlers[format]
    outname = fname + ext
    logger.debug('Writing %s format to %s', format, outname)

    # Check to make sure the file doesn't already exist
    if os.path.exists(outname):
        newname = fname + '.%03d' % chunks.max_id() + ext
        logger.error('%s already exists!. Falling back to %s.', outname, newname)
        outname = newname

    # Write it out
    with writer(outname, 'wb') as outf:
        # Write the volume header if we have one
        if chunks.vol_hdr:
            outf.write(chunks.vol_hdr)
        else:
            logger.error('Missing volume header for: %s', fname)

        # Write the data chunks
        for num, chunk in enumerate(chunks):
            try:
                # Need to skip first 4 bytes which contain the size of the block
                outf.write(decomp(chunk))
            except (OSError, IOError):
                logger.error('Error decompressing chunk: %d', num)


def setup_arg_parser():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Read NEXRAD Level2 LDM compressed blocks'
                                     ' and assemble when they are done arriving.')
    parser.add_argument('-d', '--data_dir', help='Base output directory', type=str,
                        default='/data/ldm/pub/native/radar/level2')
    parser.add_argument('-f', '--format', help='Format for output', type=str,
                        choices=('raw', 'bz2', 'gz'), default='raw')
    parser.add_argument('-g', '--generate_header', help='Generate volume header if missing',
                        action='store_true')
    parser.add_argument('-t', '--timeout', help='Timeout in seconds for waiting for data',
                        default=300, type=int)
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-p', '--path', help='Path format =string. Uses Python '
                        'string format specification', default='{0.site}/{0.dt:%Y%m%d}')
    parser.add_argument('-n', '--filename', help='Filename format string. Uses Python '
                        'string format specification',
                        default='Level2_{0.site}_{0.dt:%Y%m%d}_{0.dt:%H%M%S}.ar2v')
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
    logger.setLevel(30 + total_level * 10) # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    # Reading from standard in. Set up a poll so we can timeout waiting for data
    read_in = os.fdopen(sys.stdin.fileno(), 'rb')
    poll_in = select.poll()
    poll_in.register(read_in, select.POLLIN)

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

    need_more = True
    prod_info = None

    while need_more:
        # Time out after 5 minutes
        info = poll_in.poll(args.timeout * 1000)

        # Kick out if we time out
        if not info:
            logger.warn('Finishing due to time out.')
            break

        # Read metadata from LDM for prod id and product size, then read in the appropriate
        # amount of data.
        logger.debug('Reading....')
        try:
            prod_id, prod_length = read_metadata(read_in)
            prod_info = parse_prod_info(prod_id)
            logger.debug('Handling chunk {0.chunk_id} ({0.chunk_type}) for {0.site} '
                        '{0.volume_id} {0.dt}'.format(prod_info))
            data = check_read(read_in, prod_length)
            logger.debug('Read chunk.')
        except EOFError:
            # If we get an EOF, save our chunks to disk for reloading later
            cache = cache_dir(args.data_dir, args.site, args.volume_number)
            logger.warn('Finishing due to EOF. Saving to: %s', cache)
            chunks.savetodir(cache)
            break

        # Add the chunk, let it control whether we continue
        need_more = chunks.add(prod_info.chunk_id, prod_info.chunk_type, data)

    # Any time we kick out, write the data if we have some
    if chunks and prod_info:
        # Determine file name
        fname = args.filename.format(prod_info)
        logger.info('File: %s (S:%d E:%d N:%d M:[%s])', fname, chunks.first,
                    chunks.last, len(chunks), ' '.join(chunks.missing()))

        # Create the output dir if necessary
        subdir = args.path.format(prod_info)
        out_dir = os.path.join(args.data_dir, subdir)
        if not os.path.exists(out_dir):
            logger.debug('Creating dir: %s', out_dir)
            os.makedirs(out_dir)

        # Add header if necessary
        if args.generate_header:
            chunks.add_header(prod_info)

        write_file(os.path.join(out_dir, fname), chunks, args.format)
    else:
        logger.error('Exiting without doing anything!')
