#!/usr/bin/env python
# Copyright (c) 2017 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import asyncio
from collections import defaultdict
from datetime import datetime
import functools
import logging
import os
import os.path
import sys
import tempfile

from netCDF4 import Dataset
import numpy as np

from ldm import init_logger, read_stream, remove_footer, remove_header


def copy_attrs(src, dest, skip):
    r"""Copy all netCDF attributes from one object to another."""
    for attr in src.ncattrs():
        if not skip(attr):
            setattr(dest, attr, getattr(src, attr))


def dataset_name(dataset, template):
    r"""Create an appropriate file path from a GOES dataset."""
    sat_id = dataset.satellite_id.replace('-', '')
    channel = dataset.central_wavelength
    channel_id = dataset.channel_id

    # Get a resolution string like 500m or 1km
    if dataset.source_spatial_resolution >= 1.0:
        resolution = '{}km'.format(int(np.round(dataset.source_spatial_resolution)))
    else:
        resolution = '{}m'.format(int(np.round(dataset.source_spatial_resolution * 1000.)))

    # Get lon/lat out to 1 decimal point
    center_lat = '{0:.1f}{1}'.format(np.fabs(dataset.product_center_latitude),
                                     'N' if dataset.product_center_latitude > 0 else 'S')
    center_lon = '{0:.1f}{1}'.format(np.fabs(dataset.product_center_longitude),
                                     'E' if dataset.product_center_longitude > 0 else 'W')
    scene = dataset.source_scene

    # Parse start time into something we can use
    dt = datetime.strptime(dataset.start_date_time, '%Y%j%H%M%S')
    return template.format(satellite=sat_id, channel=channel, resolution=resolution, dt=dt,
                           scene=scene, lat=center_lat, lon=center_lon, channel_id=channel_id)


def init_nc_file(source_nc, output_nc):
    r"""Initialize an output netCDF4 file from the input tile file."""
    # Copy global attributes, create dimensions, add our metadata
    copy_attrs(source_nc, output_nc, lambda a: a.startswith('tile'))
    output_nc.product_tiles_received = 0
    output_nc.created_by = 'ldm-alchemy'
    output_nc.createDimension('y', source_nc.product_rows)
    output_nc.createDimension('x', source_nc.product_columns)

    # Copy all the variables
    for var_name, old_var in source_nc.variables.items():
        extra_args = {}

        # Need special handling for fill value, since that needs to be on the variable
        # constructor
        if hasattr(old_var, '_FillValue'):
            extra_args['fill_value'] = old_var._FillValue

        # Enable compression for 2D variables only, not coordinates
        if len(old_var.dimensions) == 2:
            extra_args['zlib'] = True
            extra_args['complevel'] = 4
            extra_args['shuffle'] = True

            # Default chunk size chosen by library has 50% file size penalty!
            chunk_height = min(source_nc.product_tile_height, source_nc.product_rows)
            chunk_width = min(source_nc.product_tile_width, source_nc.product_columns)
            extra_args['chunksizes'] = (chunk_height, chunk_width)

        # Create the variable and copy its attributes
        var = output_nc.createVariable(var_name, old_var.datatype,
                                       old_var.dimensions, **extra_args)
        copy_attrs(old_var, var, lambda a: '_FillValue' in a)

    return output_nc


def copy_tile(input_ds, output_ds):
    r"""Copy tile data from input to output."""
    # There's no need to do any scaling--just copy the integer data
    input_ds.set_auto_scale(False)
    output_ds.set_auto_scale(False)

    # Set up slices based on the column and row offsets
    col_slice = slice(input_ds.tile_column_offset,
                      input_ds.tile_column_offset + input_ds.product_tile_width)
    row_slice = slice(input_ds.tile_row_offset,
                      input_ds.tile_row_offset + input_ds.product_tile_height)

    # Copy out data for x, y, and data variables using appropriate slices
    for var_name, src_var in input_ds.variables.items():
        dest_var = output_ds.variables[var_name]
        if var_name == 'x':
            dest_var[col_slice] = src_var[:]
        elif var_name == 'y':
            dest_var[row_slice] = src_var[:]
        elif src_var.ndim == 2:
            dest_var[row_slice, col_slice] = src_var[:]

    output_ds.product_tiles_received += 1
    output_ds.sync()


def find_files(source_dir):
    r"""Find all the netCDF4 files in a directory tree."""
    for root, dirs, files in os.walk(source_dir):
        for fname in sorted(files):
            if not fname.endswith('nc4'):
                continue
            ds = Dataset(os.path.join(root, fname))
            yield ds


async def read_disk(source_dir, sinks):
    r"""Read files from disk and asynchronously put them in queue.
    
    Integrates find_files into our asynchronous framework.
    """
    for product in find_files(source_dir):
        for sink in sinks:
            logger.debug('Queued product: %s', product.filepath())
            await sink.put(product)
            # Without this, we just load files until we run out of file handles
            await asyncio.sleep(0.01)

    for sink in sinks:
        logger.debug('Flushing product sinks.')
        await sink.join()
    await asyncio.sleep(0.01)  # Just enough to let other things close out
    logger.debug('All done.')


#
# Caching and storage of tiles
#
class AssemblerManager(defaultdict):
    r"""Manages Assembler instances.
    
    Dispatches tiles that have arrived and dispatches them to the appropriate
    file assembler, creating them as necessary.
    """
    def __init__(self, out_dir, timeout, filename_template, loop):
        super().__init__()
        self.out_dir = out_dir
        self.timeout = timeout
        self.loop = loop
        self.filename = filename_template

    def __missing__(self, key):
        new = self._create_store(key)
        self[key] = new
        return new

    def _create_store(self, key):
        # Pass in call-back to call when done. We don't use the standard future callback
        # because it will end up queued--we need to run immediately.
        store = Assembler(self.out_dir, self.timeout, self.filename)
        store.task = asyncio.ensure_future(
            store.process_items(functools.partial(self.store_done, key=key)))
        return store

    async def join(self):
        # Need to iterate over copy of keys because items could be removed during iteration
        for key in list(self.keys()):
            logger.debug('Flushing chunk store queue.')
            store = self[key]
            await store.finish()
            store.task.cancel()

    async def put(self, item):
        # Find the appropriate store (will be created if necessary)
        if not isinstance(item, Dataset):
            prod_id, data = item
            item = read_netcdf_from_memory(data)
            logger.debug('Dispatching item: %s', prod_id)
        else:
            logger.debug('Dispatching item: %s', item.filepath())
        await self[dataset_name(item, self.filename)].enqueue(item)

    def store_done(self, key):
        logger.info('%s finished.', key)
        self.pop(key)


class Assembler:
    r"""Handles writing tiles to the final netCDF file."""
    def __init__(self, out_dir, timeout, filename_template):
        self._queue = asyncio.Queue(15)
        self.out_dir = out_dir
        self.timeout = timeout
        self.output = None
        self.output_name = ''
        self.template = filename_template

    async def enqueue(self, item):
        await self._queue.put(item)

    async def finish(self):
        await self._queue.join()
        self.finalize()

    def finalize(self):
        if self.output is not None:
            try:
                logger.debug('Closing file.')
                old_name = self.output.filepath()
                self.output.close()

                # Rename temporary file to final name if necessary
                if old_name != self.output_name:
                    logger.debug('Renaming output %s to %s', old_name, self.output_name)
                    os.rename(old_name, self.output_name)
            except Exception:
                logger.exception('Exception while finalizing file: ', exc_info=sys.exc_info())
            finally:
                self.output = None

    async def process_items(self, on_done):
        while True:
            try:
                product = await asyncio.wait_for(self._queue.get(), self.timeout)
                logger.debug('Processing product: %s', product.filepath())
                try:
                    if not self.output:
                        self.output_name = os.path.join(self.out_dir,
                                                        dataset_name(product, self.template))

                        # Open file if it exists, otherwise create it
                        if os.path.exists(self.output_name):
                            logger.info('Using existing file: %s', self.output_name)
                            self.output = Dataset(self.output_name, 'a')
                        else:
                            if os.path.sep in self.output_name:
                                os.makedirs(os.path.dirname(self.output_name), exist_ok=True)
                            temp_name = self.output_name + '.partial'
                            logger.debug('Creating temporary file: %s', temp_name)
                            self.output = Dataset(temp_name, 'w')

                            init_nc_file(product, self.output)

                    # Copy the tile
                    copy_tile(product, self.output)

                    # Break if done
                    self._queue.task_done()
                    if (self.output.product_tiles_received >= self.output.number_product_tiles
                            and self._queue.empty()):
                        logger.info('%s: All tiles received.',
                                    os.path.basename(self.output_name))
                        break
                    else:
                        logger.info('%s: %d of %d tiles received.',
                                    os.path.basename(self.output_name),
                                    self.output.product_tiles_received,
                                    self.output.number_product_tiles)

                except Exception:
                    logger.exception('Save data exception:', exc_info=sys.exc_info())

            # In the event of timeout, bail out.
            except asyncio.TimeoutError:
                logger.warning('Finishing due to timeout.')
                break

        self.finalize()
        on_done()


def read_netcdf_from_memory(mem):
    r"""Return a netCDF4.Dataset from data in memory.
    
    Uses a temp file until we have support in netCDF4-python.
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp.write(remove_footer(remove_header(mem)))
        return Dataset(temp.name, 'r')
    finally:
        os.remove(temp.name)


#
# Argument parsing
#
def setup_arg_parser():
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Assemble netCDF4 tiles of GOES data into '
                                                 'single files.')
    parser.add_argument('-d', '--data_dir', help='Base output directory', type=str,
                        default='/data/ldm/pub/native/radar/level2')
    parser.add_argument('-t', '--timeout', help='Timeout in seconds for waiting for data',
                        default=15, type=int)
    parser.add_argument('-s', '--source', help='Source directory for data tiles', type=str,
                        default='')
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-f', '--filename', help='Filename format string. Uses Python '
                        'string format specification',
                        default=os.path.join('{satellite}', '{scene}',
                                             'Channel{channel_id:02d}', '{dt:%Y%m%d}',
                                             '{satellite}_{dt:%Y%m%d}_{dt:%H%M%S}_'
                                             '{channel:.2f}_{resolution}_{lat}_{lon}.nc4'))
    parser.add_argument('-l', '--log', help='Filename to log information to. Uses standard'
                        ' out if not given.', type=str, default='')
    parser.add_argument('other', help='Other arguments for LDM identification', type=str,
                        nargs='*')
    return parser


if __name__ == '__main__':
    args = setup_arg_parser().parse_args()

    fmt = '[' + ' '.join(args.other) + '] %(asctime)s [%(funcName)s]: %(message)s'
    if os.path.sep in args.log:
        os.makedirs(os.path.dirname(args.log), exist_ok=True)

    logger = init_logger(logging.Formatter(fmt=fmt),
                         stream=open(args.log, 'at') if args.log else None)

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    # Set up event loop
    loop = asyncio.get_event_loop()
    manager = AssemblerManager(args.data_dir, args.timeout, args.filename, loop)
    queues = [manager]

    # Read from disk if we're pointed to a file, otherwise read from stdin pipe
    try:
        if args.source:
            loop.run_until_complete(read_disk(args.source, queues))
        else:
            # Read directly from standard in buffer
            read_in = sys.stdin.buffer
            loop.run_until_complete(read_stream(loop, read_in, queues, timeout=args.timeout))
    except Exception:
        logger.exception('Exception raised:', exc_info=sys.exc_info())
    finally:
        loop.close()
