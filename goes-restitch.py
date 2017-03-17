#!/usr/bin/env python
# Copyright (c) 2017 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

from contextlib import closing
from datetime import datetime
import logging
import os
import os.path

from netCDF4 import Dataset
import numpy as np

logger = logging.getLogger('LDM')
logger.addHandler(logging.StreamHandler())


def copy_attrs(src, dest, skip):
    r"""Copy all netCDF attributes from one object to another."""
    for attr in src.ncattrs():
        if not skip(attr):
            setattr(dest, attr, getattr(src, attr))


def dataset_name(dataset, template=os.path.join('{satellite}', '{dt:%Y%m%d}', '{scene}',
                                                '{satellite}_{channel:.2f}_{resolution}_'
                                                '{lat}_{lon}_{dt:%Y%m%d}_{dt:%H%M%S}.nc4')):
    r"""Create an appropriate file path from a GOES dataset."""
    sat_id = dataset.satellite_id.replace('-', '')
    channel = dataset.central_wavelength

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
                           scene=scene, lat=center_lat, lon=center_lon)


def find_files(source_dir):
    r"""Find all the netCDF4 files in a directory tree."""
    for root, dirs, files in os.walk(source_dir):
        for fname in files:
            if not fname.endswith('nc4'):
                continue
            yield Dataset(os.path.join(root, fname))


def output_file(source_nc, out_dir=''):
    r"""Initialize an output netCDF4 file from the input tile file."""
    # Determine output file path and create any necessary directories
    out_filename = os.path.join(out_dir, dataset_name(source_nc))
    if os.path.sep in out_filename:
        path = os.path.dirname(out_filename)
        os.makedirs(path, exist_ok=True)

    # Open file if it exists, otherwise create it
    if os.path.exists(out_filename):
        output_nc = Dataset(out_filename, 'a')
    else:
        output_nc = Dataset(out_filename, 'w')

        # Copy global attributes, create dimensions, add our metadata
        copy_attrs(source_nc, output_nc, lambda a: 'tile' in a)
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


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Assemble netCDF4 tiles of GOES data into single'
                                        ' files.')
    parser.add_argument('-s', '--source', help='Source directory for data tiles', type=str)
    parser.add_argument('-d', '--dest', help='Destination directory for data', type=str)
    parser.add_argument('-v', '--verbose', help='Make output more verbose', action='count',
                        default=0)
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Get a tile file, get the output file, and do the copy
    for src in find_files(args.source):
        with closing(output_file(src, args.dest)) as dest:
            logger.debug('Copying %s to %s', src.filepath(), dest.filepath())
            copy_tile(src, dest)
