#!/usr/bin/env python
# Copyright (c) 2017 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import asyncio
from collections import defaultdict
from datetime import datetime
import faulthandler
import functools
import logging
import os
import os.path
import re
import sys
import tempfile
import gzip

from netCDF4 import Dataset, date2num
import numpy as np
from array import array

from ldm import init_logger, read_stream, remove_footer, remove_header


def goes_time_to_dt(s):
    # 20211207 - VLAB GLM time_coverage_start and time_coverage_end global
    #              attribute strings include a 'Z' to indicate the timezone;
    #              replace all 'Z's with spaces
    #            Example time: 2021-12-02T15:43:00Z
    return datetime.strptime(re.sub("Z", "", s), '%Y-%m-%dT%H:%M:%S')


def copy_attrs(src, dest, skip):
    r"""Copy all netCDF attributes from one object to another."""
    for attr in src.ncattrs():
        if not skip(attr):
            # 20201216 - CIRA 'start_date_time' global attribute strings can have spaces
            #              instead of zeroes instead of spaces as leading values of time
            #              values, replace all spaces with zeros.  Vlab GLM files may
            #              not have spaces, but leave the code in in case they ever do.
            # 20220210 - Change Tnn indicator in dataset_name to T99 for AWIPS use
            if   attr == "start_date_time":
                setattr(dest, attr, re.sub("\s", "0", getattr(src, attr)))
            elif attr == "dataset_name":
                setattr(dest, attr, re.sub("T..", "T99", getattr(src, attr)))
            else:
                setattr(dest, attr, getattr(src, attr))

            # Make the spheroid axis lengths CF-compliant
            if attr in {'semi_major', 'semi_minor'} and hasattr(src, 'grid_mapping_name'):
                setattr(dest, attr + '_axis', getattr(src, attr))


def dataset_name(dataset, template):
    r"""Create an appropriate file path from a GOES dataset."""

    #netcdf OR_GLM-L2-GLMF-M6_G16_T05_e20211202154400 {
    #dimensions:
    #   x = 904 ;
    #   y = 506 ;
    #   nmeas = 1 ;
    #   dmw_band = 1 ;
    #variables:
    #   short x(x) ;
    #           x:long_name = "GOES fixed grid projection x-coordinate" ;
    #           x:standard_name = "projection_x_coordinate" ;
    #           x:axis = "X" ;
    #           x:units = "rad" ;
    #           x:add_offset = -0.151844 ;
    #           x:scale_factor = 5.6e-05 ;
    #   short y(y) ;
    #           y:long_name = "GOES fixed grid projection y-coordinate" ;
    #           y:standard_name = "projection_y_coordinate" ;
    #           y:axis = "Y" ;
    #           y:units = "rad" ;
    #           y:add_offset = 0.151844 ;
    #           y:scale_factor = -5.6e-05 ;
    #   short Flash_extent_density(y, x) ;
    #           Flash_extent_density:_FillValue = 0s ;
    #           Flash_extent_density:standard_name = "Flash Extent Density" ;
    #           Flash_extent_density:long_name = "GLM Flash Extent Density" ;
    #           Flash_extent_density:grid_mapping = "goes_imager_projection" ;
    #           Flash_extent_density:_Unsigned = "true" ;
    #   short Total_Optical_energy(y, x) ;
    #           Total_Optical_energy:_FillValue = 0s ;
    #           Total_Optical_energy:standard_name = "Total Optical Energy" ;
    #           Total_Optical_energy:long_name = "GLM Total Optical Energy" ;
    #           Total_Optical_energy:grid_mapping = "goes_imager_projection" ;
    #           Total_Optical_energy:_Unsigned = "true" ;
    #           Total_Optical_energy:add_offset = 0. ;
    #           Total_Optical_energy:scale_factor = 1.52597e-06 ;
    #           Total_Optical_energy:units = "nJ" ;
    #   short Minimum_flash_area(y, x) ;
    #           Minimum_flash_area:_FillValue = 0s ;
    #           Minimum_flash_area:standard_name = "Minimum Flash Area" ;
    #           Minimum_flash_area:long_name = "GLM Minimum Flash Area" ;
    #           Minimum_flash_area:grid_mapping = "goes_imager_projection" ;
    #           Minimum_flash_area:_Unsigned = "true" ;
    #           Minimum_flash_area:add_offset = 0. ;
    #           Minimum_flash_area:scale_factor = 10. ;
    #           Minimum_flash_area:units = "km^2" ;
    #   byte DQF(y, x) ;
    #           DQF:_FillValue = -1b ;
    #           DQF:grid_mapping = "goes_imager_projection" ;
    #           DQF:number_of_qf_values = 6LL ;
    #           DQF:units = "1" ;
    #           DQF:_Unsigned = "true" ;
    #           DQF:standard_name = "status_flag" ;
    #           DQF:long_name = "GLM data quality flags" ;
    #           DQF:flag_values = 0LL, 1LL ;
    #           DQF:flag_meanings = "valid, invalid" ;
    #   int goes_imager_projection ;
    #           goes_imager_projection:long_name = "GOES-R ABI fixed grid projection" ;
    #           goes_imager_projection:grid_mapping_name = "geostationary" ;
    #           goes_imager_projection:latitude_of_projection_origin = 0. ;
    #           goes_imager_projection:longitude_of_projection_origin = -75. ;
    #           goes_imager_projection:semi_major_axis = 6378137LL ;
    #           goes_imager_projection:semi_minor_axis = 6356752.31414 ;
    #           goes_imager_projection:perspective_point_height = 35786023. ;
    #           goes_imager_projection:inverse_flattening = 298.2572221 ;
    #           goes_imager_projection:sweep_angle_axis = "x" ;
    #   double flash_lat(nmeas) ;
    #           flash_lat:standard_name = "flash point latitude" ;
    #           flash_lat:units = "degrees_north" ;
    #   double flash_lon(nmeas) ;
    #           flash_lon:standard_name = "flash point longitude" ;
    #           flash_lon:units = "degrees_east" ;
    #   float flash_area(nmeas) ;
    #           flash_area:standard_name = "flash point area" ;
    #           flash_area:units = "km*km" ;
    #   float flash_energy(nmeas) ;
    #           flash_energy:standard_name = "flash point energy" ;
    #           flash_energy:units = "J" ;
    #   byte flash_DQF(nmeas) ;
    #           flash_DQF:standard_name = "flash DQF" ;
    #           flash_DQF:units = "1" ;
    #   uint64 flash_time(nmeas) ;
    #           flash_time:standard_name = "flash time" ;
    #           flash_time:units = "s" ;
    #   double flash_duration(nmeas) ;
    #           flash_duration:standard_name = "flash duration" ;
    #           flash_duration:units = "s" ;
    #   byte band_id(dmw_band) ;
    #           band_id:standard_name = "band id" ;
    #           band_id:units = "1" ;
    #
    #// global attributes:
    #           :cdm_data_type = "Image" ;
    #           :Conventions = "CF-1.7" ;
    #           :dataset_name = "OR_GLM-L2-GLMF-M6_G16_T05_e20211202154400.nc" ;
    #           :id = "93cb84a3-31ef-4823-89f5-c09d88fc89e8" ;
    #           :institution = "DOC/NOAA/NESDIS > U.S. Department of Commerce, National Oceanic and Atmospheric Administration, National Environmental Satellite, Data, and Information Services" ;
    #           :instrument_ID = "GLM-1" ;
    #           :instrument_type = "GOES R Series Geostationary Lightning Mapper" ;
    #           :iso_series_metadata_id = "f5816f53-fd6d-11e3-a3ac-0800200c9a66" ;
    #           :keywords = "ATMOSPHERE > ATMOSPHERIC ELECTRICITY > LIGHTNING, ATMOSPHERE > ATMOSPHERIC PHENOMENA > LIGHTNING" ;
    #           :keywords_vocabulary = "NASA Global Change Master Directory (GCMD) Earth Science Keywords, Version 7.0.0.0.0" ;
    #           :license = "Unclassified data.  Access is restricted to approved users only." ;
    #           :Metadata_Conventions = "Unidata Dataset Discovery v1.0" ;
    #           :naming_authority = "gov.nesdis.noaa" ;
    #           :orbital_slot = "GOES-East" ;
    #           :platform_ID = "G16" ;
    #           :processing_level = "National Aeronautics and Space Administration (NASA) L2" ;
    #           :production_data_source = "Realtime" ;
    #           :production_environment = "OE" ;
    #           :production_site = "GCP" ;
    #           :project = "GOES" ;
    #           :scene_id = "Full Disk" ;
    #           :spatial_resolution = "2km at nadir" ;
    #           :standard_name_vocabulary = "CF Standard Name Table (v25, 05 July 2013)" ;
    #           :summary = "The Lightning Detection Gridded product generates fields starting from the GLM Lightning Detection Events, Groups, Groups, Flashes product.  It consists of flash extent density, event density, average flash area, average group area, total energy, flash centroid density, and group centroid density." ;
    #           :time_coverage_end = "2021-12-02T15:44:00Z" ;
    #           :time_coverage_start = "2021-12-02T15:43:00Z" ;
    #           :timeline_id = "ABI Mode 6" ;
    #           :title = "GLM L2 Lightning Detection Gridded Product" ;
    #           :mission_name = "GLM_plot" ;
    #}

    #
    # Extract relevant information from dataset_name
    #
    # :dataset_name = "OR_GLM-L2-GLMF-M6_G16_T05_e20211202154400.nc" ;
    #

    #           groups           1         2        3           4         5         6        7
    prod_info = re.match('(OR_[A-Z]{3,5})-(L.)-([A-Z]{3,5})(C|F|M1|M2)-(M[346])_(G1[6789])_T(..)', dataset.dataset_name)

    # Product prefix
    # Example: OR_GLM
    prefix      = prod_info.group(1)

    # Product processing level
    # Example: L2, L3
    lev         = prod_info.group(2)
    level       = re.sub('L', 'Level', lev)

    # Product name
    # Examples: GLM
    l2_name     = prod_info.group(3)

    # Product coverage and scene name
    # Examples: C, F, M1, M2
    covr        = prod_info.group(4)
    if   covr == 'C':
         scene  = 'CONUS'
    elif covr == 'F':
         scene  = 'FullDisk'
    elif covr == 'M1':
         scene  = 'Mesoscale-1'
    elif covr == 'M2':
         scene  = 'Mesoscale-2'
    else:
         scene  = 'Unknown'

    # Satellite scanning mode
    # Examples: M3, M4, M6
    abi_mode    = prod_info.group(5)

    # Satellite id and satellite name
    # Examples: G16, G17, G18, G19
    #           GOES16, GOES17, GOES18, GOES19
    sat_id      = prod_info.group(6)
    satellite   = re.sub('G', 'GOES', sat_id)

    # Product longname (hardwired)
    l2_longname = 'GLMISatSS'

    # Product start, end and center times
    # :time_coverage_start = "2021-12-02T15:43:00Z" ;
    # :time_coverage_end = "2021-12-02T15:44:00Z" ;
    ssecs = int(goes_time_to_dt(dataset.time_coverage_start).strftime("%s"))
    esecs = int(goes_time_to_dt(dataset.time_coverage_end).strftime("%s"))
    csecs = (ssecs + esecs) / 2

    s_time = datetime.fromtimestamp(ssecs).strftime('%Y%j%H%M%S')
    e_time = datetime.fromtimestamp(esecs).strftime('%Y%j%H%M%S')
    c_time = datetime.fromtimestamp(csecs).strftime('%Y%j%H%M%S')

    # Product year, month, day in CCYYDDMM format
    cymd   = datetime.fromtimestamp(esecs).strftime('%Y%m%d')

    return template.format(prefix=prefix, level=level, lev=lev, mode=abi_mode, scene=scene, satellite=satellite, sat=sat_id, pname=l2_longname,l2name=l2_name,covr=covr, cymd=cymd, stime=s_time, etime=e_time, ctime=c_time)


def init_nc_file(source_nc, output_nc):
    r"""Initialize an output netCDF4 file from the input tile file."""

    #           groups           1         2        3           4         5         6        7
    prod_info = re.match('(OR_[A-Z]{3,5})-(L.)-([A-Z]{3,5})(C|F|M1|M2)-(M[346])_(G1[6789])_T(..)', source_nc.dataset_name)

    # Copy global attributes, create dimensions, add our metadata
    copy_attrs(source_nc, output_nc, lambda a: a.startswith('tile'))
    output_nc.product_tiles_received = 0

    # Product level
    lev = prod_info.group(2)
    if lev == 'L2':
        output_nc.product_flashes_seen = 0

    # Label the creator of the reconstituted image
    output_nc.created_by = 'ldm-alchemy'

    # NB: VLAB GLM tiles don't have global attributes 'product_rows' or 'product_columns'
    #     The size of the output file is a function of the coverage:
    #       coverage      lins   eles  resolution
    #       CONUS       - 1500 x 2500     2 km
    #       FullDisk    - 5424 x 5424     2 km
    #       Mesoscale-1 -  500 x  500     2 km
    #       Mesoscale-2 -  500 x  500     2 km

    # Product rows and columns
    covr = prod_info.group(4)
    if   covr == 'C':
        product_rows    = 1500
        product_columns = 2500
    elif covr == 'F':
        product_rows    = 5424
        product_columns = 5424
    elif covr == 'M1':
        product_rows    =  500
        product_columns =  500
    elif covr == 'M2':
        product_rows    =  500
        product_columns =  500
    else:
        product_rows    = 5425
        product_columns = 5425
    
    # Create output file dimensions
    # Dimensions that are in both Level2 and Level3 files
    output_nc.createDimension('y', product_rows)
    output_nc.createDimension('x', product_columns)

    # Dimensions that are in Level2 but not Level3 files
    if lev == 'L2':
        output_nc.createDimension('nmeas', None)
        output_nc.createDimension('dmw_band', 1)

    # Create a scalar time coordinate variable from the string attribute
    # NB: VLAB GLM tiles don't have 'start_date_time' global attribute
    #     It does have 'time_coverage_start' global attribute
    dt = goes_time_to_dt(source_nc.time_coverage_start)

    time_var = output_nc.createVariable('time', np.int32)
    time_var.units = 'seconds since 2017-01-01'
    time_var.standard_name = 'time'
    time_var.long_name = 'The start date / time that the satellite began capturing the scene'
    time_var.axis = 'T'
    time_var.calendar = 'standard'
    time_var[:] = date2num(dt, time_var.units)

    # NB: VLAB GLM tiles don't have 'product_tile_height' or 'product_tile_width' global attributes
    #       product_tile_height == y dimension size
    #       product_tile_width  == x dimension size
    #     For FullDisk VLAB GLM files, the tile sizes are:
    #      lines elems
    #       404 x 904 - image rows 1
    #       506 x 904 - image rows 2 - 10
    #       467 x 904 - image rows 11
    #     For chunking, set 'product_tile_height' to 504 and 'product_tile_width' to 904
    product_tile_height = 506
    product_tile_width  = 904

    chunk_height = min(product_tile_height, product_rows)
    chunk_width  = min(product_tile_width,  product_columns)

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
             extra_args['chunksizes'] = (chunk_height, chunk_width)

        # Create the variable and copy its attributes
        var = output_nc.createVariable(var_name, old_var.datatype,
                                       old_var.dimensions, **extra_args)
        copy_attrs(old_var, var, lambda a: '_FillValue' in a)

        # Need to add time coordinate to any variable that cares
        if hasattr(var, 'grid_mapping'):
            var.coordinates = ' '.join(('time',) + var.dimensions)

    return output_nc


def copy_tile(input_ds, output_ds):
    r"""Copy data from input tile to output."""

    # There's no need to do any scaling--just copy the integer data
    input_ds.set_auto_scale(False)
    output_ds.set_auto_scale(False)

    #           groups           1         2        3           4         5         6        7
    prod_info = re.match('(OR_[A-Z]{3,5})-(L.)-([A-Z]{3,5})(C|F|M1|M2)-(M[346])_(G1[6789])_T(..)', input_ds.dataset_name)

    # Log the tile being processed
    product_level = prod_info.group(2)
    tile_number   = prod_info.group(7)
    logger.info('copy_tile:: processing %s tile T%s', product_level, tile_number)

    # Set up slices based on the column and row offsets and
    # product column and row width and height
    #
    # NB: VLAB GLM tiles don't have:
    #      'product_tile_height'
    #      'product_tile_width'
    #      'tile_row_offset'
    #      'tile_column_offset'
    #     So:
    #       product_tile_height == y tile dimension size
    #       product_tile_width  == x tile dimension size
    #
    #       tile_row_offset    - needs to be calculated from tile number
    #       tile_column_offset - needs to be calculated from tile number
    #
    product_tile_height = input_ds.dimensions['y'].size
    product_tile_width  = input_ds.dimensions['x'].size

    # Product row and column sizes for standard 2km products
    covr = prod_info.group(4)
    if   covr == 'C':
         product_rows    = 1500
         product_columns = 2500
    elif covr == 'F':
         product_rows    = 5424
         product_columns = 5424
    elif covr == 'M1':
         product_rows    =  500
         product_columns =  500
    elif covr == 'M2':
         product_rows    =  500
         product_columns =  500
    else:
         product_rows    = 5425
         product_columns = 5425

    # Product tile number and tile row
    #
    # NB: this assumes FullDisk images since there are no other ones at the time
    #     this was written
    #
    tile_number = int(prod_info.group(7))
    tile_row    = int((tile_number+2) / 6)

    # Set the height for the shortest tiles
    product_tile_height_min = 404

    # Calculate tile_row_offset and tile_column_offset
    if   tile_number <= 3:
        tile_row_offset    = 0
        tile_column_offset = product_tile_width * (tile_number + 1)
    elif tile_number <= 57:
        tile_row_offset    = product_tile_height * (tile_row - 1) + product_tile_height_min -1
        tile_column_offset = product_tile_width  * (tile_number - (tile_row*6 - 2))
    else:
        tile_row_offset    = product_tile_height * (tile_row - 1) + product_tile_height_min -1
        tile_column_offset = product_tile_width  * (tile_number - (tile_row*6 - 2) + 1)

    logger.debug('product_tile_height: %s', product_tile_height)
    logger.debug('product_tile_width: %s', product_tile_width)
    logger.debug('tile_row_offset: %s', tile_row_offset)
    logger.debug('tile_column_offset: %s', tile_column_offset)

    col_slice_start = tile_column_offset
    tile_width      = product_tile_width
    col_slice_end   = col_slice_start + tile_width

    if col_slice_end > product_columns:
        tile_width    = product_tile_width - (col_slice_end - product_columns)
        col_slice_end = col_slice_start + tile_width

    row_slice_start = tile_row_offset
    tile_height     = product_tile_height
    row_slice_end   = row_slice_start + tile_height

    if row_slice_end > product_rows:
        tile_height   = product_tile_height - (row_slice_end - product_rows)
        row_slice_end = row_slice_start + tile_height

    col_slice = slice(col_slice_start, col_slice_end)
    row_slice = slice(row_slice_start, row_slice_end)

    # 1-D variables in Level 2 VLAB GLM files
    #
    # 'nmeas' is the unlimited dimension
    # 'dwm_band' has value 1
    #
    # double flash_lat(nmeas) ;
    # double flash_lon(nmeas) ;
    # float  flash_area(nmeas) ;
    # float  flash_energy(nmeas) ;
    # byte   flash_DQF(nmeas) ;
    # uint64 flash_time(nmeas) ;
    # double flash_duration(nmeas) ;
    # byte   band_id(dmw_band) ;
    #
    # flash_num                      - number of flashes in this tile
    # output_ds.product_flashes_seen - total number of flashes

    # The number of flashes is only available in Level2 files
    if product_level == 'L2':
        flash_num         = input_ds.dimensions['nmeas'].size
        flash_col_start   = output_ds.product_flashes_seen
        flash_col_end     = flash_num
        output_ds.product_flashes_seen += flash_num
    else:
        flash_col_start   = 0
        flash_col_end     = 0
    
    # The 'x' and 'y' values in VLAB GLM products are invariant
    y_vals = array('h',range(product_rows))
    x_vals = array('h',range(product_columns))

    # Copy data for x, y, and data variables using appropriate slices
    for var_name, src_var in input_ds.variables.items():
        dest_var = output_ds.variables[var_name]
        if   var_name == 'x':
            logger.debug('copy_tile:: var_name = %s', var_name)
            #dest_var[col_slice] = src_var[:tile_width]
            dest_var[:] = x_vals[:]
        elif var_name == 'y':
            logger.debug('copy_tile:: var_name = %s', var_name)
            #dest_var[row_slice] = src_var[:tile_height]
            dest_var[:] = y_vals[:]
        elif src_var.ndim == 2:
            logger.debug('copy_tile:: var_name = %s, src_var.ndim = %d', var_name, src_var.ndim)
            dest_var[row_slice, col_slice] = src_var[0:tile_height,0:tile_width]
        elif var_name == 'band_id':
            logger.debug('copy_tile:: var_name = %s', var_name)
            dest_var[0] = src_var[:]
        elif var_name != 'goes_imager_projection':
            logger.debug('copy_tile:: var_name = %s', var_name)
            dest_var[flash_col_start:] = src_var[0:flash_col_end]

    output_ds.product_tiles_received += 1
    output_ds.sync()


def find_files(source_dir):
    r"""Find all the netCDF4 files in a directory tree."""
    for root, dirs, files in os.walk(source_dir):
        for fname in sorted(files):
            logger.debug('Input file: %s', fname)
            if not fname.endswith('nc'):
                if not fname.endswith('gz'):
                    continue
                else:
                    # 20201222 - processing for gzip compressed files
                    f = gzip.open(os.path.join(root,fname), 'rb')
                    content = f.read()
                    f.close()
                    temp = tempfile.NamedTemporaryFile(delete=False)
                    temp.write(content)
                    fname = temp.name

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

            if os.path.exists("/opt/ldm/util/L2ProdLinkAndPqinsert.sh"):
                cmd = "/opt/ldm/util/L2ProdLinkAndPqinsert.sh %s SPARE 96 /opt/ldm/logs/L2ProdLinkAndPqinsert.log" % (self.output_name)
                f = os.popen(cmd)
                message = f.readlines()
                f.close()

    async def process_items(self, on_done):
        while True:
            try:
                product = await asyncio.wait_for(self._queue.get(), self.timeout)
                logger.debug('Processing product: %s', product.filepath())
                try:
                    if not self.output:
                        self.output_name = os.path.join(self.out_dir, dataset_name(product, self.template))

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
                    #if (self.output.product_tiles_received >= self.output.number_product_tiles
                    if (self.output.product_tiles_received >= 61
                            and self._queue.empty()):
                        logger.info('%s: All tiles received.',
                                    os.path.basename(self.output_name))
                        break
                    else:
                        logger.info('%s: %d of %d tiles received.',
                                    os.path.basename(self.output_name),
                                    self.output.product_tiles_received, 61)

                                    #self.output.number_product_tiles)

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
    except Exception:
        logger.exception('Error opening file for netCDF input')
    finally:
        os.remove(temp.name)


#
# Argument parsing
#
def setup_arg_parser():
    import argparse

    # Example invocation:
    #
    # ------------------------------------------
    # - CSU/CIRA GOES-16 GeoColor netCDF4 Data -
    # ------------------------------------------
    #

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Assemble netCDF4 tiles of GOES data into '
                                                 'single files.')
    parser.add_argument('-d', '--data_dir', help='Base output directory', type=str,
                        default='/data/ldm/pub/native/satellite/GOES/GLM')
    parser.add_argument('-t', '--timeout', help='Timeout in seconds for waiting for data',
                        default=60, type=int)
    parser.add_argument('-s', '--source', help='Source directory for data tiles', type=str,
                        default='')
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-f', '--filename', help='Filename format string. Uses Python string format specification',
                        default=os.path.join('{satellite}', 'Products', '{pname}', '{level}', '{scene}', '{cymd}',
                                             '{prefix}-{lev}-{l2name}{covr}-{mode}_{sat}_s{stime}0_e{etime}0_c{ctime}0.nc'))
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

    # Try to catch core dumps
    faulthandler.enable(open('/opt/ldm/logs/glm-restitch-crash.log', 'w'))

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
