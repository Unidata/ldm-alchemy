#!/usr/bin/env python
# Copyright (c) 2008-2015 MetPy Developers.
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause

import asyncio
from datetime import datetime
import logging
import os.path
from pathlib import Path

from boltons.cacheutils import LRU
from netCDF4 import Dataset, date2num
from metpy.io.metar import MetarProduct
from process_stations import StationLookup
from l2assemble import read_metadata


def create_netcdf_file(fname):
    ds = Dataset(fname, 'w')
    ds.createDimension('sample')
    ds.createDimension('station')
    ds.featureType = 'timeSeries'
    ds.Conventions = 'CF-1.7'
    ds.institution = 'UCAR/Unidata'
    ds.source = 'METAR Reports'
    ds.title = 'Reformatted METAR observations'

    station_dims = ('station',)
    lon_var = ds.createVariable('lon', 'f4', station_dims)
    lon_var.long_name = 'longitude'
    lon_var.units = 'degrees_east'
    lon_var.standard_name = 'longitude'

    lat_var = ds.createVariable('lat', 'f4', station_dims)
    lat_var.long_name = 'latitude'
    lat_var.units = 'degrees_north'
    lat_var.standard_name = 'latitude'

    alt_var = ds.createVariable('alt', 'f4', station_dims)
    alt_var.long_name = 'vertical distance above mean sea level'
    alt_var.units = 'meter'
    alt_var.positive = 'up'
    alt_var.standard_name = 'surface_altitude'

    stid_var = ds.createVariable('stid', str, station_dims)
    stid_var.long_name = 'station id'
    stid_var.standard_name = 'platform_id'
    stid_var.cf_role = 'timeseries_id'

    stn_name_var = ds.createVariable('name', str, station_dims)
    stn_name_var.long_name = 'station name'
    stn_name_var.standard_name = 'platform_name'

    data_dims = ('sample',)
    index_var = ds.createVariable('index', 'i4', data_dims)
    index_var.instance_dimension = 'station'

    # TODO: Can we compress these and actually see the gains (i.e. when does compression happen
    # TODO: at time data added or at file write time?)
    time_var = ds.createVariable('time', 'i4', data_dims)
    time_var.long_name = 'time of observation'
    time_var.units = 'minutes since 1970-1-1 00:00:00'

    def create_data_variable(name, units, std_name='', dtype='f4', missing=-999.):
        var = ds.createVariable(name, dtype, data_dims)
        var.units = units
        if std_name:
            var.standard_name = std_name
        var.coordinates = 'lon lat alt time stid'
        var.missing_value = missing

    create_data_variable('altimeter_setting', 'millibars')
    create_data_variable('dewpoint', 'degC', 'dew_point_temperature')
    create_data_variable('sea_level_pressure', 'millibars', 'air_pressure_at_sea_level')
    create_data_variable('sky_coverage', 'percent', 'cloud_area_fraction')
    create_data_variable('temperature', 'degC', 'air_temperature')
    create_data_variable('visibility', 'meters', 'visibility_in_air')
    create_data_variable('wind_direction', 'degrees', 'wind_from_direction')
    create_data_variable('wind_speed', 'm/s', 'wind_speed')
    create_data_variable('wind_gust', 'm/s', 'wind_speed_of_gust')

    return ds


class SurfaceNetcdf:
    def __init__(self, dt, output_dir, lookup):
        out_path = Path(os.path.join(output_dir, 'METAR_{dt:%Y%m%d_%H00}.nc'.format(dt=dt)))
        self.station_lookup = lookup
        self.stations = dict()
        if out_path.exists():
            self.ds = Dataset(str(out_path), 'a')
            for ind, stid in enumerate(self.ds['stid']):
                self.stations[stid] = ind
        else:
            self.ds = create_netcdf_file(str(out_path))

    def add_ob(self, ob):
        ob_num = len(self.ds.dimensions['sample'])

        if ob.get('visibility', '') == 'CAVOK':
            del ob['visibility']

        self.append_to_var('altimeter_setting', ob_num, ob.get('altimeter'))

        temp, dewp = ob.get('temperature', (None, None))
        self.append_to_var('temperature', ob_num, temp)
        self.append_to_var('dewpoint', ob_num, dewp)

        self.append_to_var('sea_level_pressure', ob_num, ob.get('sea_level_pressure'))

        coverage = max((c[1] for c in ob.get('sky_coverage', [])), default=0.) / 8.
        self.ds['sky_coverage'][ob_num] = 100 * coverage

        self.append_to_var('visibility', ob_num, ob.get('visibility'))

        wind = ob.get('wind', dict())
        if wind.get('direction', '') == 'VRB':
            wind['direction'] = None
        self.append_to_var('wind_direction', ob_num, wind.get('direction'))
        self.append_to_var('wind_speed', ob_num, wind.get('speed'))
        self.append_to_var('wind_gust', ob_num, wind.get('gust'))

        self.ds['time'][ob_num] = date2num(ob['datetime'], self.ds['time'].units)
        self.ds['index'][ob_num] = self.update_stations(ob['stid'])

    def append_to_var(self, varname, index, value):
        if value:
            self.ds[varname][index] = value.to(self.ds[varname].units)
        else:
            self.ds[varname][index] = self.ds[varname].missing_value

    def update_stations(self, stid):
        if stid in self.stations:
            stn_index = self.stations[stid]
        else:
            stn_info = self.station_lookup(stid)

            stn_index = len(self.ds.dimensions['station'])
            self.stations[stid] = stn_index

            self.ds['lon'][stn_index] = stn_info.longitude
            self.ds['lat'][stn_index] = stn_info.latitude
            self.ds['alt'][stn_index] = stn_info.altitude
            self.ds['stid'][stn_index] = stn_info.id
            self.ds['name'][stn_index] = stn_info.name
        return stn_index


async def flush_files(cache):
    while True:
        await asyncio.sleep(300)
        logger.debug('Syncing files to disk.')
        for file in cache.values():
            file.ds.sync()

#
# Handling of input
#
async def read_product(stream):
    # Read metadata from LDM for prod id and product size, then read in the appropriate
    # amount of data.
    prod_id, prod_length = await read_metadata(stream)
    data = await stream.readexactly(prod_length)
    logger.debug('Read product: %s. (%d bytes)', prod_id, len(data))
    return data


async def read_stream(loop, input_stream, sinks, tasks):
    stream_reader = asyncio.StreamReader(loop=loop)
    transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stream_reader), input_stream)
    try:
        while True:
            prod = await read_product(stream_reader)

            for sink in sinks:
                await sink.put(prod)
    except EOFError:
        # If we get an EOF, flush out the queues top down.
        logger.warning('Finishing due to EOF.')
        for sink in sinks:
            logger.debug('Flushing processing queue. Items left: %d', sink.qsize())
            await sink.join()
        for t in tasks:
            logger.debug('Cancelling task: %s', t)
            t.cancel()
        await asyncio.sleep(0.01)  # Just enough to let other things close out
        transport.close()


async def parse_product(queue, cache):
    while True:
        prod = await queue.get()
        try:
            product = MetarProduct(prod.decode('latin-1')[1:-1])
            for metar in product.reports:
                if not metar['null']:
                    key = metar['datetime'].replace(minute=0)
                    if key > datetime.utcnow():
                        key = key.replace(month=(key.month - 1) % 12)
                    if key > datetime.utcnow():
                        key = key.replace(year=key.year - 1)
                    sink = cache[key]
                    sink.add_ob(metar)
        except Exception as e:
            logger.exception('Processing error -- %s', prod, exc_info=e)
        finally:
            queue.task_done()


if __name__ == '__main__':
    import argparse
    import sys

    logger = logging.getLogger('LDMHandler')
    handler = logging.StreamHandler(open('metar2nc.log', 'at'))
    logger.addHandler(handler)

    parser = argparse.ArgumentParser(description='Write METAR reports as data in a'
                                                 'netCDF file')
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-o', '--output', help='Output directory', type=str, default='.')
    args = parser.parse_args()

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10

    read_in = sys.stdin.buffer

    station_lookup = StationLookup()
    cache = LRU(max_size=10, on_miss=lambda key: SurfaceNetcdf(key, output_dir=args.output,
                                                               lookup=station_lookup))

    # Set up event loop
    loop = asyncio.get_event_loop()

    product_queue = asyncio.Queue()
    tasks = [asyncio.ensure_future(parse_product(product_queue, cache)),
             asyncio.ensure_future(flush_files(cache))]

    # Add callback for stdin and start loop
    loop.run_until_complete(read_stream(loop, read_in, [product_queue], tasks))
    print(station_lookup)
    loop.close()
