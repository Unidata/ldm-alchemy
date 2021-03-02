#!/usr/bin/env python
# Copyright (c) 2015-2020 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
from datetime import datetime
import zlib

from aws import UploadS3
from ldm_async import LDMReader, set_log_file, set_log_level, setup_arg_parser

def remove_header(buff):
    msg_start = buff[:32].rfind(b'\r\r\n')
    if msg_start == -1:
        return buff
    return buff[msg_start + 3:]


def decompress_as_needed(data):
    """Un-zlib the product as necessary."""
    frames = bytearray()
    zlib_compressed_frames = False
    binary_data = remove_header(bytes(data))
    while binary_data:
        decomp = zlib.decompressobj()
        try:
            frames.extend(decomp.decompress(binary_data))
            zlib_compressed_frames = True
            binary_data = decomp.unused_data
        except zlib.error:
            frames.extend(binary_data)
            break

    if zlib_compressed_frames:
        # Remove NOAAPORT CCB header and leave WMO header
        logger.debug('Handling zlib frames')
        data = bytes(frames)
        ccb_len = int.from_bytes(data[:2], 'big')

        # CCB length (in "byte pairs") is low 14 bits; high 2 bits should be 01
        if ccb_len >> 14 == 1:
            ccb_len &= 0x3fff
            return data[ccb_len * 2:]
        else: # If we don't get expected leading bits, just return raw data
            return data
    else:
        return data


# 18 skips header block to product description block, 22 is byte offset into that
DATE_OFF = 18 + 22
TIME_OFF = DATE_OFF + 2
def read_volume_time(buff):
    vol_date = int.from_bytes(buff[DATE_OFF:DATE_OFF + 2], byteorder='big')
    vol_time = int.from_bytes(buff[TIME_OFF:TIME_OFF + 4], byteorder='big')
    return datetime.utcfromtimestamp((vol_date - 1) * 86400 + vol_time)


class UploadNIDS(UploadS3):
    def run(self, item):
        data = item.data

        # Strip off start transmission and sequence number
        if data.startswith(b'\x01\r\r\n') and data[7:11] == b' \r\r\n':
            logger.debug('Stripping start bytes %s', data[:11])
            data = data[11:]

        # Strip off end transmission bytes
        if data.endswith(b'\r\r\n\x03'):
            logger.debug('Stripping end bytes %s', data[-4:])
            data = data[:-4]

        # See if we need to deflate
        data = decompress_as_needed(data)

        logger.debug('start bytes: %s', data[:64])
        logger.debug('end bytes: %s', data[-64:])

        item = item._replace(data=data)
        super().run(item)

    @staticmethod
    def make_key(prod_id, data):
        # Get the proper datetime by parsing from the product since we have it in memory
        dt = read_volume_time(remove_header(data))

        # Get site and product from ID, like: SDUS85 KBOU 280642 /pN0MFTG !nids/
        site_prod_group = prod_id.split(' ')[3]
        prod = site_prod_group[2:5]
        site = site_prod_group[5:8]

        return f'{site}_{prod}_{dt:%Y_%m_%d_%H_%M_%S}'


if __name__ == '__main__':
    parser = setup_arg_parser('Upload products from LDM to AWS S3.')
    parser.add_argument('-b', '--bucket', help='Bucket to upload files to', type=str,
                        required=True)
    args = parser.parse_args()

    logger = set_log_file('upload-nids.log')
    set_log_level(args)

    ldm = LDMReader(nthreads=args.threads)
    ldm.connect(UploadNIDS(args.bucket))
    ldm.run()
