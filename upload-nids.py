#!/usr/bin/env python
# Copyright (c) 2015-2020 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
from datetime import datetime
import zlib

from aws import UploadS3
from ldm_async import LDMReader, set_log_file, set_log_level, setup_arg_parser

def remove_header(buff):
    msg_start = buff[:64].rfind(b'\r\r\n')
    if msg_start == -1:
        return buff
    return buff[msg_start + 3:]


def handle_zlib(buff):
    try:
        return remove_header(zlib.decompressobj().decompress(buff))
    except zlib.error:
        return buff


# 18 skips header block to product description block, 22 is byte offset into that
DATE_OFF = 18 + 22
TIME_OFF = DATE_OFF + 2
def read_volume_time(buff):
    vol_date = int.from_bytes(buff[DATE_OFF:DATE_OFF + 2], byteorder='big')
    vol_time = int.from_bytes(buff[TIME_OFF:TIME_OFF + 4], byteorder='big')
    return datetime.utcfromtimestamp((vol_date - 1) * 86400 + vol_time)


class UploadNIDS(UploadS3):
    @staticmethod
    def prod_id_to_key(prod_id, data):
        # Get the proper datetime by parsing from the product since we have it in memory
        dt = read_volume_time(handle_zlib(remove_header(data)))

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
    ldm.process()
