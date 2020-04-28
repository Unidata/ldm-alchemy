#!/usr/bin/env python
# Copyright (c) 2015-2020 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
from datetime import datetime, timedelta

from aws import UploadS3
from ldm_async import LDMReader, set_log_file, set_log_level, setup_arg_parser

class UploadNIDS(UploadS3):
    @staticmethod
    def prod_id_to_key(prod_id):
        # SDUS85 KBOU 280642 /pN0MFTG !nids/
        parts = prod_id.split(' ')
        date_group = parts[2]
        site_prod_group = parts[3]
        day = int(date_group[:2])
        hour = int(date_group[2:4])
        minute = int(date_group[4:6])

        # Use current datetime to fill in missing pieces of date/time. If we get an error,
        # or get a time in the future, fall back to using yesterday's date.
        now = datetime.utcnow()
        try:
            dt = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
            if dt > now:
                raise ValueError
        except ValueError:
            now = now - timedelta(days=1)
            dt = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)

        prod = site_prod_group[2:5]
        site = site_prod_group[5:8]
        return f'{site}/{prod}/{dt:%Y/%m/%d}/Level3_{site}_{prod}_{dt:%Y%m%d_%H%M}.nids'


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
