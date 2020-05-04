# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import asyncio
import logging
from pathlib import Path

from async_base import Main
from ldm_async import set_log_level, setup_arg_parser, Product
from upload_nids import UploadNIDS

logger = logging.getLogger('alchemy')


async def async_glob_path(path, glob):
    for p in Path(path).glob(glob):
        _, stid, prod, *_ = p.stem.split('_')
        yield Product('A B C --' + prod + stid, p.read_bytes())
        await asyncio.sleep(0.001)


class PathReader(Main):
    def __init__(self, path, glob, **kwargs):
        self.path = path
        self.glob = glob
        super().__init__(nthreads=kwargs.pop('nthreads'))

    def __aiter__(self):
        return async_glob_path(self.path, self.glob)


if __name__ == '__main__':
    parser = setup_arg_parser('Upload products from LDM to AWS S3.')
    parser.add_argument('-b', '--bucket', help='Bucket to upload files to', type=str,
                        default='unidata-nexrad-level3')
    parser.add_argument('-p', '--path', help='Bucket to upload files to', type=str,
                        required=True)
    args = parser.parse_args()
    logger.addHandler(logging.StreamHandler())
    set_log_level(args)

    reader = PathReader(args.path, "**/*.nids", nthreads=args.threads)
    reader.connect(UploadNIDS(args.bucket))
    reader.run()
