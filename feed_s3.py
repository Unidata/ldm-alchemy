#!/usr/bin/env python
# Copyright (c) 2015-2020 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import base64
from contextlib import contextmanager
from datetime import datetime, timedelta
import hashlib
import threading

import botocore.exceptions

from ldm_async import set_log_file, Job, LDMReader

logger = set_log_file('feed_s3.log')

#
# Pools for AWS access objects
#
class SharedObjectPool(object):
    """A shared pool of managed objects

    Objects are created while a lock is held, then are doled out and returned using
    a context manager.
    """
    _create_lock = threading.Lock()  # We want one lock among all subclasses

    def __init__(self):
        import queue
        self._queue = queue.Queue()

    def _create_new(self):
        pass

    def borrow(self):
        if self._queue.empty():
            with self._create_lock:
                return self._create_new()

        return self._queue.get()

    def put(self, item):
        self._queue.put(item)

    @contextmanager
    def use(self):
        obj = self.borrow()
        try:
            yield obj
        finally:
            self.put(obj)


class S3BucketPool(SharedObjectPool):
    def __init__(self, bucket):
        super().__init__()
        self.bucket_name = bucket

    def _create_new(self):
        import boto3
        return boto3.session.Session().resource('s3').Bucket(self.bucket_name)


class SNSBucketPool(SharedObjectPool):
    def __init__(self, name):
        import boto3
        super().__init__()
        self.sns_arn = boto3.client('sns').create_topic(Name=name)['TopicArn']

    def _create_new(self):
        import boto3
        return boto3.session.Session().resource('sns').Topic(self.sns_arn)


class UploadS3(Job):
    def __init__(self, bucket):
        super().__init__('UploadS3')
        self.bucket_pool = S3BucketPool(bucket)
        # self.sns_pool = SNSBucketPool(args.sns)

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
            now = now - timedelta(day=1)
            dt = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)

        prod = site_prod_group[2:5]
        site = site_prod_group[5:8]
        return (f'{site}/{prod}/{dt.year}/{dt.month}/{dt.day}/Level3_{site}_{prod}_'
                f'{dt:%Y%m%d_%H%M}.nids')

    def run(self, item):
        with self.bucket_pool.use() as bucket:  #, sns_pool.use() as topic:
            try:
                # Calculate MD5 checksum for integrity
                digest = base64.b64encode(hashlib.md5(item.data).digest()).decode('ascii')
                key = self.prod_id_to_key(item.prod_id)

                # Write to S3
                logger.info('Uploading to S3 under key: %s (md5: %s)', key, digest)
                # bucket.put_object(Key=key, Body=item.data, ContentMD5=digest)
            except botocore.exceptions.ClientError as e:
                logger.exception('Error putting object on S3:', exception=e)
                raise IOError from e


def setup_arg_parser():
    """Set up command line argument parsing."""
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Upload products from LDM to AWS S3.')
    parser.add_argument('-b', '--bucket', help='Bucket to upload files to', type=str,
                        required=True)
    parser.add_argument('--threads', help='Specify number of threads to use.', default=20,
                        type=int)
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('other', help='Other arguments for LDM identification', type=str,
                        nargs='*')
    return parser


if __name__ == '__main__':
    args = setup_arg_parser().parse_args()

    # Figure out how noisy we should be. Start by clipping between -2 and 2.
    total_level = min(2, max(-2, args.quiet - args.verbose))
    logger.setLevel(30 + total_level * 10)  # Maps 2 -> 50, 1->40, 0->30, -1->20, -2->10
    logger.debug('Logging initialized.')

    ldm = LDMReader(nthreads=args.threads)
    ldm.connect(UploadS3(args.bucket))
    ldm.process()

