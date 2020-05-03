# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import base64
from contextlib import contextmanager
import hashlib
import logging
import threading

import botocore.exceptions

from ldm_async import Job

logger = logging.getLogger('LDM')


class UploadS3(Job):
    def __init__(self, bucket, name='UploadS3'):
        super().__init__(name)
        self.bucket_pool = S3BucketPool(bucket)
        # self.sns_pool = SNSBucketPool(args.sns)

    @staticmethod
    def make_key(prod_id, data):
        return prod_id

    def run(self, item):
        with self.bucket_pool.use() as bucket:  #, sns_pool.use() as topic:
            try:
                # Calculate MD5 checksum for integrity
                digest = base64.b64encode(hashlib.md5(item.data).digest()).decode('ascii')
                key = self.make_key(*item)

                # Write to S3
                logger.info('Uploading to S3 as: %s', key)
                bucket.put_object(Key=key, Body=item.data, ContentMD5=digest)
            except botocore.exceptions.ClientError as e:
                logger.exception('Error putting object on S3:', exc_info=e)
                raise IOError from e


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
