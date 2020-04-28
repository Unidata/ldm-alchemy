#!/usr/bin/env python
# Copyright (c) 2015-2020 University Corporation for Atmospheric Research/Unidata
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT

import asyncio
import functools
import sys
import threading

from contextlib import contextmanager

from ldm_async import set_log_file, LDMReader

logger = set_log_file('feed_s3.log')

#
# Coroutines for handling S3
#
def when_item_done(loop, queue, name, item, future):
    try:
        future.result()
        logger.debug('Finished %s.', name)
    except IOError:
        logger.warning('Failed to process %s. Queuing for retry...', name)
        loop.call_later(15, queue.put_nowait, item)
    except Exception:
        logger.exception('Item done exception:', exc_info=sys.exc_info())
    finally:
        queue.task_done()


async def write_products_s3(loop, queue, bucket_pool, sns_pool):
    while True:
        product = await queue.get()
        try:
            key = args.key.format(product.prod_id)
            logger.debug('Writing product to %s on S3 %s', key, bucket_pool.bucket_name)
            fut = loop.run_in_executor(None, upload_product_s3, bucket_pool, key, product, sns_pool)
            fut.add_done_callback(functools.partial(when_item_done, loop, queue, key, product))
        except Exception:
            logger.exception('write_products_s3 exception:', exc_info=sys.exc_info())


def upload_product_s3(s3_pool, key, product, sns_pool):
    with s3_pool.use() as bucket, sns_pool.use() as topic:
        put_s3_checked(bucket, key, product.data)
        # prod_info = product.prod_id
        # message = {'S3Bucket': s3_pool.bucket_name, 'Key': key}
        # message.update(prod_info.to_sns_message_dict())
        # topic.publish(Message=json.dumps(message),
        #               MessageAttributes=prod_info.to_sns_filter_attrs())


def put_s3_checked(bucket, key, data, **kwargs):
    import botocore.exceptions
    import hashlib
    import base64
    try:
        # Calculate MD5 checksum for integrity
        digest = base64.b64encode(hashlib.md5(data).digest()).decode('ascii')

        # Write to S3
        logger.debug('Uploading to S3 under key: %s (md5: %s)', key, digest)
        bucket.put_object(Key=key, Body=data, ContentMD5=digest, **kwargs)
    except botocore.exceptions.ClientError as e:
        logger.exception('Error putting object on S3:', exception=e)
        raise IOError from e


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


#
# Argument parsing
#
def setup_arg_parser():
    import argparse

    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Read NEXRAD Level2 LDM compressed blocks'
                                     ' and assemble when they are done arriving.')
    parser.add_argument('-d', '--data_dir', help='Base output directory', type=str,
                        default='/data/ldm/pub/native/radar/level2')
    parser.add_argument('-s', '--s3', help='Write to specified S3 bucket rather than disk.',
                        type=str)
    parser.add_argument('--sns', help='When saving products, publish to this SNS topic',
                        type=str, default='NewNEXRADLevel2ObjectFilterable')
    parser.add_argument('--s3-volume-args', help='Additional arguments to pass when sending '
                        ' volumes to S3. This is useful for passing along access controls.',
                        type=str)
    parser.add_argument('-k', '--key', help='Key format string when storing products. Uses '
                        'Python string format specification',
                        default='{0.site}/{0.volume_id}/{0.dt:%Y%m%d-%H%M%S}-'
                                '{0.product_id:03d}-{0.product_type}')
    parser.add_argument('--threads', help='Specify number of threads to use.', default=20,
                        type=int)
    parser.add_argument('-v', '--verbose', help='Make output more verbose. Can be used '
                                                'multiple times.', action='count', default=0)
    parser.add_argument('-q', '--quiet', help='Make output quieter. Can be used '
                                              'multiple times.', action='count', default=0)
    parser.add_argument('-p', '--path', help='Path format string. Uses Python '
                        'string format specification', default='{0.site}/{0.dt:%Y%m%d}')
    parser.add_argument('-n', '--filename', help='Filename format string. Uses Python '
                        'string format specification',
                        default='Level2_{0.site}_{0.dt:%Y%m%d_%H%M%S}.ar2v')
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

    # tasks = []
    #
    # # Set up storing products internally
    # product_queue = asyncio.Queue()
    # queues = [product_queue]
    #
    # # If we need to save the products to s3, set that up as well
    # if args.save_products:
    #     import boto3
    #     s3_queue = asyncio.Queue()
    #     bucket_pool = S3BucketPool(args.save_products)
    #     sns_pool = SNSBucketPool(args.sns)
    #     tasks.append(asyncio.ensure_future(write_products_s3(loop, s3_queue, bucket_pool,
    #                                                        sns_pool)))
    #     queues.append(s3_queue)

    ldm.process()

