import logging

from aws import S3Reader
from ldm_async import set_log_level, setup_arg_parser, Product
from upload_nids import UploadNIDS

logger = logging.getLogger('alchemy')


class RenameNIDS(UploadNIDS):
    def __init__(self, name='ReadProduct'):
        super().__init__(name)

    def run(self, item):
        if 'Level3' not in item.key:
            logger.debug('Skipping %s', item.key)
            return

        orig_data = item.get()['Body'].read()
        stid, prod, *_ = item.key.split('/')

        mock_prod = Product('A B C --' + prod + stid, orig_data)
        logger.info('Renaming %s', item.key)
        super().run(mock_prod)

        # If uploading fails in super().run, then we won't run delete due to exception.
        item.delete()


if __name__ == '__main__':
    parser = setup_arg_parser('Upload products from LDM to AWS S3.')
    parser.add_argument('-b', '--bucket', help='Bucket to upload files to', type=str,
                        default='unidata-nexrad-level3')
    args = parser.parse_args()
    logger.addHandler(logging.StreamHandler())
    set_log_level(args)

    reader = S3Reader(bucket=args.bucket, nthreads=args.threads,
                      Marker='MAF/', count=2)
    reader.connect(RenameNIDS(args.bucket))
    reader.run()
