import base64
from datetime import datetime
import glob
from hashlib import md5
from pathlib import Path

import boto3
import botocore

if __name__ == '__main__':
    l2bucket = boto3.session.Session().resource('s3').Bucket('noaa-nexrad-level2')
    for fpath in glob.iglob('/data/level2/*/*/*.ar2v'):
        local_path = Path(fpath)
        #print('Checking:', local_path)
        _, stid, date, time = local_path.stem.split('_')
        dt = datetime.strptime(date + time, '%Y%m%d%H%M%S')
        testobj = next(iter(l2bucket.objects.filter(Prefix='{0:%Y/%m/%d/}'.format(dt) + stid)))

        _, ver = Path(testobj.key).stem.rsplit('_', maxsplit=1)
        key = '{0:%Y}/{0:%m}/{0:%d}/{1}/{1}{0:%Y%m%d_%H%M%S}_{2}'.format(dt, stid, ver)
        obj = l2bucket.Object(key)

        with local_path.open('rb') as fobj:
            local_bytes = fobj.read()
            local_hash = md5(local_bytes).hexdigest()
            try:
                remote_hdr = obj.get()
                need_upload = local_hash != remote_hdr['ETag'] and len(local_bytes) > remote_hdr['ContentLength']
            except botocore.exceptions.ClientError:
                need_upload = True

            if need_upload:
                print('Uploading to:', obj.key)
                obj.put(Body=local_bytes, ContentMD5=base64.b64encode(md5(local_bytes).digest()).decode('ascii'),
                        GrantRead='URI=http://acs.amazonaws.com/groups/global/AllUsers',
                        GrantFullControl='emailaddress=aws-pds-noaa-bdp-nexrad@amazon.com')
