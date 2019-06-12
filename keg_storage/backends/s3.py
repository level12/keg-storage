import io
import botocore
import boto3
import boto3.s3.transfer

from .base import StorageBackend, FileNotFoundInStorageError


class S3Storage(StorageBackend):

    def __init__(self, bucket, *args, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_region='us-east-1', aws_profile=None, name='s3', **kwargs):

        super().__init__(*args, **kwargs)
        self.name = name

        self.session = self._create_boto_session(key_id=aws_access_key_id,
                                                 secret_key=aws_secret_access_key,
                                                 profile=aws_profile,
                                                 region=aws_region)
        self.s3 = self.session.client('s3')

        self.bucket = self.session.resource('s3').Bucket(bucket)

    def _create_boto_session(self, key_id=None, secret_key=None, profile=None, region=None):
        return boto3.session.Session(aws_access_key_id=key_id,
                                     aws_secret_access_key=secret_key,
                                     profile_name=profile,
                                     region_name=region)

    def list(self, path):
        return [x.key for x in self.bucket.objects.filter(Prefix=path).all()]

    def get(self, path, dest, decrypt=True):
        try:
            resp = self.s3.get_object(
                Bucket=self.bucket.name,
                Key=path,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundInStorageError(storage_type=self, filename=path)
            raise

        stream = self.decrypt(resp['Body']) if decrypt else resp['Body']

        with open(dest, mode='wb') as fp:
            for chunk in stream:
                fp.write(chunk)

    def put(self, path, dest, encrypt=True):
        data = io.BytesIO()
        with open(path, mode='rb') as fp:
            if encrypt:
                # S3 Requires a seekable file object and encryption is, by nature, unseekable until
                # the entire thing is encrypted.

                for chunk in self.encrypt(fp):
                    data.write(chunk)
                data.seek(0)

            self.s3.put_object(
                Bucket=self.bucket.name,
                Key=dest,
                Body=fp
            )

    def delete(self, path):
        # The s3.Bucket.objectsCollection evaluates as True even if empty.
        # Extracting the filenames is the easiest way to get its length.
        filenames = [f.key for f in self.list(path)]
        if not filenames:
            raise FileNotFoundInStorageError(storage_type=self, filename=path)
        result = self.bucket.delete_objects(Delete={'Objects': [{'Key': path}]})
        return result['ResponseMetadata']['HTTPStatusCode'] == 200

    def link_for(self, path, expires):
        try:
            # Check the file exists
            resource = self.session.resource('s3')
            resource.ObjectSummary(self.bucket.name, path).size
        except botocore.exceptions.ClientError:
            raise KeyError('That object does not exists')

        params = {'Bucket': self.bucket.name, 'Key': path}
        return self.s3.generate_presigned_url(ClientMethod='get_object',
                                              ExpiresIn=expires,
                                              Params=params)
