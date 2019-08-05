import arrow
import botocore
import boto3
import boto3.s3.transfer

from .base import (
    StorageBackend,
    FileNotFoundInStorageError,
    ListEntry,
)


class S3Storage(StorageBackend):

    def __init__(self, bucket, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_region='us-east-1', aws_profile=None, name='s3'):
        self.name = name

        self.session = self._create_boto_session(key_id=aws_access_key_id,
                                                 secret_key=aws_secret_access_key,
                                                 profile=aws_profile,
                                                 region=aws_region)
        self.s3 = self.session.client('s3')

        self.bucket = self.session.resource('s3').Bucket(bucket)
        self.transfer = boto3.s3.transfer.S3Transfer(self.s3)

    def _create_boto_session(self, key_id=None, secret_key=None, profile=None, region=None):
        return boto3.session.Session(aws_access_key_id=key_id,
                                     aws_secret_access_key=secret_key,
                                     profile_name=profile,
                                     region_name=region)

    def list(self, path):
        return [
            ListEntry(name=x.key, last_modified=arrow.get(x.last_modified), size=x.size)
            for x in self.bucket.objects.filter(Prefix=path).all()
        ]

    def get(self, path, dest):
        try:
            self.transfer.download_file(self.bucket.name, path, dest)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                raise FileNotFoundInStorageError(storage_type=self, filename=path)
            raise e

    def put(self, path, dest):
        return self.transfer.upload_file(path, self.bucket.name, dest)

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
