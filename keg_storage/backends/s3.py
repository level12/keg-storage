import arrow
import boto3
from botocore.exceptions import ClientError

from .base import (
    StorageBackend,
    FileNotFoundInStorageError,
    ListEntry,
    RemoteFile,
    FileMode,
)


class S3FileBase(RemoteFile):
    def __init__(self, mode, bucket, filename, client):
        self.s3 = client
        self.bucket = bucket
        self.filename = filename
        super().__init__(mode)


class S3Reader(S3FileBase):
    def __init__(self, bucket, filename, client):
        super().__init__(FileMode.read, bucket, filename, client)
        self.reader = None

        obj = self.s3.get_object(Bucket=self.bucket, Key=self.filename)
        self.reader = obj['Body']

    def read(self, size: int):
        return self.reader.read(size)

    def close(self):
        # Reader may be None if an exception is thrown in the constructor
        if self.reader is not None:
            self.reader.close()


class S3Writer(S3FileBase):
    def __init__(self, bucket, filename, client, chunk_size=10 * 1024 * 1024):
        super().__init__(FileMode.write, bucket, filename, client)
        self.part_ids = []
        self.multipart_id = None

        self.buffer = bytearray()
        self.chunk_size = chunk_size

    def _flush_buffer(self):
        if len(self.buffer) == 0:
            return

        if self.multipart_id is None:
            self._init_multipart()

        body = self.buffer[:self.chunk_size]
        part = self.s3.upload_part(
            Bucket=self.bucket,
            Key=self.filename,
            PartNumber=len(self.part_ids) + 1,
            UploadId=self.multipart_id,
            Body=body
        )
        self.part_ids.append(part['ETag'])
        self.buffer = self.buffer[self.chunk_size:]

    def _init_multipart(self):
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.filename)
        self.multipart_id = mpu['UploadId']

    def _finalize_multipart(self):
        while len(self.buffer):
            self._flush_buffer()

        if len(self.part_ids) == 0:
            return

        part_info = {
            'Parts': [
                {
                    'PartNumber': idx,
                    'ETag': part_id,
                }
                for idx, part_id in enumerate(self.part_ids, start=1)
            ]
        }
        self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.filename,
            UploadId=self.multipart_id,
            MultipartUpload=part_info
        )
        self.part_ids = []
        self.multipart_id = None

    def close(self):
        if self.multipart_id is not None:
            self._finalize_multipart()

    def abort(self):
        if self.multipart_id is None:
            return
        self.s3.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.filename,
            UploadId=self.multipart_id,
        )
        self.part_ids = []
        self.multipart_id = None

    def write(self, data: bytes):
        self.buffer.extend(data)
        while len(self.buffer) >= self.chunk_size:
            self._flush_buffer()


class S3Storage(StorageBackend):
    def __init__(
            self,
            bucket,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_region='us-east-1',
            aws_profile=None,
            name='s3'
    ):
        super().__init__()
        self.name = name
        self.bucket = bucket
        self.session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            profile_name=aws_profile,
            region_name=aws_region
        )
        self.client = self.session.client('s3')

    def list(self, path):
        results = []

        more_results = True
        continue_token = None

        while more_results:
            resp = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=path,
                **(dict(ContinuationToken=continue_token) if continue_token is not None else {})
            )
            results.extend([
                ListEntry(
                    name=obj['Key'],
                    last_modified=arrow.get(obj['LastModified']),
                    size=obj['Size']
                )
                for obj in resp.get('Contents', [])
            ])
            more_results = resp['IsTruncated']
            continue_token = resp['NextContinuationToken'] if more_results else None

        return results

    def _create_reader(self, path):
        try:
            return S3Reader(self.bucket, path, self.client)
        except ClientError as err:
            if err.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundInStorageError(storage_type=self, filename=path)
            raise

    def _create_writer(self, path):
        return S3Writer(self.bucket, path, self.client)

    def open(self, path: str, mode: FileMode):
        if mode & FileMode.read and mode & FileMode.write:
            raise NotImplementedError('Read+write mode not supported by the S3 backend')
        elif mode & FileMode.read:
            return self._create_reader(path)
        elif mode & FileMode.write:
            return self._create_writer(path)
        else:
            raise ValueError('Unsupported mode')

    def delete(self, path):
        self.client.delete_object(
            Bucket=self.bucket,
            Key=path
        )
