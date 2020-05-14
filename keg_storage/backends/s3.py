import typing
from datetime import datetime

import arrow
import boto3
from botocore.exceptions import ClientError

from .base import (
    ShareLinkOperation,
    StorageBackend,
    FileNotFoundInStorageError,
    ListEntry,
    RemoteFile,
    FileMode,
)
from ..utils import expire_time_to_seconds


class S3FileBase(RemoteFile):
    """
    Read and write operations for S3 are very different so individual subclasses are used for each.
    Read+Write mode is not available for this backend.
    """
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
    """
    Writes to S3 are quite a bit more complicated than reads. To support large files, we cannot
    write in a single operation and the API does not encourage streaming writes so we make use of
    the multipart API methods.

    The process can be summarized as:
        * Create a multipart upload and get an upload key to use with subsequent calls.
        * Upload "parts" of the file using the upload key and get back an ID for each part.
        * Combine the parts using the upload key and all the part IDs from the above steps.

    The chunked nature of the uploads should be mostly invisible to the caller since S3Writer
    maintains a local buffer.

    Because creating a multipart upload itself has an actual cost and there is no guarantee that
    anything will actually be written, we initialize the multipart upload lazily.
    """
    def __init__(self, bucket, filename, client, chunk_size=10 * 1024 * 1024):
        super().__init__(FileMode.write, bucket, filename, client)
        # The upload key that we will get when we intialize the multipart upload
        self.multipart_id = None

        # The IDs for each part we upload in the order they should be combined
        self.part_ids = []

        # A local buffer to limit the number of parts we need to create
        self.buffer = bytearray()

        # The maximum size of an uploaded part
        self.chunk_size = chunk_size

    def _flush_buffer(self):
        """
        Upload the contents of the local buffer to an S3 part.
        """
        if not self.buffer:
            # If the buffer is already empty, do nothing
            return

        if self.multipart_id is None:
            # Create the multipart upload if it has not been initialized yet
            self._init_multipart()

        # Upload the first `chunk_size` bytes from the buffer to create a part
        body = self.buffer[:self.chunk_size]
        part = self.s3.upload_part(
            Bucket=self.bucket,
            Key=self.filename,
            PartNumber=len(self.part_ids) + 1,
            UploadId=self.multipart_id,
            Body=body
        )

        # Store the resulting part ID to recombine later
        self.part_ids.append(part['ETag'])

        # Cycle the buffer
        self.buffer = self.buffer[self.chunk_size:]

    def _init_multipart(self):
        """
        Create the S3 multipart upload
        """
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.filename)
        self.multipart_id = mpu['UploadId']

    def _finalize_multipart(self):
        """
        Recombine all the uploaded parts into a single S3 object.
        """
        # If no data was uploaded, we don't need to do anything else
        if not self.part_ids:
            return

        # Specify how to combine the parts. The PartNumber indicates the order and must be >= 1
        part_info = {
            'Parts': [
                {
                    'PartNumber': idx,
                    'ETag': part_id,
                }
                for idx, part_id in enumerate(self.part_ids, start=1)
            ]
        }

        # Combine the parts into a single S3 object
        self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.filename,
            UploadId=self.multipart_id,
            MultipartUpload=part_info
        )

        # Clear now invalid IDs
        self.part_ids = []
        self.multipart_id = None

    def close(self):
        # Ensure any locally buffered data is uploaded to a part
        while len(self.buffer):
            self._flush_buffer()

        # If the multipart upload was initialized, finalize it.
        if self.multipart_id is not None:
            self._finalize_multipart()

    def abort(self):
        """
        Use if for some reason you want to discard all the data written and not create an S3 object
        """
        self.buffer.clear()
        if self.multipart_id is None:
            # If the multipart upload was never initialized, do nothing
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
            # _flush_buffer uploads only one part so multiple calls may be needed for large writes
            self._flush_buffer()


class S3Storage(StorageBackend):
    def __init__(
            self,
            bucket,
            aws_region,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_profile=None,
            name='s3'
    ):
        super().__init__(name)
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
        params = {
            'Bucket': self.bucket,
            'Prefix': path,
        }

        # The list_objects_v2 endpoint may paginate the results if there are a bunch of objects
        # that match the prefix
        while more_results:
            resp = self.client.list_objects_v2(**params)
            results.extend([
                ListEntry(
                    name=obj['Key'],
                    last_modified=arrow.get(obj['LastModified']),
                    size=obj['Size']
                )
                for obj in resp.get('Contents', [])
            ])

            # When IsTruncated is true it indicates that we need to call the endpoint again with
            # the given ContinuationToken to get the next batch of object descriptions.
            # The continuation token is an opaque string that points to the next page of results.
            more_results = resp['IsTruncated']
            if more_results:
                params['ContinuationToken'] = resp['NextContinuationToken']

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

    def open(self, path: str, mode: typing.Union[FileMode, str]):
        mode = FileMode.as_mode(mode)

        if mode & FileMode.read and mode & FileMode.write:
            raise NotImplementedError('Read+write mode not supported by the S3 backend')
        elif mode & FileMode.read:
            return self._create_reader(path)
        elif mode & FileMode.write:
            return self._create_writer(path)
        else:
            raise ValueError('Unsupported mode. Accepted modes are FileMode.read or FileMode.write')

    def delete(self, path):
        self.client.delete_object(
            Bucket=self.bucket,
            Key=path
        )

    def link_to(
            self,
            path: str,
            operation: typing.Union[ShareLinkOperation, str],
            expire: typing.Union[arrow.Arrow, datetime]
    ) -> str:
        operation = ShareLinkOperation.as_operation(operation)
        if operation.name is None:
            # This is a composite of multiple values which is not supported
            raise NotImplementedError('S3 backends cannot generate a link for multiple operations')

        extra_params = {}
        if operation == ShareLinkOperation.download:
            method = 'get_object'
        elif operation == ShareLinkOperation.upload:
            method = 'put_object'
            extra_params = {'ContentType': 'application/octet-stream'}
        elif operation == ShareLinkOperation.remove:
            method = 'delete_object'
        else:  # pragma: no cover
            # This should be impossible to reach so long as all operations are covered above
            raise ValueError('Unknown operation')

        return self.client.generate_presigned_url(
            ClientMethod=method,
            ExpiresIn=int(expire_time_to_seconds(expire)),
            Params={
                'Bucket': self.bucket,
                'Key': path,
                **extra_params
            },
        )
