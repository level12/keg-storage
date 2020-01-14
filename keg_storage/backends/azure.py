import base64
import os
import typing
import urllib.parse
from datetime import datetime

import arrow
from azure.storage.blob import (
    BlobServiceClient,
    BlobBlock,
    ContainerClient,
    generate_blob_sas,
)
from azure.storage.blob._models import BlobPrefix

from keg_storage.backends import base


class AzureFile(base.RemoteFile):
    """
    Base class for Azure file interface. Since read and write operations are very different and
    integrating the two would introduce a lot of complexity there are distinct subclasses for files
    opened for reading and writing.
    """
    def __init__(self, path: str, mode: base.FileMode, container_client: ContainerClient,
                 chunk_size: int = 10 * 1024 * 1024):
        """
        :param path: blob name
        :param mode: file mode
        :param container_client: container client instance to use for API calls
        :param chunk_size: read/write buffer size
        """
        super().__init__(mode)
        self.chunk_size = chunk_size
        self.path = path
        self.client = container_client.get_blob_client(self.path)

        # Local buffer to reduce number of requests
        self.buffer = bytearray()


class AzureWriter(AzureFile):
    """
    We are using Azure Block Blobs for all operations. The process for writing them is substantially
    similar to that of S3 with a couple of differences.
        1. We generate the IDs for the blocks
        2. There is no separate call to instantiate the upload. The first call to put_block will
           create the blob.
    """
    def __init__(self, path: str, mode: base.FileMode, container_client: ContainerClient,
                 chunk_size: int = None):
        max_block_size = 100 * 1024 * 1024
        if chunk_size is not None:
            # chunk_size cannot be larger than max_block_size due to API restrictions
            chunk_size = min(chunk_size, max_block_size)
        super().__init__(
            path=path,
            mode=mode,
            container_client=container_client,
            chunk_size=chunk_size,
        )
        self.blocks = []

    def _gen_block_id(self) -> str:
        """
        Generate a unique ID for the block. This is meant to be opaque but it is generated from:
            1. The index of the block as an 64 bit unsigned big endian integer
            2. 40 bytes of random data
        The two parts are concatenated and base64 encoded giving us 64 bytes which is the maximum
        Azure allows.
        """
        index_part = len(self.blocks).to_bytes(8, byteorder='big', signed=False)
        random_part = os.urandom(40)
        return base64.b64encode(index_part + random_part).decode()

    def _flush(self):
        if len(self.buffer) == 0:
            # If there is no buffered data, we don't need to do anything
            return

        # Upload at most chunk_size bytes to a new block
        block_id = self._gen_block_id()
        self.client.stage_block(block_id=block_id, data=bytes(self.buffer[:self.chunk_size]))

        # Store the block_id to later concatenate when we close this file
        self.blocks.append(BlobBlock(block_id=block_id))

        # Cycle the buffer
        self.buffer = self.buffer[self.chunk_size:]

    def _finalize(self):
        self.client.commit_block_list(block_list=self.blocks)
        self.blocks = []

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)
        while len(self.buffer) >= self.chunk_size:
            # Write may be bigger than the chunk size and _flush() only uploads a single chunk so
            # repeated calls may be necessary
            self._flush()

    def close(self):
        self._flush()
        if self.blocks:
            # If we haven't created any blocks, we don't need to finalize
            self._finalize()


class AzureReader(AzureFile):
    """
    The Azure reader uses byte ranged API calls to fill a local buffer to avoid lots of API overhead
    for small read sizes.
    """
    def __init__(self, path: str, mode: base.FileMode, container_client: ContainerClient,
                 chunk_size=10 * 1024 * 1024):
        super().__init__(
            path=path,
            mode=mode,
            container_client=container_client,
            chunk_size=chunk_size,
        )
        self.stream = self.client.download_blob()
        self.chunks = self.stream.chunks()

    def _read_from_buffer(self, max_size):
        """
        Read up to max_size bytes from the local buffer.
        """
        read_size = min(len(self.buffer), max_size)
        output = self.buffer[:read_size]
        self.buffer = self.buffer[read_size:]
        return output

    def read(self, size: int) -> bytes:
        output_buf = bytes()

        while len(output_buf) < size:
            if len(self.buffer) == 0:
                try:
                    # Load the next chunk into the local buffer
                    next_chunk = next(self.chunks)
                    self.buffer.extend(next_chunk)
                except StopIteration:
                    # All chunks have been consumed
                    break

            read_remainder = size - len(output_buf)
            output_buf += self._read_from_buffer(read_remainder)

        return output_buf


class AzureStorage(base.StorageBackend):
    def __init__(self, account: str, key: str, bucket: str, name: str = 'azure'):
        super().__init__()
        self.name = name
        self.account = account
        self.key = key
        self.bucket = bucket
        self.account_url = 'https://{}.blob.core.windows.net'.format(self.account)

    def _create_service_client(self):
        return BlobServiceClient(
            account_url=self.account_url,
            credential=self.key,
        )

    def _create_container_client(self):
        service_client = self._create_service_client()
        return service_client.get_container_client(self.bucket)

    def _clean_path(self, path: str):
        return path.lstrip('/')

    def list(self, path: str) -> typing.List[base.ListEntry]:
        client = self._create_container_client()

        if not path.endswith('/'):
            path = path + '/'
        path = self._clean_path(path)
        list_iter = client.walk_blobs(path)

        def construct_entry(blob):
            if isinstance(blob, BlobPrefix):
                return base.ListEntry(name=blob.name, last_modified=None, size=0)
            return base.ListEntry(
                name=blob.name,
                last_modified=arrow.get(blob.last_modified),
                size=blob.size,
            )

        return [construct_entry(blob) for blob in list_iter]

    def open(self, path: str, mode: typing.Union[base.FileMode, str],
             buffer_size: int = 10 * 1024 * 1024) -> AzureFile:  # noqa
        mode = base.FileMode.as_mode(mode)

        path = self._clean_path(path)

        if (mode & base.FileMode.read) and (mode & base.FileMode.write):
            raise NotImplementedError('Read+write mode not supported by the Azure backend')
        elif mode & base.FileMode.write:
            return AzureWriter(
                path=path,
                mode=mode,
                container_client=self._create_container_client(),
                chunk_size=buffer_size
            )
        elif mode & base.FileMode.read:
            return AzureReader(
                path=path,
                mode=mode,
                container_client=self._create_container_client(),
                chunk_size=buffer_size
            )
        else:
            raise ValueError('Unsupported mode. Accepted modes are FileMode.read or FileMode.write')

    def delete(self, path: str):
        path = self._clean_path(path)
        container_client = self._create_container_client()
        container_client.delete_blob(path)

    def create_upload_url(self, path: str, expire: typing.Union[arrow.Arrow, datetime],
                          ip: typing.Optional[str] = None):
        """
        Create an SAS URL that can be used to upload a blob without any additional authentication.
        This url can be used in following way to authenticate a client and upload to the
        pre-specified path:

            client = BlobClient.from_blob_url(url)
            client.upload_blob(data)
        """
        return self._create_sas_url(
            path=path,
            sas_permissions='c',
            expire=expire,
            ip=ip,
        )

    def create_download_url(self, path: str, expire: typing.Union[arrow.Arrow, datetime],
                            ip: typing.Optional[str] = None):
        """
        Create an SAS URL that can be used to download a blob without any additional authentication.
        This url may be accessed directly to download the blob:

            requests.get(url)
        """
        return self._create_sas_url(
            path=path,
            sas_permissions='r',
            expire=expire,
            ip=ip,
        )

    def _create_sas_url(self, path: str, sas_permissions: str,
                        expire: typing.Union[arrow.Arrow, datetime],
                        ip: typing.Optional[str] = None):
        path = self._clean_path(path)
        expire = expire.datetime if isinstance(expire, arrow.Arrow) else expire
        token = generate_blob_sas(
            account_name=self.account,
            container_name=self.bucket,
            blob_name=path,
            account_key=self.key,
            permission=sas_permissions,
            expiry=expire,
            ip=ip
        )
        url = urllib.parse.urljoin(self.account_url, '{}/{}'.format(self.bucket, path))
        return '{}?{}'.format(url, token)
