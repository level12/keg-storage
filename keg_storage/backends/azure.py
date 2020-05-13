import base64
import os
import typing
import urllib.parse
import warnings
from datetime import datetime
from typing import ClassVar, List, Optional

import arrow
from azure.storage.blob import (
    BlobBlock,
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContainerClient,
    generate_blob_sas,
    generate_container_sas,
)
from azure.storage.blob._models import BlobPrefix

from keg_storage.backends import base


DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024


class AzureFile(base.RemoteFile):
    """
    Base class for Azure file interface. Since read and write operations are very different and
    integrating the two would introduce a lot of complexity there are distinct subclasses for files
    opened for reading and writing.
    """

    def __init__(self, mode: base.FileMode, blob_client: BlobClient, chunk_size=DEFAULT_CHUNK_SIZE):
        """
        :param mode: file mode
        :param blob_client: blob client instance to use for API calls
        :param chunk_size: read/write buffer size
        """
        super().__init__(mode)
        self.chunk_size = chunk_size
        self.client = blob_client

        # Local buffer to reduce number of requests
        self.buffer = bytearray()


class AzureWriter(AzureFile):
    """
    We are using Azure Block Blobs for all operations. The process for writing them is substantially
    similar to that of S3 with a couple of differences.

    1. We generate the IDs for the blocks
    2. There is no separate call to instantiate the upload. The first call to put_block will create
        the blob.

    """

    max_block_size: ClassVar[int] = 100 * 1024 * 1024

    def __init__(
        self,
        mode: base.FileMode,
        blob_client: BlobClient,
        chunk_size=DEFAULT_CHUNK_SIZE,
    ):
        if chunk_size is not None:
            # chunk_size cannot be larger than max_block_size due to API restrictions
            chunk_size = min(chunk_size, self.max_block_size)
        super().__init__(
            mode=mode,
            blob_client=blob_client,
            chunk_size=chunk_size,
        )
        self.blocks: List[BlobBlock] = []

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

    def __init__(
        self,
        mode: base.FileMode,
        blob_client: BlobClient,
        chunk_size=DEFAULT_CHUNK_SIZE,
    ):
        super().__init__(
            mode=mode,
            blob_client=blob_client,
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
    account_url: Optional[str]
    container_url: Optional[str]
    blob_url: Optional[str]

    def __init__(
        self,
        account: Optional[str] = None,
        key: Optional[str] = None,
        bucket: Optional[str] = None,
        sas_container_url: Optional[str] = None,
        sas_blob_url: Optional[str] = None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        name: str = "azure",
    ):
        super().__init__(name)
        self.chunk_size = chunk_size

        self.account = account
        self.key = key
        self.bucket = bucket

        self.account_url = None
        self.container_url = None
        self.blob_url = None

        if account and key and bucket:
            self.account_url = 'https://{}.blob.core.windows.net'.format(self.account)
        elif sas_container_url:
            self.container_url = sas_container_url
        elif sas_blob_url:
            self.blob_url = sas_blob_url
        else:
            raise ValueError(
                "Must provide a sas_container_url, a sas_blob_url, "
                "or a combination of account, key, and bucket"
            )

    def _create_container_client(self) -> ContainerClient:
        """Create a ContainerClient.

        First see if a ``container_url`` was configured. Otherwise fall back to credentials.
        """

        if self.container_url:
            return ContainerClient.from_container_url(self.container_url)

        service_client = BlobServiceClient(account_url=self.account_url, credential=self.key)
        return service_client.get_container_client(self.bucket)

    def _create_blob_client(self, path: str) -> BlobClient:
        """Create a BlobClient for the given path.

        First see if a ``blob_url`` was configured. Otherwise fall back to ``container_url`` or
        credentials.
        """

        if self.blob_url:
            blob_client = BlobClient.from_blob_url(self.blob_url)
            if blob_client.blob_name != path:
                raise ValueError("Invalid path for the configured SAS blob URL")
            return blob_client

        container_client = self._create_container_client()
        return container_client.get_blob_client(path)

    def _clean_path(self, path: str):
        return path.lstrip('/')

    def list(self, path: str) -> typing.List[base.ListEntry]:
        if self.blob_url:
            raise ValueError("Cannot perform list operation when configured with SAS blob URL")
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

    def open(self, path: str, mode: typing.Union[base.FileMode, str]) -> AzureFile:
        mode = base.FileMode.as_mode(mode)

        path = self._clean_path(path)
        blob_client = self._create_blob_client(path)

        if (mode & base.FileMode.read) and (mode & base.FileMode.write):
            raise NotImplementedError('Read+write mode not supported by the Azure backend')
        elif mode & base.FileMode.write:
            return AzureWriter(mode=mode, blob_client=blob_client, chunk_size=self.chunk_size)
        elif mode & base.FileMode.read:
            return AzureReader(mode=mode, blob_client=blob_client, chunk_size=self.chunk_size)
        else:
            raise ValueError('Unsupported mode. Accepted modes are FileMode.read or FileMode.write')

    def delete(self, path: str):
        path = self._clean_path(path)
        blob_client = self._create_blob_client(path)
        blob_client.delete_blob()

    def create_upload_url(self, path: str, expire: typing.Union[arrow.Arrow, datetime]):
        """
        Create an SAS URL that can be used to upload a blob without any additional authentication.
        This url can be used in following way to authenticate a client and upload to the
        pre-specified path:

            client = BlobClient.from_blob_url(url)
            client.upload_blob(data)
        """
        warnings.warn('create_upload_url is deprecated. Use link_to instead', DeprecationWarning)
        return self.link_to(
            path=path,
            operation=base.ShareLinkOperation.upload,
            expire=expire
        )

    def create_download_url(self, path: str, expire: typing.Union[arrow.Arrow, datetime]):
        """
        Create an SAS URL that can be used to download a blob without any additional authentication.
        This url may be accessed directly to download the blob:

            requests.get(url)
        """
        warnings.warn('create_download_url is deprecated. Use link_to instead', DeprecationWarning)
        return self.link_to(
            path=path,
            operation=base.ShareLinkOperation.download,
            expire=expire
        )

    def link_to(
            self,
            path: str,
            operation: typing.Union[base.ShareLinkOperation, str],
            expire: typing.Union[arrow.Arrow, datetime],
    ) -> str:
        if not self.account_url or not self.key:
            raise ValueError('Cannot create a SAS URL without account credentials')
        path = self._clean_path(path)
        expire = expire.datetime if isinstance(expire, arrow.Arrow) else expire

        operation = base.ShareLinkOperation.as_operation(operation)
        perms = BlobSasPermissions(
            read=operation & base.ShareLinkOperation.download,
            add=False,  # only useful for append blobs that are not supported
            create=operation & base.ShareLinkOperation.upload,
            write=operation & base.ShareLinkOperation.upload,
            delete=operation & base.ShareLinkOperation.remove,
        )

        token = generate_blob_sas(
            account_name=self.account,
            container_name=self.bucket,
            blob_name=path,
            account_key=self.key,
            permission=perms,
            expiry=expire,
        )
        escaped_path = urllib.parse.quote(path, safe="")
        url = urllib.parse.urljoin(self.account_url, '{}/{}'.format(self.bucket, escaped_path))
        return '{}?{}'.format(url, token)

    def create_container_url(self, expire: typing.Union[arrow.Arrow, datetime],
                             ip: typing.Optional[str] = None):
        if not self.account_url:
            raise ValueError('Cannot create a SAS URL without account credentials')
        expire = expire.datetime if isinstance(expire, arrow.Arrow) else expire
        token = generate_container_sas(
            account_name=self.account,
            container_name=self.bucket,
            account_key=self.key,
            permission='rwdl',
            expiry=expire,
            ip=ip
        )
        url = urllib.parse.urljoin(self.account_url, self.bucket)
        return '{}?{}'.format(url, token)
