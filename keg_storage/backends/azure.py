import base64
import os
import typing

import arrow
from azure.storage.blob import (
    BlockBlobService,
    BlobPrefix,
    BlobBlock,
)

from keg_storage.backends import base


class AzureFile(base.RemoteFile):
    """
    Base class for Azure file interface. Since read and write operations are very different and
    integrating the two would introduce a lot of complexity there are distinct subclasses for files
    opened for reading and writing.
    """
    def __init__(self, container_name: str, path: str, mode: base.FileMode,
                 service: BlockBlobService, chunk_size: int = 10 * 1024 * 1024):
        """
        :param container_name: container / bucket name
        :param path: blob name
        :param mode: file mode
        :param service: blob service instance to use for API calls
        :param chunk_size: read/write buffer size
        """
        super().__init__(mode)
        self.chunk_size = chunk_size
        self.container_name = container_name
        self.path = path
        self.service = service

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
    def __init__(self, container_name: str, path: str, mode: base.FileMode,
                 service: BlockBlobService, chunk_size: int = BlockBlobService.MAX_BLOCK_SIZE):
        super().__init__(
            container_name=container_name,
            path=path,
            mode=mode,
            service=service,
            # chunk_size cannot be larger than MAX_BLOCK_SIZE due to API restrictions
            chunk_size=min(chunk_size, BlockBlobService.MAX_BLOCK_SIZE),
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
        self.service.put_block(
            container_name=self.container_name,
            blob_name=self.path,
            block=bytes(self.buffer[:self.chunk_size]),
            block_id=block_id
        )

        # Store the block_id to later concatenate when we close this file
        self.blocks.append(BlobBlock(id=block_id))

        # Cycle the buffer
        self.buffer = self.buffer[self.chunk_size:]

    def _finalize(self):
        self.service.put_block_list(
            container_name=self.container_name,
            blob_name=self.path,
            block_list=self.blocks
        )
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
    def __init__(self, container_name: str, path: str, mode: base.FileMode,
                 service: BlockBlobService, chunk_size=10 * 1024 * 1024):
        super().__init__(
            container_name=container_name,
            path=path,
            mode=mode,
            service=service,
            chunk_size=chunk_size,
        )
        # Get overall blob size to avoid out of bounds requests
        self.blob_size = self._get_blob_size()

        # Current offset for the next request to fill the local buffer
        self.current_offset = 0

        # Current read position in the blob including reads filled from the local buffer
        self.read_position = 0

    def _get_blob_size(self):
        """
        Retrieve the overall size of the blob
        """
        blob = self.service.get_blob_properties(
            container_name=self.container_name,
            blob_name=self.path
        )
        return blob.properties.content_length

    def _refill_buffer(self):
        """
        Retrieve the next chunk and add it to the local buffer.
        """
        range_end = self.current_offset + self.chunk_size - 1  # -1 because range is inclusive
        blob = self.service.get_blob_to_bytes(
            container_name=self.container_name,
            blob_name=self.path,
            start_range=self.current_offset,
            end_range=range_end,
        )
        self.current_offset += len(blob.content)
        self.buffer.extend(blob.content)

    def _read_from_buffer(self, max_size):
        """
        Read up to max_size bytes from the local buffer.
        """
        read_size = min(len(self.buffer), max_size)
        output = self.buffer[:read_size]
        self.buffer = self.buffer[read_size:]
        self.read_position += len(output)
        return output

    def read(self, size: int) -> bytes:
        output_buf = bytes()

        # Read up to the end of the file or `size` bytes whichever is less
        read_size = min(self.blob_size - self.read_position, size)

        while len(output_buf) < read_size:
            if len(self.buffer) == 0:
                # Buffer is empty, refill it.
                self._refill_buffer()

            remaining = read_size - len(output_buf)
            read = self._read_from_buffer(remaining)
            output_buf += read

        return output_buf


class AzureStorage(base.StorageBackend):
    def __init__(self, account: str, key: str, bucket: str, name: str = 'azure'):
        super().__init__()
        self.name = name
        self.account = account
        self.key = key
        self.bucket = bucket

    def _create_service(self):
        return BlockBlobService(
            account_name=self.account,
            account_key=self.key,
        )

    def list(self, path: str) -> typing.List[base.ListEntry]:
        service = self._create_service()
        if not path.endswith('/'):
            path = path + '/'
        list_iter = service.list_blobs(container_name=self.bucket, prefix=path, delimiter='/')

        def construct_entry(blob):
            if isinstance(blob, BlobPrefix):
                return base.ListEntry(name=blob.name, last_modified=None, size=0)
            return base.ListEntry(
                name=blob.name,
                last_modified=arrow.get(blob.properties.last_modified),
                size=blob.properties.content_length,
            )

        return [construct_entry(blob) for blob in list_iter]

    def open(self, path: str, mode: typing.Union[base.FileMode, str],
             buffer_size: int = 10 * 1024 * 1024) -> AzureFile:  # noqa
        mode = base.FileMode.as_mode(mode)

        if (mode & base.FileMode.read) and (mode & base.FileMode.write):
            raise NotImplementedError('Read+write mode not supported by the Azure backend')
        elif mode & base.FileMode.write:
            return AzureWriter(
                container_name=self.bucket,
                path=path,
                mode=mode,
                service=self._create_service(),
                chunk_size=buffer_size
            )
        elif mode & base.FileMode.read:
            return AzureReader(
                container_name=self.bucket,
                path=path,
                mode=mode,
                service=self._create_service(),
                chunk_size=buffer_size
            )
        else:
            raise ValueError('Unsupported mode. Accepted modes are FileMode.read or FileMode.write')

    def delete(self, path: str):
        service = self._create_service()
        service.delete_blob(
            container_name=self.bucket,
            blob_name=path
        )
