import enum
import typing

import arrow


class ListEntry(typing.NamedTuple):
    name: str
    last_modified: arrow.Arrow
    size: int


class FileMode(enum.Flag):
    read = enum.auto()
    write = enum.auto()

    def __str__(self):
        s = 'r' if self & FileMode.read else ''
        s += 'w' if self & FileMode.write else ''
        return s


class RemoteFile:
    iter_chunk_size = 5 * 1024 * 1024

    def __init__(self, mode: FileMode):
        self.mode = mode

    def read(self, size: int) -> bytes:
        raise NotImplementedError

    def write(self, data: bytes) -> None:
        raise NotImplementedError

    def close(self):
        pass

    def iter_chunks(self, chunk_size: int = None):
        chunk_size = chunk_size if chunk_size is not None else self.iter_chunk_size

        while True:
            chunk = self.read(chunk_size)
            if chunk == b'':
                break
            yield chunk

    def __iter__(self):
        return self.iter_chunks()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class StorageBackend:
    name = None

    def __init__(self, *args, **kwargs):
        pass

    def list(self, path: str) -> typing.List[ListEntry]:
        """
        Returns a list of string keys available under the directory or prefix given
        """
        raise NotImplementedError()

    def open(self, path: str, mode: FileMode) -> RemoteFile:
        raise NotImplementedError()

    def delete(self, path: str):
        raise NotImplementedError()

    def get(self, path, dest):
        with self.open(path, FileMode.read) as infile:
            with open(dest, 'wb') as outfile:
                for chunk in infile.iter_chunks():
                    outfile.write(chunk)

    def put(self, path, dest):
        buffer_size = 5 * 1024 * 1024

        with self.open(dest, FileMode.write) as outfile:
            with open(path, 'rb') as infile:
                buf = infile.read(buffer_size)
                while buf:
                    outfile.write(buf)
                    buf = infile.read(buffer_size)

    def __str__(self):
        return self.__class__.__name__


class FileNotFoundInStorageError(Exception):
    def __init__(self, storage_type, filename):
        self.storage_type = storage_type
        self.filename = filename

    def __str__(self):
        return "File {} not found in {}.".format(self.filename, str(self.storage_type))
