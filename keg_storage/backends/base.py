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
        return f'{s}b'

    @classmethod
    def as_mode(cls, obj):
        if isinstance(obj, cls):
            return obj
        if not isinstance(obj, str):
            raise ValueError('as_mode() accepts only FileMode or str arguments')

        mode = cls(0)
        if 'r' in obj:
            mode |= cls.read
        if 'w' in obj:
            mode |= cls.write
        return mode


class RemoteFile:
    """
    This is a base class for objects returned by a backend's `open()` method. This is a file-like
    object that provides read/write operations to the remote file. When creating a new backend, you
    should subclass this and implement `read()` and `write()` methods at minimum.

    After construction, a RemoteFile is presumed to be in an "open" state and should accept calls
    to any of its methods. Any cleanup should be done in the `close()` method.
    """

    # This is the default chunk size to use when iterating over this object
    iter_chunk_size = 5 * 1024 * 1024

    def __init__(self, mode: FileMode):
        """
        Override this constructor to accept any additional arguments needed by the backend and to
        perform any initialization required to get the file into an "open" state.
        """
        self.mode = mode

    def read(self, size: int) -> bytes:
        """
        Read and return up to `size` bytes from the remote file. If the end of the file is reached
        this should return an empty bytes string.
        """
        raise NotImplementedError

    def write(self, data: bytes) -> None:
        """
        Write the data buffer to the remote file.
        """
        raise NotImplementedError

    def close(self):
        """
        Cleanup and deallocate any held resources. This method may be called multiple times on a
        single instance. If the file was already closed, this method should do nothing.
        """
        pass

    def iter_chunks(self, chunk_size: int = None):
        """
        Iterate over the file in blocks of `chunk_size`.
        """
        chunk_size = chunk_size or self.iter_chunk_size

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
        Returns a list of `ListEntry`s representing files available under the directory or prefix
        given in `path`.
        """
        raise NotImplementedError()

    def open(self, path: str, mode: typing.Union[FileMode, str]) -> RemoteFile:
        """
        Returns a instance of RemoteFile for the given `path` that can be used for
        reading and/or writing depending on the `mode` given.
        """
        raise NotImplementedError()

    def delete(self, path: str):
        """
        Delete the remote file specified by `path`.
        """
        raise NotImplementedError()

    def get(self, path: str, dest: str) -> None:
        """
        Copies a remote file at `path` to the `dest` path given on the local filesystem.
        """
        with self.open(path, FileMode.read) as infile, open(dest, str(FileMode.write)) as outfile:
            for chunk in infile.iter_chunks():
                outfile.write(chunk)

    def put(self, path: str, dest: str) -> None:
        """
        Copies a local file at `path` to a remote file at `dest`.
        """
        buffer_size = 5 * 1024 * 1024

        with self.open(dest, FileMode.write) as outfile, open(path, str(FileMode.read)) as infile:
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
