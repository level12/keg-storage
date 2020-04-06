import enum
import typing

import arrow


ProgressCallback = typing.Callable[[int], None]


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
    def as_mode(cls, obj) -> "FileMode":
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
    def __init__(self, name: str):
        self.name = name

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
        with open(dest, str(FileMode.write)) as outfile:
            self.download(path, outfile)

    def download(
        self,
        path: str,
        file_obj: typing.IO,
        *,
        progress_callback: typing.Optional[ProgressCallback] = None
    ):
        """
        Copies a remote file at `path` to a file-like object `file_obj`.

        If desired, a progress callback can be supplied. The function should accept an int
        parameter, which will be the number of bytes downloaded so far.
        """
        bytes_read = 0

        with self.open(path, FileMode.read) as infile:
            for chunk in infile.iter_chunks():
                file_obj.write(chunk)
                bytes_read += len(chunk)
                if progress_callback:
                    progress_callback(bytes_read)

    def put(self, path: str, dest: str) -> None:
        """
        Copies a local file at `path` to a remote file at `dest`.
        """
        with open(path, str(FileMode.read)) as infile:
            self.upload(infile, dest)

    def upload(
        self,
        file_obj: typing.IO,
        path: str,
        *,
        progress_callback: typing.Optional[ProgressCallback] = None
    ):
        """
        Copies the contents of a file-like object `file_obj` to a remote file at `path`

        If desired, a progress callback can be supplied. The function should accept an int
        parameter, which will be the number of bytes uploaded so far.
        """
        bytes_written = 0
        buffer_size = 5 * 1024 * 1024

        with self.open(path, FileMode.write) as outfile:
            buf = file_obj.read(buffer_size)
            while buf:
                outfile.write(buf)
                bytes_written += len(buf)
                if progress_callback:
                    progress_callback(bytes_written)
                buf = file_obj.read(buffer_size)

    def __str__(self):
        return self.__class__.__name__


class FileNotFoundInStorageError(Exception):
    def __init__(self, storage_type, filename):
        self.storage_type = storage_type
        self.filename = filename

    def __str__(self):
        return "File {} not found in {}.".format(self.filename, str(self.storage_type))
