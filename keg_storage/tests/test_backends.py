import os

import arrow
import click
import pytest

import keg_storage
from keg_storage.backends.base import (
    ListEntry,
    FileMode,
    RemoteFile,
)
from keg_storage.cli import handle_not_found


class FakeRemoteFile(RemoteFile):
    iter_chunk_size = 20

    def __init__(self, path, mode):
        super().__init__(mode)
        self.file = open(path, str(self.mode))

    def read(self, size):
        return self.file.read(size)

    def write(self, data):
        return self.file.write(data)

    def close(self):
        self.file.close()


class FakeBackend(keg_storage.StorageBackend):
    def __init__(self, dir):
        super().__init__()
        self.base_dir = dir

    def list(self, path):
        results = []
        path = os.path.join(self.base_dir, path)
        for dirent in os.scandir(path):
            stat = dirent.stat()
            results.append(
                ListEntry(
                    name=dirent.name,
                    last_modified=arrow.get(stat.st_mtime),
                    size=stat.st_size
                )
            )
        return results

    def open(self, path, mode):
        path = os.path.join(self.base_dir, path)
        return FakeRemoteFile(path, mode)

    def delete(self, path):
        path = os.path.join(self.base_dir, path)
        os.unlink(path)


class TestStorageBackend:

    def test_methods_not_implemented(self):

        interface = keg_storage.StorageBackend()

        cases = {
            interface.list: ('path',),
            interface.delete: ('path',),
            interface.open: ('path', FileMode.read),
        }

        for method, args in cases.items():
            with pytest.raises(NotImplementedError):
                method(*args)

    def test_get(self, tmp_path):
        remote = tmp_path / 'remote'
        local = tmp_path / 'local'

        remote.mkdir()
        local.mkdir()

        data = b'a' * 15000000
        with (remote / 'input_file.txt').open('wb') as fp:
            fp.write(data)

        interface = FakeBackend(str(remote))

        output_path = local / 'output_file.txt'
        interface.get('input_file.txt', str(output_path))

        with output_path.open('rb') as of:
            assert of.read() == data

    def test_put(self, tmp_path):
        remote = tmp_path / 'remote'
        local = tmp_path / 'local'

        remote.mkdir()
        local.mkdir()

        data = b'a' * 15000000
        input_path = local / 'input_file.txt'
        with input_path.open('wb') as fp:
            fp.write(data)

        interface = FakeBackend(str(remote))
        interface.put(str(input_path), 'output_file.txt')

        with (remote / 'output_file.txt').open('rb') as of:
            assert of.read() == data

    def test_str(self, tmp_path):
        interface = FakeBackend(str(tmp_path))
        assert str(interface) == 'FakeBackend'

    def test_remote_file_iter_chunks(self, tmp_path):
        file_path = tmp_path / 'test_file.txt'
        with file_path.open('wb') as fp:
            fp.write(
                b'a' * 100 +
                b'b' * 100 +
                b'c' * 5
            )

        file = FakeRemoteFile(str(file_path), FileMode.read)
        chunks = list(file.iter_chunks(100))
        assert chunks == [
            b'a' * 100,
            b'b' * 100,
            b'c' * 5
        ]

    def test_remote_file_closes_on_delete(self, tmp_path):
        file_path = tmp_path / 'test_file.txt'
        file = FakeRemoteFile(str(file_path), FileMode.write)

        real_file = file.file
        assert real_file.closed is False

        del file
        assert real_file.closed is True

    def test_remote_file_context_manager(self, tmp_path):
        file_path = tmp_path / 'test_file.txt'
        file = FakeRemoteFile(str(file_path), FileMode.write)
        real_file = file.file

        with file:
            assert real_file.closed is False
        assert real_file.closed is True

    def test_remote_file_iter(self, tmp_path):
        file_path = tmp_path / 'test_file.txt'
        with file_path.open('wb') as fp:
            fp.write(
                b'a' * 20 +
                b'b' * 20 +
                b'c' * 20
            )

        file = FakeRemoteFile(str(file_path), FileMode.read)
        chunks = list(file)
        assert chunks == [
            b'a' * 20,
            b'b' * 20,
            b'c' * 20
        ]


class TestFileNotFoundException:
    def test_click_wrapper(self, tmp_path):
        backend = FakeBackend(str(tmp_path))

        @handle_not_found
        def test_func():
            raise keg_storage.FileNotFoundInStorageError(backend, 'foo')

        with pytest.raises(click.FileError) as exc_info:
            test_func()
        assert exc_info.value.filename == 'foo'
        assert exc_info.value.message == 'Not found in FakeBackend.'


class TestFileMode:
    def test_str(self):
        assert str(FileMode.read) == 'rb'
        assert str(FileMode.write) == 'wb'
        assert str(FileMode.read | FileMode.write) == 'rwb'

    def test_as_mode(self):
        assert FileMode.as_mode(FileMode.read) == FileMode.read

        assert FileMode.as_mode('r') == FileMode.read
        assert FileMode.as_mode('w') == FileMode.write
        assert FileMode.as_mode('rw') == FileMode.read | FileMode.write
        assert FileMode.as_mode('wr') == FileMode.read | FileMode.write
        assert FileMode.as_mode('rb') == FileMode.read

        with pytest.raises(ValueError) as exc:
            FileMode.as_mode(1)
        assert str(exc.value) == 'as_mode() accepts only FileMode or str arguments'
