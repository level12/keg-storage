import io
import os
import pathlib
import re

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

    def __init__(self, path: pathlib.Path, mode: FileMode):
        super().__init__(mode)
        self.file = path.open(mode=str(self.mode))

    def read(self, size):
        return self.file.read(size)

    def write(self, data):
        return self.file.write(data)

    def close(self):
        self.file.close()


class FakeBackend(keg_storage.StorageBackend):
    def __init__(self, base_dir: pathlib.Path):
        super().__init__("fake")
        self.base_dir = base_dir

    def list(self, path: str):
        results = []
        for dirent in os.scandir(self.base_dir / path):
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
        return FakeRemoteFile(self.base_dir / path, mode)

    def delete(self, path):
        path = os.path.join(self.base_dir, path)
        os.unlink(path)


class TestStorageBackend:

    def test_methods_not_implemented(self):
        interface = keg_storage.StorageBackend("incomplete")

        cases = {
            interface.list: ("path",),
            interface.delete: ("path",),
            interface.open: ("path", FileMode.read),
        }

        for method, args in cases.items():
            with pytest.raises(NotImplementedError):
                method(*args)

    def test_get(self, tmp_path: pathlib.Path):
        remote = tmp_path / 'remote'
        local = tmp_path / 'local'

        remote.mkdir()
        local.mkdir()

        data = b'a' * 15000000
        with (remote / 'input_file.txt').open('wb') as fp:
            fp.write(data)

        interface = FakeBackend(remote)

        output_path = local / 'output_file.txt'
        interface.get('input_file.txt', str(output_path))

        with output_path.open('rb') as of:
            assert of.read() == data

    def test_download(self, tmp_path: pathlib.Path):
        remote = tmp_path / "remote"

        remote.mkdir()

        data = b"a" * 15_000_000
        with (remote / "input_file.txt").open("wb") as fp:
            fp.write(data)

        buf = io.BytesIO()

        interface = FakeBackend(remote)
        interface.download("input_file.txt", buf)

        assert buf.getvalue() == data

    def test_download_progress(self, tmp_path: pathlib.Path):
        progress_updates = []

        def progress_callback(n: int) -> None:
            progress_updates.append(n)

        remote = tmp_path / "remote"
        remote.mkdir()

        data = b"a" * 15_000_000
        with (remote / "input_file.txt").open("wb") as fp:
            fp.write(data)

        buf = io.BytesIO()

        interface = FakeBackend(remote)
        interface.download("input_file.txt", buf, progress_callback=progress_callback)

        assert len(progress_updates) > 0
        assert progress_updates[-1] == len(data)

        assert buf.getvalue() == data

    def test_put(self, tmp_path: pathlib.Path):
        remote = tmp_path / 'remote'
        local = tmp_path / 'local'

        remote.mkdir()
        local.mkdir()

        data = b'a' * 15000000
        input_path = local / 'input_file.txt'
        with input_path.open('wb') as fp:
            fp.write(data)

        interface = FakeBackend(remote)
        interface.put(str(input_path), 'output_file.txt')

        with (remote / 'output_file.txt').open('rb') as of:
            assert of.read() == data

    def test_upload(self, tmp_path: pathlib.Path):
        remote = tmp_path / "remote"

        remote.mkdir()

        data = b"a" * 15_000_000
        buf = io.BytesIO(data)

        interface = FakeBackend(remote)
        interface.upload(buf, "output_file.txt")

        with (remote / "output_file.txt").open("rb") as of:
            assert of.read() == data

    def test_upload_progress(self, tmp_path: pathlib.Path):
        progress_updates = []

        def progress_callback(n: int) -> None:
            progress_updates.append(n)

        remote = tmp_path / "remote"
        remote.mkdir()

        data = b"a" * 15_000_000
        buf = io.BytesIO(data)

        interface = FakeBackend(remote)
        interface.upload(buf, "output_file.txt", progress_callback=progress_callback)

        assert len(progress_updates) > 0
        assert progress_updates[-1] == len(data)

        with (remote / "output_file.txt").open("rb") as of:
            assert of.read() == data

    def test_str(self, tmp_path: pathlib.Path):
        interface = FakeBackend(tmp_path)
        assert str(interface) == 'FakeBackend'

    def test_remote_file_iter_chunks(self, tmp_path: pathlib.Path):
        file_path = tmp_path / 'test_file.txt'
        with file_path.open('wb') as fp:
            fp.write(b"a" * 100 + b"b" * 100 + b"c" * 5)

        file = FakeRemoteFile(file_path, FileMode.read)
        chunks = list(file.iter_chunks(100))
        assert chunks == [
            b'a' * 100,
            b'b' * 100,
            b'c' * 5
        ]

    def test_remote_file_closes_on_delete(self, tmp_path: pathlib.Path):
        file_path = tmp_path / 'test_file.txt'
        file = FakeRemoteFile(file_path, FileMode.write)

        real_file = file.file
        assert real_file.closed is False

        del file
        assert real_file.closed is True

    def test_remote_file_context_manager(self, tmp_path: pathlib.Path):
        file_path = tmp_path / 'test_file.txt'
        file = FakeRemoteFile(file_path, FileMode.write)
        real_file = file.file

        with file:
            assert real_file.closed is False
        assert real_file.closed is True

    def test_remote_file_iter(self, tmp_path: pathlib.Path):
        file_path = tmp_path / 'test_file.txt'
        with file_path.open('wb') as fp:
            fp.write(b"a" * 20 + b"b" * 20 + b"c" * 20)

        file = FakeRemoteFile(file_path, FileMode.read)
        chunks = list(file)
        assert chunks == [
            b'a' * 20,
            b'b' * 20,
            b'c' * 20
        ]


class TestFileNotFoundException:
    def test_click_wrapper(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path)

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

        assert FileMode.as_mode("r") == FileMode.read
        assert FileMode.as_mode("w") == FileMode.write
        assert FileMode.as_mode("rw") == FileMode.read | FileMode.write
        assert FileMode.as_mode("wr") == FileMode.read | FileMode.write
        assert FileMode.as_mode("rb") == FileMode.read

        with pytest.raises(
            ValueError, match=re.escape("as_mode() accepts only FileMode or str arguments")
        ):
            FileMode.as_mode(1)
