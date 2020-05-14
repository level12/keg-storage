import io
import os
import pathlib
import re
from unittest import mock
from urllib.parse import urlparse

import arrow
import click
import freezegun
import itsdangerous
import pytest

import keg_storage
from keg_storage.backends.base import (
    InternalLinkTokenData,
    ListEntry,
    FileMode,
    RemoteFile,
    ShareLinkOperation,
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


class FakeBackend(keg_storage.InternalLinksStorageBackend):
    def __init__(
            self,
            base_dir: pathlib.Path,
            secret_key: bytes = None,
            linked_endpoint: str = None
    ):
        super().__init__(secret_key=secret_key, linked_endpoint=linked_endpoint, name="fake")
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


class TestInternalLinkTokenData:
    def test_serialize(self):
        tok_data = InternalLinkTokenData(path='foo', operations=ShareLinkOperation.upload)
        assert tok_data.serialize() == {'key': 'foo', 'op': 'u'}

    def test_deserialize(self):
        tok_data = InternalLinkTokenData.deserialize({'key': 'foo', 'op': 'u'})
        assert tok_data.path == 'foo'
        assert tok_data.operations == ShareLinkOperation.upload

    def test_allowed_operations(self):
        tok_data = InternalLinkTokenData(path='foo', operations=ShareLinkOperation.download)
        assert tok_data.allow_download is True
        assert tok_data.allow_upload is False
        assert tok_data.allow_remove is False

        tok_data = InternalLinkTokenData(path='foo', operations=ShareLinkOperation.upload)
        assert tok_data.allow_download is False
        assert tok_data.allow_upload is True
        assert tok_data.allow_remove is False

        tok_data = InternalLinkTokenData(path='foo', operations=ShareLinkOperation.remove)
        assert tok_data.allow_download is False
        assert tok_data.allow_upload is False
        assert tok_data.allow_remove is True

        tok_data = InternalLinkTokenData(
            path='foo',
            operations=ShareLinkOperation.download | ShareLinkOperation.upload
        )
        assert tok_data.allow_download is True
        assert tok_data.allow_upload is True
        assert tok_data.allow_remove is False

        tok_data = InternalLinkTokenData(
            path='foo',
            operations=ShareLinkOperation.download | ShareLinkOperation.remove
        )
        assert tok_data.allow_download is True
        assert tok_data.allow_upload is False
        assert tok_data.allow_remove is True


class TestInternalLinkStorageBackend:
    def test_create_link_token_no_secret_key(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path)
        with pytest.raises(ValueError, match='Backend must be configured with secret_key .*'):
            backend.create_link_token(path='foo', operation=ShareLinkOperation.download,
                                      expire=arrow.utcnow())

    @freezegun.freeze_time('2020-04-27')
    def test_create_link_token_success(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32)
        token = backend.create_link_token(
            path='foo',
            operation=ShareLinkOperation.download,
            expire=arrow.get(2020, 4, 27, 1)
        )

        signer = itsdangerous.TimedJSONWebSignatureSerializer(
            secret_key=b'a' * 32,
            salt='fake'
        )
        body, header = signer.loads(token, salt='fake', return_header=True)
        assert body == {
            'key': 'foo',
            'op': 'd'
        }
        assert header == {
            'alg': 'HS512',
            'iat': arrow.get(2020, 4, 27).timestamp,
            'exp': arrow.get(2020, 4, 27, 1).timestamp
        }

    def test_deserialize_link_token_no_secret_key(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path)
        with pytest.raises(ValueError, match='Backend must be configured with secret_key .*'):
            backend.deserialize_link_token('foo')

    @freezegun.freeze_time('2020-04-27')
    def test_deserialize_link_token_bad_signature(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32)
        token = backend.create_link_token(path='foo', operation=ShareLinkOperation.download,
                                          expire=arrow.get(2020, 4, 27, 1))
        backend.name = 'fake1'
        with pytest.raises(itsdangerous.BadSignature, match='Signature .* does not match'):
            backend.deserialize_link_token(token)

    def test_deserialize_link_token_expired(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32)
        with freezegun.freeze_time('2020-04-26'):
            token = backend.create_link_token(path='foo', operation=ShareLinkOperation.download,
                                              expire=arrow.get(2020, 4, 26, 23, 59, 59))

        with freezegun.freeze_time('2020-04-27'):
            with pytest.raises(itsdangerous.SignatureExpired, match='Signature expired'):
                backend.deserialize_link_token(token)

    @freezegun.freeze_time('2020-04-27')
    def test_deserialize_link_token_success(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32)
        token = backend.create_link_token(
            path='foo',
            operation=ShareLinkOperation.download,
            expire=arrow.get(2020, 4, 27, 1)
        )

        result = backend.deserialize_link_token(token)
        assert result.path == 'foo'
        assert result.operations == ShareLinkOperation.download

    @freezegun.freeze_time('2020-04-27')
    def test_link_to_no_secret_key(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, linked_endpoint='aaa.xyz')
        with pytest.raises(
                ValueError,
                match='Backend must be configured with linked_endpoint and secret_key .*'
        ):
            backend.link_to(
                path='foo',
                operation=ShareLinkOperation.download,
                expire=arrow.get(2020, 4, 28)
            )

    @freezegun.freeze_time('2020-04-27')
    def test_link_to_no_linked_endpoint(self, tmp_path: pathlib.Path):
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32)
        with pytest.raises(
                ValueError,
                match='Backend must be configured with linked_endpoint and secret_key .*'
        ):
            backend.link_to(
                path='foo',
                operation=ShareLinkOperation.download,
                expire=arrow.get(2020, 4, 28)
            )

    @freezegun.freeze_time('2020-04-27')
    @mock.patch('flask.url_for', autospec=True, spec_set=True)
    def test_link_to_success(self, m_url_for, tmp_path: pathlib.Path):
        m_url_for.side_effect = 'http://localhost/storage?token={token}'.format
        backend = FakeBackend(tmp_path, secret_key=b'a' * 32, linked_endpoint='aaa.xyz')
        link = backend.link_to(
            path='foo',
            operation=ShareLinkOperation.download,
            expire=arrow.get(2020, 4, 28)
        )
        query = urlparse(link).query
        assert re.match(r'^token=[\w\-_]+\.[\w\-_]+\.[\w\-_]+$', query)


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


class TestShareLinkOperation:
    def test_str(self):
        assert str(ShareLinkOperation.download) == 'd'
        assert str(ShareLinkOperation.upload) == 'u'
        assert str(ShareLinkOperation.remove) == 'r'
        assert str(ShareLinkOperation.download | ShareLinkOperation.upload) == 'du'
        assert str(ShareLinkOperation.download | ShareLinkOperation.remove) == 'dr'
        assert str(ShareLinkOperation.upload | ShareLinkOperation.remove) == 'ur'
        assert str(
            ShareLinkOperation.download | ShareLinkOperation.upload | ShareLinkOperation.remove
        ) == 'dur'

    def test_as_operation(self):
        assert (
            ShareLinkOperation.as_operation(ShareLinkOperation.download) ==
            ShareLinkOperation.download
        )
        assert (
            ShareLinkOperation.as_operation(ShareLinkOperation.download | ShareLinkOperation.upload)
            == ShareLinkOperation.download | ShareLinkOperation.upload
        )

        assert ShareLinkOperation.as_operation('d') == ShareLinkOperation.download
        assert ShareLinkOperation.as_operation('u') == ShareLinkOperation.upload
        assert ShareLinkOperation.as_operation('r') == ShareLinkOperation.remove
        assert (
            ShareLinkOperation.as_operation('du') ==
            ShareLinkOperation.download | ShareLinkOperation.upload
        )
        assert (
            ShareLinkOperation.as_operation('ud') ==
            ShareLinkOperation.download | ShareLinkOperation.upload
        )
        assert (
            ShareLinkOperation.as_operation('dr') ==
            ShareLinkOperation.download | ShareLinkOperation.remove
        )
        assert (
            ShareLinkOperation.as_operation('rd') ==
            ShareLinkOperation.download | ShareLinkOperation.remove
        )
        assert (
            ShareLinkOperation.as_operation('ur') ==
            ShareLinkOperation.upload | ShareLinkOperation.remove
        )
        assert (
            ShareLinkOperation.as_operation('ru') ==
            ShareLinkOperation.upload | ShareLinkOperation.remove
        )
        assert (
            ShareLinkOperation.as_operation('dur') ==
            ShareLinkOperation.download | ShareLinkOperation.upload | ShareLinkOperation.remove
        )
        assert (
            ShareLinkOperation.as_operation('rud') ==
            ShareLinkOperation.download | ShareLinkOperation.upload | ShareLinkOperation.remove
        )

        with pytest.raises(
            ValueError,
            match=re.escape("as_operation() accepts only ShareLinkOperation or str arguments")
        ):
            ShareLinkOperation.as_operation(1)
