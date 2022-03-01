import io
import os
import pathlib
from unittest import mock

import arrow
import flask
import flask_webtest
import freezegun
import pytest

from keg_storage import ShareLinkOperation, backends
from keg_storage_ta.views import create_local_storage, ObjectView, StorageLocation


class TestViewMixin:
    def setup(self):
        self.client = flask_webtest.TestApp(flask.current_app)

    def test_get_no_token(self, tmp_path: pathlib.Path):
        url = flask.url_for('public.link-view')

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)}, status=404)
        assert resp.status_code == 404

        resp = self.client.get(f'{url}?token', headers={'StorageRoot': str(tmp_path)}, status=404)
        assert resp.status_code == 404

    def test_expired_token(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)

        now = arrow.utcnow()
        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=now.shift(hours=1)
        )

        with freezegun.freeze_time(now.shift(hours=1, seconds=1).datetime):
            resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)}, status=403)
        assert resp.status_code == 403

    def test_bad_signature(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        storage.name = 'foo'

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=arrow.utcnow().shift(hours=1)
        )

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)}, status=403)
        assert resp.status_code == 403

    def test_get_operation_not_allowed(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.upload,
            expire=arrow.utcnow().shift(hours=1)
        )

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)}, status=403)
        assert resp.status_code == 403

    def test_get_success(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        storage.upload(io.BytesIO(b'foo'), 'abc.txt')
        assert tmp_path.joinpath('abc.txt').exists()

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=arrow.utcnow().shift(hours=1)
        )
        assert 'output_path' not in url

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.content_type == 'application/octet-stream'
        assert resp.body == b'foo'

    def test_get_with_output_path(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        storage.upload(io.BytesIO(b'foo'), 'abc.txt')
        assert tmp_path.joinpath('abc.txt').exists()

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=arrow.utcnow().shift(hours=1),
            output_path='myfile.txt',
        )

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.content_type == 'application/octet-stream'
        assert resp.content_disposition == 'attachment; filename=myfile.txt'
        assert resp.body == b'foo'

    @pytest.mark.parametrize('method', ['post', 'put'])
    def test_post_put_operation_not_allowed(self, tmp_path: pathlib.Path, method: str):
        storage = create_local_storage(tmp_path)

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=arrow.utcnow().shift(hours=1)
        )

        req_method = getattr(self.client, method)

        resp = req_method(url, 'foo', headers={'StorageRoot': str(tmp_path)}, status=403)
        assert resp.status_code == 403
        assert not tmp_path.joinpath('abc.txt').exists()

    @pytest.mark.parametrize('method', ['post', 'put'])
    def test_post_put_success(self, tmp_path: pathlib.Path, method):
        storage = create_local_storage(tmp_path)

        assert not tmp_path.joinpath('abc.txt').exists()

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.upload,
            expire=arrow.utcnow().shift(hours=1)
        )

        req_method = getattr(self.client, method)

        resp = req_method(url, 'foo', headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.body == b'OK'

        with open(tmp_path / 'abc.txt', 'rb') as fp:
            assert fp.read() == b'foo'

    def test_delete_operation_not_allowed(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        storage.upload(io.BytesIO(b'foo'), 'abc.txt')

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.download,
            expire=arrow.utcnow().shift(hours=1)
        )

        resp = self.client.delete(url, headers={'StorageRoot': str(tmp_path)}, status=403)
        assert resp.status_code == 403
        assert tmp_path.joinpath('abc.txt').exists()

    def test_delete_success(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        storage.upload(io.BytesIO(b'foo'), 'abc.txt')

        assert tmp_path.joinpath('abc.txt').exists()

        url = storage.link_to(
            path='abc.txt',
            operation=ShareLinkOperation.remove,
            expire=arrow.utcnow().shift(hours=1)
        )

        resp = self.client.delete(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.body == b'OK'

        assert not tmp_path.joinpath('abc.txt').exists()

    def test_multi_operation(self, tmp_path: pathlib.Path):
        storage = create_local_storage(tmp_path)
        full_path = tmp_path / 'abc.txt'

        url = storage.link_to(
            path='abc.txt',
            operation=(
                ShareLinkOperation.download | ShareLinkOperation.upload | ShareLinkOperation.remove
            ),
            expire=arrow.utcnow().shift(hours=1)
        )

        data = os.urandom(20 * 1024 * 1024)

        resp = self.client.post(url, data, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        with full_path.open('rb') as fp:
            assert fp.read() == data

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.body == data

        resp = self.client.delete(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert not full_path.exists()


class TestStorageOperations:
    def test_storage_prefix_path(self):
        assert ObjectView.storage_prefix_path(StorageLocation.folder1, 'foo.txt') == \
            'Folder-One/foo.txt'
        assert ObjectView.storage_prefix_path(StorageLocation.folder1, '.foo.txt') == \
            'Folder-One/foo.txt'

    def test_storage_get_profile(self):
        assert ObjectView.storage_get_profile('storage.s3') == \
            flask.current_app.storage.get_interface('storage.s3')

    def test_storage_get_profile_unnamed(self):
        assert ObjectView.storage_get_profile() == \
            flask.current_app.storage.get_interface('storage.s3')

    @mock.patch('keg_storage.plugin.uuid.uuid4', lambda: 'bar')
    def test_storage_generate_filename(self):
        assert ObjectView.storage_generate_filename('foo.txt') == 'bar.txt'
        assert ObjectView.storage_generate_filename('foo') == 'bar'

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_upload_file(self):
        file_obj = io.BytesIO()
        filename = ObjectView.storage_upload_file(file_obj, 'foo.txt')
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.upload.assert_called_once_with(
            file_obj,
            path=f'Folder-One/{filename}'
        )
        assert filename != 'foo.txt'
        assert filename.endswith('.txt')

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_upload_file_preserve_name(self):
        file_obj = io.BytesIO()
        filename = ObjectView.storage_upload_file(file_obj, 'foo.txt', preserve_filename=True)
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.upload.assert_called_once_with(
            file_obj,
            path=f'Folder-One/{filename}'
        )
        assert filename == 'foo.txt'

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.not-s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_upload_file_alternate_profile(self):
        file_obj = io.BytesIO()
        filename = ObjectView.storage_upload_file(
            file_obj, 'foo.txt', storage_profile='storage.not-s3'
        )
        storage = flask.current_app.storage.get_interface('storage.not-s3')
        storage.upload.assert_called_once_with(
            file_obj,
            path=f'Folder-One/{filename}'
        )

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_upload_file_alternate_location(self):
        file_obj = io.BytesIO()
        filename = ObjectView.storage_upload_file(
            file_obj, 'foo.txt', storage_location=StorageLocation.folder2
        )
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.upload.assert_called_once_with(
            file_obj,
            path=f'Folder-Two/{filename}'
        )

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    @mock.patch('keg_storage.plugin.uuid.uuid4', lambda: 'bar')
    def test_storage_upload_form_file(self):
        client = flask_webtest.TestApp(flask.current_app)
        upload_param = ('my_file', 'foo.txt', b'12345')
        resp = client.post('/object', upload_files=[upload_param])
        storage = flask.current_app.storage.get_interface('storage.s3')
        assert storage.upload.call_count == 1
        call_args = storage.upload.call_args
        assert call_args[1]['path'] == 'Folder-One/bar.txt'
        assert resp.body == b'bar.txt'

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_download_file(self):
        buffer = ObjectView.storage_download_file('foo.txt')
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.download.assert_called_once_with(
            'Folder-One/foo.txt',
            buffer,
        )

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    def test_storage_delete_file(self):
        ObjectView.storage_delete_file('foo.txt')
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.delete.assert_called_once_with(
            path='Folder-One/foo.txt',
        )

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    @mock.patch('keg_storage.plugin.uuid.uuid4', lambda: 'bar')
    def test_storage_duplicate_file(self):
        ObjectView.storage_duplicate_file('foo.txt')
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.copy.assert_called_once_with(
            'Folder-One/foo.txt',
            'Folder-One/bar.txt',
        )

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    @mock.patch('keg_storage.plugin.arrow.utcnow', lambda: arrow.get('2022-03-01 10:00:00'))
    def test_storage_get_download_link(self):
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.link_to.return_value = 'retval'
        link = ObjectView.storage_get_download_link('foo.txt', 15)
        storage.link_to.assert_called_once_with(
            'Folder-One/foo.txt',
            backends.ShareLinkOperation.download,
            arrow.get('2022-03-01 10:15:00'),
        )
        assert link == 'retval'

    @mock.patch.dict(
        'flask.current_app.storage._interfaces',
        {'storage.s3': mock.Mock(spec=backends.StorageBackend)}
    )
    @mock.patch('keg_storage.plugin.uuid.uuid4', lambda: 'bar')
    @mock.patch('keg_storage.plugin.arrow.utcnow', lambda: arrow.get('2022-03-01 10:00:00'))
    def test_storage_get_upload_link(self):
        storage = flask.current_app.storage.get_interface('storage.s3')
        storage.link_to.return_value = 'retval'
        url, filename = ObjectView.storage_get_upload_link('foo.txt', 15)
        storage.link_to.assert_called_once_with(
            'Folder-One/bar.txt',
            backends.ShareLinkOperation.upload,
            arrow.get('2022-03-01 10:15:00'),
        )
        assert url == 'retval'
        assert filename == 'bar.txt'
