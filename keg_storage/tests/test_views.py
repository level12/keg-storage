import io
import os
import pathlib

import arrow
import flask
import flask_webtest
import freezegun
import pytest

from keg_storage import ShareLinkOperation
from keg_storage_ta.views import create_local_storage


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

        resp = self.client.get(url, headers={'StorageRoot': str(tmp_path)})
        assert resp.status_code == 200
        assert resp.content_type == 'application/octet-stream'
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
