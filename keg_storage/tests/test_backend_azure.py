import base64
import datetime
import string
import urllib.parse as urlparse
from io import BytesIO

import arrow
import mock
import pytest
from azure.storage.blob import BlobProperties
from azure.storage.blob._models import BlobPrefix

from keg_storage import (
    backends,
    base,
)


@mock.patch.object(backends.AzureStorage, '_create_container_client')
class TestAzureStorage:
    def create_storage(self, **kwargs):
        return backends.AzureStorage(**{
            'account': 'foo',
            'key': base64.b64encode(b'a' * 64).decode(),
            'bucket': 'test',
            **kwargs,
        })

    def test_list(self, m_client):
        m_walk = m_client.return_value.walk_blobs

        def blob(name, last_modified, size):
            return BlobProperties(**{
                'name': name,
                'Last-Modified': last_modified,
                'Content-Length': size
            })

        prefix = BlobPrefix(prefix='abc/')

        m_walk.return_value = [
            prefix,
            blob('abc/foo.txt', datetime.datetime(2019, 10, 5, 1, 1, 1), 123),
            blob('abc/bar.txt', datetime.datetime(2019, 10, 5, 1, 1, 2), 321),
            blob('baz.txt', datetime.datetime(2019, 10, 5, 1, 1, 3), 100),
        ]
        storage = self.create_storage()
        results = storage.list('xyz')

        m_walk.assert_called_once_with('xyz/')
        assert results == [
            base.ListEntry(name='abc/', last_modified=None, size=0),
            base.ListEntry(name='abc/foo.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 1), size=123),  # noqa
            base.ListEntry(name='abc/bar.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 2), size=321),  # noqa
            base.ListEntry(name='baz.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 3), size=100),
        ]

    def test_list_leading_slashes(self, m_client):
        m_walk = m_client.return_value.walk_blobs
        m_walk.return_value = []
        storage = self.create_storage()
        storage.list('//xyz')
        m_walk.assert_called_once_with('xyz/')

    def test_open_read_write(self, m_client):
        storage = self.create_storage()
        with pytest.raises(NotImplementedError) as exc:
            storage.open('foo', base.FileMode.read | base.FileMode.write)
        assert str(exc.value) == 'Read+write mode not supported by the Azure backend'

    def test_open_bad_mode(self, m_client):
        storage = self.create_storage()
        with pytest.raises(ValueError) as exc:
            storage.open('foo', base.FileMode(0))
        assert (
            str(exc.value) == 'Unsupported mode. Accepted modes are FileMode.read or FileMode.write'
        )

    def test_read_operations(self, m_client):
        storage = self.create_storage()

        data = ''.join([
            string.digits,
            string.ascii_lowercase,
            string.ascii_uppercase,
        ]).encode()
        chunks = [data[i: i+10] for i in range(0, len(data), 10)]

        m_blob_client = m_client.return_value.get_blob_client
        m_stream = m_blob_client.return_value.download_blob
        m_stream.return_value.chunks.return_value = iter(chunks)

        with storage.open('foo', base.FileMode.read, buffer_size=10) as f:
            assert f.read(1) == b'0'
            assert f.read(2) == b'12'
            assert f.read(10) == b'3456789abc'
            assert f.read(30) == b'defghijklmnopqrstuvwxyzABCDEFG'
            assert f.read(30) == b'HIJKLMNOPQRSTUVWXYZ'

    @mock.patch('keg_storage.backends.azure.os.urandom', autospec=True, spec_set=True)
    def test_write_operations(self, m_urandom, m_client):
        storage = self.create_storage()

        m_urandom.side_effect = lambda x: b'\x00' * x

        block_data = {}
        blob = BytesIO()

        def mock_stage_block(**kwargs):
            block_id = kwargs['block_id']
            assert block_id not in block_data
            block_data[block_id] = kwargs['data']

        def mock_commit_block_list(**kwargs):
            blocks = kwargs['block_list']
            for b in blocks:
                blob.write(block_data[b.id])

        m_blob_client = m_client.return_value.get_blob_client

        m_stage_block = m_blob_client.return_value.stage_block
        m_stage_block.side_effect = mock_stage_block

        m_commit_list = m_blob_client.return_value.commit_block_list
        m_commit_list.side_effect = mock_commit_block_list

        def block_id(index_bytes):
            return base64.b64encode(index_bytes + bytes([0] * 40)).decode()

        with storage.open('foo', base.FileMode.write, buffer_size=10) as f:
            f.write(b'ab')
            m_stage_block.assert_not_called()
            m_commit_list.assert_not_called()
            assert blob.getvalue() == b''
            assert block_data == {}

            f.write(b'cdefghijklm')
            m_stage_block.assert_called_once_with(
                block_id=block_id(b'\x00\x00\x00\x00\x00\x00\x00\x00'),
                data=b'abcdefghij',
            )
            m_commit_list.assert_not_called()
            assert blob.getvalue() == b''
            assert set(block_data.values()) == {b'abcdefghij'}

            f.write(b'nopqrstuvwxyz')
            m_stage_block.assert_called_with(
                block_id=block_id(b'\x00\x00\x00\x00\x00\x00\x00\x01'),
                data=b'klmnopqrst',
            )
            m_stage_block.reset_mock()
            m_commit_list.assert_not_called()
            assert blob.getvalue() == b''
            assert set(block_data.values()) == {b'abcdefghij', b'klmnopqrst'}

            f.write(b'12')
            m_stage_block.assert_not_called()
            m_commit_list.assert_not_called()
            assert blob.getvalue() == b''
            assert set(block_data.values()) == {b'abcdefghij', b'klmnopqrst'}

        m_stage_block.assert_called_with(
            block_id=block_id(b'\x00\x00\x00\x00\x00\x00\x00\x02'),
            data=b'uvwxyz12',
        )
        m_commit_list.assert_called_once()
        _, kwargs = m_commit_list.call_args
        assert [b.id for b in kwargs['block_list']] == [
            block_id(b'\x00\x00\x00\x00\x00\x00\x00\x00'),
            block_id(b'\x00\x00\x00\x00\x00\x00\x00\x01'),
            block_id(b'\x00\x00\x00\x00\x00\x00\x00\x02')
        ]
        assert blob.getvalue() == b'abcdefghijklmnopqrstuvwxyz12'

    def test_write_nothing(self, m_client):
        storage = self.create_storage()

        m_blob_client = m_client.return_value.get_blob_client
        m_stage_block = m_blob_client.return_value.stage_block
        m_commit_list = m_blob_client.return_value.commit_block_list

        f = storage.open('foo', base.FileMode.write)
        f.close()

        m_stage_block.assert_not_called()
        m_commit_list.assert_not_called()

    def test_open_leading_slashes(self, m_client):
        m_blob_client = m_client.return_value.get_blob_client
        storage = self.create_storage()
        storage.open('//xyz', 'r')
        m_blob_client.assert_called_once_with('xyz')

    def test_delete(self, m_client):
        storage = self.create_storage()
        storage.delete('foo')

        m_client.return_value.delete_blob.assert_called_once_with('foo')

    def test_delete_leading_slashes(self, m_client):
        storage = self.create_storage()
        storage.delete('/foo')

        m_client.return_value.delete_blob.assert_called_once_with('foo')

    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    def test_upload_url(self, m_client, expire):
        storage = self.create_storage()
        url = storage.create_upload_url(
            'abc/def.txt',
            expire=expire
        )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc/def.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['c']
        assert qs['sig']
        assert 'sip' not in qs

        # with IP restriction
        url = storage.create_upload_url(
            'abc/def.txt',
            expire=expire,
            ip='127.0.0.1'
        )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc/def.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['c']
        assert qs['sig']
        assert qs['sip'] == ['127.0.0.1']

    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    def test_download_url(self, m_client, expire):
        storage = self.create_storage()
        url = storage.create_download_url(
            'abc/def.txt',
            expire=expire
        )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc/def.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['r']
        assert qs['sig']
        assert 'sip' not in qs

        # with IP restriction
        storage = self.create_storage()
        url = storage.create_download_url(
            'abc/def.txt',
            expire=expire,
            ip='127.0.0.1'
        )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc/def.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['r']
        assert qs['sig']
        assert qs['sip'] == ['127.0.0.1']
