import base64
import datetime
import re
import string
import urllib.parse as urlparse
from io import BytesIO
from typing import Union
from unittest import mock
import urllib.parse

import arrow
import pytest
from azure.storage.blob import BlobClient, BlobProperties, ContainerClient
from azure.storage.blob._models import BlobPrefix

from keg_storage import backends
from keg_storage.backends import base


def create_storage(**kwargs):
    return backends.AzureStorage(**{
        'account': 'foo',
        'key': base64.b64encode(b'a' * 64).decode(),
        'bucket': 'test',
        **kwargs,
    })


class TestAzureStorageBasics:
    def test_construct_incomplete(self):
        with pytest.raises(
            ValueError,
            match=(
                "Must provide a sas_container_url, a sas_blob_url, "
                "or a combination of account, key, and bucket"
            ),
        ):
            backends.AzureStorage(account="foo", bucket="test")

    @mock.patch.object(ContainerClient, "from_container_url")
    def test_construct_with_container_url(self, m_from_container_url: mock.MagicMock):
        storage = backends.AzureStorage(
            sas_container_url="https://foo.blob.core.windows.net/test?sp=rwdl"
        )
        storage.upload(BytesIO(b"hello"), "inbox/message.txt")

        m_from_container_url.return_value.get_blob_client.assert_called_once_with(
            "inbox/message.txt"
        )

    @mock.patch.object(BlobClient, "from_blob_url")
    def test_construct_with_blob_url(self, m_from_blob_url: mock.MagicMock):
        blob_name = "inbox/message.txt"
        escaped_blob_name = urllib.parse.quote("inbox/message.txt", safe="")
        sas_blob_url = f"https://foo.blob.core.windows.net/test/{escaped_blob_name}?sp=cw"

        # Set this attribute on the mock client, because it is used for path validation.
        m_from_blob_url.return_value.blob_name = blob_name

        storage = backends.AzureStorage(sas_blob_url=sas_blob_url)
        storage.upload(BytesIO(b"hello"), blob_name)

        m_from_blob_url.assert_called_once_with(sas_blob_url)

        # Make sure path validation is working.
        with pytest.raises(ValueError, match="Invalid path for the configured SAS blob URL"):
            storage.upload(BytesIO(b"hello"), "inbox/another-message.txt")

        # Make sure list operation fails.
        with pytest.raises(ValueError, match="Cannot perform list operation .* SAS blob URL"):
            storage.list("inbox/")

    def test_open_read_write(self):
        storage = create_storage()
        with pytest.raises(
            NotImplementedError,
            match=re.escape("Read+write mode not supported by the Azure backend"),
        ):
            storage.open("foo", base.FileMode.read | base.FileMode.write)

    def test_open_bad_mode(self):
        storage = create_storage()
        with pytest.raises(
            ValueError,
            match=re.escape("Unsupported mode. Accepted modes are FileMode.read or FileMode.write"),
        ):
            storage.open("foo", base.FileMode(0))


@mock.patch.object(backends.AzureStorage, '_create_blob_client')
class TestAzureStorageOperations:
    @mock.patch.object(backends.AzureStorage, '_create_container_client')
    def test_list(self, m_container_client: mock.MagicMock, m_blob_client: mock.MagicMock):
        m_walk = m_container_client.return_value.walk_blobs

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
        storage = create_storage()
        results = storage.list('xyz')

        m_walk.assert_called_once_with('xyz/')
        assert results == [
            base.ListEntry(name='abc/', last_modified=None, size=0),
            base.ListEntry(name='abc/foo.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 1), size=123),  # noqa
            base.ListEntry(name='abc/bar.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 2), size=321),  # noqa
            base.ListEntry(name='baz.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 3), size=100),
        ]

    @mock.patch.object(backends.AzureStorage, '_create_container_client')
    def test_list_leading_slashes(
        self, m_container_client: mock.MagicMock, m_blob_client: mock.MagicMock
    ):
        m_walk = m_container_client.return_value.walk_blobs
        m_walk.return_value = []
        storage = create_storage()
        storage.list('//xyz')
        m_walk.assert_called_once_with('xyz/')

    def test_read_operations(self, m_blob_client: mock.MagicMock):
        storage = create_storage(chunk_size=10)

        data = ''.join([
            string.digits,
            string.ascii_lowercase,
            string.ascii_uppercase,
        ]).encode()
        chunks = [data[i : i + 10] for i in range(0, len(data), 10)]  # noqa: E203

        m_stream = m_blob_client.return_value.download_blob
        m_stream.return_value.chunks.return_value = iter(chunks)

        with storage.open('foo', base.FileMode.read) as f:
            assert f.read(1) == b'0'
            assert f.read(2) == b'12'
            assert f.read(10) == b'3456789abc'
            assert f.read(30) == b'defghijklmnopqrstuvwxyzABCDEFG'
            assert f.read(30) == b'HIJKLMNOPQRSTUVWXYZ'

    @mock.patch('keg_storage.backends.azure.os.urandom', autospec=True, spec_set=True)
    def test_write_operations(self, m_urandom: mock.MagicMock, m_blob_client: mock.MagicMock):
        storage = create_storage(chunk_size=10)

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

        m_stage_block = m_blob_client.return_value.stage_block
        m_stage_block.side_effect = mock_stage_block

        m_commit_list = m_blob_client.return_value.commit_block_list
        m_commit_list.side_effect = mock_commit_block_list

        def block_id(index_bytes):
            return base64.b64encode(index_bytes + bytes([0] * 40)).decode()

        with storage.open('foo', base.FileMode.write) as f:
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

    def test_write_nothing(self, m_blob_client: mock.MagicMock):
        storage = create_storage()

        m_stage_block = m_blob_client.return_value.stage_block
        m_commit_list = m_blob_client.return_value.commit_block_list

        f = storage.open('foo', base.FileMode.write)
        f.close()

        m_stage_block.assert_not_called()
        m_commit_list.assert_not_called()

    def test_open_leading_slashes(self, m_blob_client: mock.MagicMock):
        storage = create_storage()
        storage.open("//xyz", "r")
        m_blob_client.assert_called_once_with("xyz")

    def test_delete(self, m_blob_client: mock.MagicMock):
        storage = create_storage()
        storage.delete("foo")

        m_blob_client.assert_called_once_with("foo")
        m_blob_client.return_value.delete_blob.assert_called_once_with()

    def test_delete_leading_slashes(self, m_blob_client: mock.MagicMock):
        storage = create_storage()
        storage.delete("/foo")

        m_blob_client.assert_called_once_with("foo")
        m_blob_client.return_value.delete_blob.assert_called_once_with()


class TestAzureStorageUtilities:
    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    def test_upload_url(self, expire: Union[arrow.Arrow, datetime.datetime]):
        storage = create_storage()
        with pytest.warns(DeprecationWarning):
            url = storage.create_upload_url(
                'abc/def.txt',
                expire=expire
            )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc%2Fdef.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['cw']
        assert qs['sig']
        assert 'sip' not in qs

    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    def test_download_url(self, expire: Union[arrow.Arrow, datetime.datetime]):
        storage = create_storage()
        with pytest.warns(DeprecationWarning):
            url = storage.create_download_url(
                'abc/def.txt',
                expire=expire
            )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc%2Fdef.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['r']
        assert qs['sig']
        assert 'sip' not in qs

    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    @pytest.mark.parametrize('ops,perms', [
        (base.ShareLinkOperation.download, 'r'),
        (base.ShareLinkOperation.upload, 'cw'),
        (base.ShareLinkOperation.remove, 'd'),
        (base.ShareLinkOperation.download | base.ShareLinkOperation.upload, 'rcw'),
        (base.ShareLinkOperation.upload | base.ShareLinkOperation.remove, 'cwd'),
    ])
    def test_link_to(self, expire: Union[arrow.Arrow, datetime.datetime],
                     ops: base.ShareLinkOperation, perms: str):
        storage = create_storage()
        url = storage.link_to(
            'abc/def.txt',
            operation=ops,
            expire=expire
        )
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test/abc%2Fdef.txt'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == [perms]
        assert qs['sig']
        assert 'sip' not in qs

    def test_sas_create_container_url(self):
        storage = backends.AzureStorage(
            **{"sas_container_url": "https://foo.blob.core.windows.net/test?sp=rwdl"}
        )

        with pytest.raises(ValueError, match="Cannot create a SAS URL without account credentials"):
            storage.create_container_url(arrow.get(2019, 1, 2, 3, 4, 5))

    @pytest.mark.parametrize('account,key', [
        (None, 'foo'),
        ('foo', None),
        (None, None),
    ])
    def test_sas_link_to(self, account, key):
        storage = backends.AzureStorage(
            account=account,
            key=key,
            sas_container_url='https://foo.blob.core.windows.net/test?sp=rwdl'
        )

        with pytest.raises(ValueError, match="Cannot create a SAS URL without account credentials"):
            storage.link_to("foo/bar.txt", backends.ShareLinkOperation.upload,
                            arrow.get(2019, 1, 2, 3, 4, 5))

    @pytest.mark.parametrize('expire', [
        arrow.get(2019, 1, 2, 3, 4, 5),
        datetime.datetime(2019, 1, 2, 3, 4, 5)
    ])
    def test_create_container_url(self, expire: Union[arrow.Arrow, datetime.datetime]):
        storage = create_storage()
        url = storage.create_container_url(expire=expire)
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['rwdl']
        assert qs['sig']
        assert 'sip' not in qs

        # with IP restriction
        url = storage.create_container_url(expire=expire, ip='127.0.0.1')
        parsed = urlparse.urlparse(url)
        assert parsed.netloc == 'foo.blob.core.windows.net'
        assert parsed.path == '/test'
        qs = urlparse.parse_qs(parsed.query)

        assert qs['se'] == ['2019-01-02T03:04:05Z']
        assert qs['sp'] == ['rwdl']
        assert qs['sig']
        assert qs['sip'] == ['127.0.0.1']
