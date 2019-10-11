import datetime
import string
from io import BytesIO

import arrow
import mock
import pytest
from azure.storage.blob import (
    Blob,
    BlobProperties,
    BlobPrefix,
)

from keg_storage import (
    backends,
    base,
)


@mock.patch.object(backends.AzureStorage, '_create_service')
class TestAzureStorage:
    def create_storage(self, **kwargs):
        return backends.AzureStorage(**{
            'account': 'foo',
            'key': 'bar',
            'bucket': 'test',
            **kwargs,
        })

    def test_list(self, m_service):
        m_list = m_service.return_value.list_blobs

        def blob(name, last_modified, size):
            props = BlobProperties()
            props.last_modified = last_modified
            props.content_length = size
            return Blob(name=name, props=props)

        prefix = BlobPrefix()
        prefix.name = 'abc/'

        m_list.return_value = [
            prefix,
            blob('abc/foo.txt', datetime.datetime(2019, 10, 5, 1, 1, 1), 123),
            blob('abc/bar.txt', datetime.datetime(2019, 10, 5, 1, 1, 2), 321),
            blob('baz.txt', datetime.datetime(2019, 10, 5, 1, 1, 3), 100),
        ]
        storage = self.create_storage()
        results = storage.list('xyz')

        m_list.assert_called_once_with(
            container_name='test',
            prefix='xyz/',
            delimiter='/',
        )
        assert results == [
            base.ListEntry(name='abc/', last_modified=None, size=0),
            base.ListEntry(name='abc/foo.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 1), size=123),  # noqa
            base.ListEntry(name='abc/bar.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 2), size=321),  # noqa
            base.ListEntry(name='baz.txt', last_modified=arrow.get(2019, 10, 5, 1, 1, 3), size=100),
        ]

    def test_open_read_write(self, m_service):
        storage = self.create_storage()
        with pytest.raises(NotImplementedError) as exc:
            storage.open('foo', base.FileMode.read | base.FileMode.write)
        assert str(exc.value) == 'Read+write mode not supported by the Azure backend'

    def test_open_bad_mode(self, m_service):
        class FakeFileMode:
            def __and__(self, other):
                return False

        storage = self.create_storage()
        with pytest.raises(ValueError) as exc:
            storage.open('foo', FakeFileMode())
        assert str(exc.value) == 'Unsupported mode'

    def test_read_operations(self, m_service):
        storage = self.create_storage()

        data = ''.join([
            string.digits,
            string.ascii_lowercase,
            string.ascii_uppercase,
        ]).encode()
        chunks = [data[i: i+10] for i in range(0, len(data), 10)]

        props = BlobProperties()
        props.content_length = len(data)
        m_service.return_value.get_blob_properties.return_value = Blob(props=props)

        m_get = m_service.return_value.get_blob_to_bytes
        m_get.side_effect = [Blob(content=c) for c in chunks]

        with storage.open('foo', base.FileMode.read, buffer_size=10) as f:
            assert f.blob_size == 62
            assert f.read(1) == b'0'
            m_get.assert_called_with(
                container_name='test',
                blob_name='foo',
                start_range=0,
                end_range=9,
            )

            assert f.read(2) == b'12'
            assert f.read(10) == b'3456789abc'
            m_get.assert_called_with(
                container_name='test',
                blob_name='foo',
                start_range=10,
                end_range=19,
            )
            assert f.read(30) == b'defghijklmnopqrstuvwxyzABCDEFG'
            m_get.assert_any_call(
                container_name='test',
                blob_name='foo',
                start_range=20,
                end_range=29,
            )
            m_get.assert_any_call(
                container_name='test',
                blob_name='foo',
                start_range=30,
                end_range=39,
            )
            m_get.assert_any_call(
                container_name='test',
                blob_name='foo',
                start_range=40,
                end_range=49,
            )

            assert f.read(30) == b'HIJKLMNOPQRSTUVWXYZ'
            m_get.assert_any_call(
                container_name='test',
                blob_name='foo',
                start_range=50,
                end_range=59,
            )
            m_get.assert_any_call(
                container_name='test',
                blob_name='foo',
                start_range=60,
                end_range=69,
            )

    def test_write_operations(self, m_service):
        storage = self.create_storage()

        blob = BytesIO()

        m_create = m_service.return_value.create_blob
        m_create.return_value = BlobProperties()

        m_append = m_service.return_value.append_block
        m_append.side_effect = lambda **kwargs: blob.write(kwargs['block'])

        with storage.open('foo', base.FileMode.write, buffer_size=10) as f:
            f.write(b'ab')
            m_create.assert_not_called()
            m_append.assert_not_called()
            assert blob.getvalue() == b''

            f.write(b'cdefghijklm')
            m_create.assert_called_once_with(container_name='test', blob_name='foo')
            m_create.reset_mock()
            m_append.assert_called_once_with(
                container_name='test',
                blob_name='foo',
                block=b'abcdefghij'
            )
            assert blob.getvalue() == b'abcdefghij'

            f.write(b'nopqrstuvwxyz')
            m_create.assert_not_called()
            m_append.assert_called_with(
                container_name='test',
                blob_name='foo',
                block=b'klmnopqrst'
            )
            m_append.reset_mock()
            assert blob.getvalue() == b'abcdefghijklmnopqrst'

            f.write(b'12')
            m_create.assert_not_called()
            m_append.assert_not_called()
            assert blob.getvalue() == b'abcdefghijklmnopqrst'

        m_append.assert_called_with(
            container_name='test',
            blob_name='foo',
            block=b'uvwxyz12'
        )
        assert blob.getvalue() == b'abcdefghijklmnopqrstuvwxyz12'

    def test_write_nothing(self, m_service):
        storage = self.create_storage()
        m_create = m_service.return_value.create_blob
        m_append = m_service.return_value.append_block

        f = storage.open('foo', base.FileMode.write)
        f.close()

        m_create.assert_not_called()
        m_append.assert_not_called()

    def test_delete(self, m_service):
        storage = self.create_storage()
        storage.delete('foo')

        m_service.return_value.delete_blob.assert_called_once_with(
            container_name='test',
            blob_name='foo'
        )
