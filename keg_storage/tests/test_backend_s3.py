from collections import namedtuple
import tempfile

from botocore.exceptions import ClientError
import pytest
from mock import mock

import keg_storage.backends as backends
from keg_storage.backends.base import FileNotFoundInStorageError


class TestS3Storage:

    @mock.patch('keg_storage.backends.S3Storage._create_boto_session', autospec=True, spec_set=True)
    def test_init_sets_up_correctly(self, result):
        s3 = backends.S3Storage('bucket', aws_access_key_id='key', aws_secret_access_key='secret',
                                name='test')
        assert s3.name == 'test'

        s3 = backends.S3Storage('bucket', name='test-no-key')
        assert s3.name == 'test-no-key'

        s3 = backends.S3Storage('bucket', aws_profile='profile', name='test-with-profile')
        assert result.call_args[1]['profile'] == 'profile'
        assert s3.name == 'test-with-profile'

    def test_get(self, tmpdir):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        with tempfile.NamedTemporaryFile() as remote, tempfile.NamedTemporaryFile() as local:
            remote.write(b'hello')
            remote.seek(0)

            def get_object(Bucket, Key):
                return {'Body': remote}

            s3.s3.get_object = mock.MagicMock(side_effect=get_object)
            s3.get(remote.name, local.name)
            assert local.read() == b'hello'

    def test_get_error_not_found(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        # Mock an S3/botocore ClientError
        client_error = ClientError(
            error_response={
                'Error': {'Code': 'NoSuchKey'}
            },
            operation_name='foo'
        )
        s3.s3.get_object = mock.MagicMock(side_effect=client_error)

        with pytest.raises(FileNotFoundInStorageError) as exc_info:
            s3.get('bar', 'baz')

        assert str(exc_info.value.filename) == 'bar'
        assert str(exc_info.value.storage_type) == 'S3Storage'

    def test_get_error_other(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        # Mock an S3/botocore ClientError
        client_error = ClientError(
            error_response={
                'Error': {'Code': '403', 'Message': 'Permission Denied'}
            },
            operation_name='foo'
        )
        s3.s3.get_object = mock.MagicMock(side_effect=client_error)
        with pytest.raises(ClientError) as exc_info:
            s3.get('bar', 'baz')
        error = exc_info.value.response['Error']
        assert error['Code'] == '403'
        assert error['Message'] == 'Permission Denied'

    def test_delete(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        TestS3FileObject = namedtuple('TestS3FileObject', ['key'])
        file_objects = [TestS3FileObject('foo')]
        s3.list = mock.MagicMock(return_value=file_objects)
        boto_delete_response = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        s3.bucket.delete_objects = mock.MagicMock(boto_delete_response)
        s3.delete('foo')
        s3.list.assert_called_once_with('foo')
        s3.bucket.delete_objects.assert_called_once_with(Delete={'Objects': [{'Key': 'foo'}]})

    def test_delete_error_not_found(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        s3.list = mock.MagicMock(return_value=[])
        # Boto returns 200 even when no file was found to delete
        boto_delete_response = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        s3.bucket.delete_objects = mock.MagicMock(boto_delete_response)
        with pytest.raises(FileNotFoundInStorageError) as exc_info:
            s3.delete('foo')
        s3.list.assert_called_once_with('foo')
        assert exc_info.value.storage_type == s3
        assert exc_info.value.filename == 'foo'


class TestFileNotFoundException:
    def test_filenotfoundexception_init(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        fnf_exc = FileNotFoundInStorageError(s3, 'foo')
        assert fnf_exc.storage_type == s3
        assert fnf_exc.filename == 'foo'
        assert str(fnf_exc) == "File foo not found in S3Storage."
