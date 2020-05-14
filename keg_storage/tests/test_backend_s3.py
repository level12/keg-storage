import datetime
import io
import re
from unittest import mock

import arrow
import freezegun
import pytest
from botocore.exceptions import ClientError

import keg_storage.backends as backends
from keg_storage.backends.base import (
    FileMode,
    FileNotFoundInStorageError,
    ListEntry,
    ShareLinkOperation,
)


@mock.patch('keg_storage.backends.s3.boto3', autospec=True, spec_set=True)
class TestS3Storage:
    def test_init_sets_up_correctly(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1', aws_access_key_id='key',
                                aws_secret_access_key='secret', name='test')
        assert s3.name == 'test'
        assert s3.bucket == 'bucket'

        m_boto.session.Session.assert_called_once_with(
            aws_access_key_id='key',
            aws_secret_access_key='secret',
            profile_name=None,
            region_name='us-east-1'
        )

    def test_list(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')
        s3.client.list_objects_v2.return_value = {
            'IsTruncated': False,
            'Contents': [
                {
                    'Key': 'file-1.wps',
                    'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 1),
                    'Size': 10 * 1024
                },
                {
                    'Key': 'file-2.rm',
                    'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 2),
                    'Size': 20 * 1024
                },
            ]
        }

        results = s3.list('foo/bar')

        s3.client.list_objects_v2.assert_called_once_with(
            Bucket='bucket',
            Prefix='foo/bar'
        )

        assert results == [
            ListEntry(
                name='file-1.wps',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 1),
                size=10240
            ),
            ListEntry(
                name='file-2.rm',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 2),
                size=20480
            )
        ]

    def test_list_pagenated(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')

        s3.client.list_objects_v2.side_effect = [
            {
                'IsTruncated': True,
                'NextContinuationToken': 'next-token-1',
                'Contents': [
                    {
                        'Key': 'file-1.wps',
                        'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 1),
                        'Size': 10 * 1024
                    },
                    {
                        'Key': 'file-2.rm',
                        'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 2),
                        'Size': 20 * 1024
                    },
                ]
            },
            {
                'IsTruncated': True,
                'NextContinuationToken': 'next-token-2',
                'Contents': [
                    {
                        'Key': 'file-3.rar',
                        'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 3),
                        'Size': 5 * 1024
                    },
                ]
            },
            {
                'IsTruncated': False,
                'Contents': [
                    {
                        'Key': 'file-4.rtf',
                        'LastModified': datetime.datetime(2019, 8, 26, 15, 30, 4),
                        'Size': 1024
                    },
                ]
            },

        ]

        results = s3.list('foo/bar')

        assert s3.client.list_objects_v2.call_args_list == [
            mock.call(Bucket='bucket', Prefix='foo/bar'),
            mock.call(Bucket='bucket', Prefix='foo/bar', ContinuationToken='next-token-1'),
            mock.call(Bucket='bucket', Prefix='foo/bar', ContinuationToken='next-token-2'),
        ]

        assert results == [
            ListEntry(
                name='file-1.wps',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 1),
                size=10240
            ),
            ListEntry(
                name='file-2.rm',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 2),
                size=20480
            ),
            ListEntry(
                name='file-3.rar',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 3),
                size=5120
            ),
            ListEntry(
                name='file-4.rtf',
                last_modified=arrow.get(2019, 8, 26, 15, 30, 4),
                size=1024
            ),
        ]

    def test_delete(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')

        s3.delete('foo/bar')

        s3.client.delete_object.assert_called_once_with(
            Bucket='bucket',
            Key='foo/bar'
        )

    def test_open_read(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')

        result = s3.open('foo/bar', FileMode.read)
        assert isinstance(result, backends.s3.S3Reader)
        s3.client.get_object.assert_called_once_with(Bucket='bucket', Key='foo/bar')

    def test_open_write(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')

        result = s3.open('foo/bar', FileMode.write)
        assert isinstance(result, backends.s3.S3Writer)

    def test_open_read_write(self, m_boto):
        s3 = backends.S3Storage("bucket", aws_region="us-east-1")
        with pytest.raises(
            NotImplementedError, match=re.escape("Read+write mode not supported by the S3 backend")
        ):
            s3.open("foo/bar", FileMode.read | FileMode.write)

        with pytest.raises(
            ValueError,
            match=re.escape("Unsupported mode. Accepted modes are FileMode.read or FileMode.write"),
        ):
            s3.open("foo/bar", FileMode(0))

    def test_read_operations(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')
        body_obj = io.BytesIO(b'a' * 100)
        s3.client.get_object.return_value = {
            'Body': body_obj
        }

        with s3.open('foo/bar', FileMode.read) as fp:
            assert fp.read(1) == b'a'
            assert fp.read(2) == b'aa'
            assert fp.read(37) == b'a' * 37
            assert fp.read(50) == b'a' * 50

        s3.client.get_object.assert_called_once_with(Bucket='bucket', Key='foo/bar')
        assert fp.reader.closed is True

    def test_read_not_found(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')
        s3.client.get_object.side_effect = ClientError({'Error': {'Code': 'NoSuchKey'}}, 'foo')

        with pytest.raises(FileNotFoundInStorageError) as exc:
            s3.open('foo/bar', FileMode.read)

        assert exc.value.filename == 'foo/bar'
        assert str(exc.value.storage_type) == 'S3Storage'

    def test_write_operations(self, m_boto):
        m_client = mock.MagicMock()
        m_client.create_multipart_upload.return_value = {'UploadId': 'upload-id'}
        m_client.upload_part.side_effect = [{'ETag': f'etag-{x}'} for x in range(5)]

        with backends.s3.S3Writer('bucket', 'foo/bar', m_client, chunk_size=100) as fp:
            m_client.create_multipart_upload.assert_not_called()

            fp.write(b'a')
            m_client.create_multipart_upload.assert_not_called()
            m_client.upload_part.assert_not_called()

            fp.write(b'b' * 100)
            m_client.create_multipart_upload.assert_called_once_with(
                Bucket='bucket',
                Key='foo/bar'
            )
            m_client.upload_part.assert_called_once_with(
                Bucket='bucket',
                Key='foo/bar',
                PartNumber=1,
                UploadId='upload-id',
                Body=b'a' + b'b' * 99
            )

            # test a write bigger than the buffer size
            fp.write(b'c' * 200)
            m_client.upload_part.assert_any_call(
                Bucket='bucket',
                Key='foo/bar',
                PartNumber=2,
                UploadId='upload-id',
                Body=b'b' + b'c' * 99
            )
            m_client.upload_part.assert_any_call(
                Bucket='bucket',
                Key='foo/bar',
                PartNumber=3,
                UploadId='upload-id',
                Body=b'c' * 100
            )

            m_client.complete_multipart_upload.assert_not_called()

        m_client.upload_part.assert_called_with(
            Bucket='bucket',
            Key='foo/bar',
            PartNumber=4,
            UploadId='upload-id',
            Body=b'c'
        )

        m_client.complete_multipart_upload.assert_called_once_with(
            Bucket='bucket',
            Key='foo/bar',
            UploadId='upload-id',
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': 1,
                        'ETag': 'etag-0',
                    },
                    {
                        'PartNumber': 2,
                        'ETag': 'etag-1',
                    },
                    {
                        'PartNumber': 3,
                        'ETag': 'etag-2',
                    },
                    {
                        'PartNumber': 4,
                        'ETag': 'etag-3',
                    },
                ]
            }
        )

    def test_write_abort(self, m_boto):
        m_client = mock.MagicMock()
        m_client.create_multipart_upload.return_value = {'UploadId': 'upload-id'}
        m_client.upload_part.return_value = {'ETag': 'etag-0'}

        with backends.s3.S3Writer('bucket', 'foo/bar', m_client, chunk_size=100) as fp:
            fp.write(b'a' * 99)  # fill the buffer but don't trigger a flush
            fp.abort()
        # We haven't created the upload yet so aborting should do nothing
        m_client.abort_multipart_upload.assert_not_called()

        with backends.s3.S3Writer('bucket', 'foo/bar', m_client, chunk_size=100) as fp:
            fp.write(b'a' * 100)  # Force a buffer flush
            fp.abort()

        m_client.complete_multipart_upload.assert_not_called()
        m_client.abort_multipart_upload.assert_called_once_with(
            Bucket='bucket',
            Key='foo/bar',
            UploadId='upload-id'
        )

    def test_write_flushes(self, m_boto):
        m_client = mock.MagicMock()
        m_client.create_multipart_upload.return_value = {'UploadId': 'upload-id'}
        m_client.upload_part.return_value = {'ETag': 'etag-0'}

        with backends.s3.S3Writer('bucket', 'foo/bar', m_client, chunk_size=100) as fp:
            fp.write(b'a' * 99)
        m_client.create_multipart_upload.assert_called()
        m_client.upload_part.assert_called()
        m_client.complete_multipart_upload.assert_called()

    def test_link_to_bad_operation(self, m_boto):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')

        with pytest.raises(NotImplementedError,
                           match='S3 backends cannot generate a link for multiple operations'):
            s3.link_to(
                path='foo/bar',
                operation=ShareLinkOperation.download | ShareLinkOperation.upload,
                expire=arrow.utcnow().shift(hours=1)
            )

        assert not s3.client.generate_presigned_url.called

    @pytest.mark.parametrize('op,method,extra_params', [
        (ShareLinkOperation.download, 'get_object', {}),
        (ShareLinkOperation.upload, 'put_object', {'ContentType': 'application/octet-stream'}),
        (ShareLinkOperation.remove, 'delete_object', {}),
    ])
    @freezegun.freeze_time('2020-04-27')
    def test_link_to_success(self, m_boto, op, method, extra_params):
        s3 = backends.S3Storage('bucket', aws_region='us-east-1')
        s3.client.generate_presigned_url.return_value = 'https://localhost/foo'

        result = s3.link_to(path='foo/bar', operation=op, expire=arrow.get(2020, 4, 27, 1))
        assert result == 'https://localhost/foo'

        s3.client.generate_presigned_url.assert_called_once_with(
            ClientMethod=method,
            ExpiresIn=3600,
            Params={'Bucket': 'bucket', 'Key': 'foo/bar', **extra_params}
        )
