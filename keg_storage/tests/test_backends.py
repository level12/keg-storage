import click
import pytest

import keg_storage
from keg_storage.cli import handle_not_found


class TestStorageBackend:

    def test_methods_not_implemented(self):

        interface = keg_storage.StorageBackend()

        cases = {
            interface.list: ('path',),
            interface.delete: ('path',),
            interface.put: ('path', 'dest'),
            interface.get: ('path', 'dest'),
        }

        for method, args in cases.items():
            try:
                method(*args)
            except NotImplementedError:
                pass
            else:
                raise AssertionError('Should have raised exception')


class TestFileNotFoundException:
    def test_click_wrapper(self):
        s3 = keg_storage.S3Storage('bucket', 'key', 'secret', name='test')

        @handle_not_found
        def test_func():
            raise keg_storage.FileNotFoundInStorageError(s3, 'foo')

        with pytest.raises(click.FileError) as exc_info:
            test_func()
        assert exc_info.value.filename == 'foo'
        assert exc_info.value.message == 'Not found in S3Storage.'
