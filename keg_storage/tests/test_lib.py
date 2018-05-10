from mock import mock

import keg_storage


class TestStorage:
    def test_app_to_init_call_init(self):
        app = mock.MagicMock()
        app.config = {'STORAGE_PROFILES': [(keg_storage.backends.StorageBackend, {'name': 'test'})]}
        storage = keg_storage.Storage(app)
        assert 'test' in storage._interfaces
        assert storage.interface == 'test'
