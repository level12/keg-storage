from unittest import mock

import keg_storage


class TestStorage:
    def test_app_to_init_call_init(self):
        app = mock.MagicMock()
        app.config = {
            "KEG_STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "test"})]
        }
        storage = keg_storage.Storage(app)
        assert "test" in storage._interfaces
        assert storage.interface == "test"

    def test_migration_storage_profiles(self):
        # Old name gets translated to current name.
        app = mock.MagicMock()
        app.config = {
            "STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "found"})]
        }
        storage = keg_storage.Storage(app)
        assert "found" in storage._interfaces
        assert storage.interface == "found"

        # If both are there, use the current name.
        app = mock.MagicMock()
        app.config = {
            "STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "ignored"})],
            "KEG_STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "found"})],
        }
        storage = keg_storage.Storage(app)
        assert "ignored" not in storage._interfaces
        assert "found" in storage._interfaces
        assert storage.interface == "found"
