from unittest import mock

import pytest

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

        # Test plugin lookup.
        assert isinstance(storage.get_interface(), keg_storage.backends.StorageBackend)
        assert isinstance(storage.get_interface("test"), keg_storage.backends.StorageBackend)

        # Test invalid plugin.
        with pytest.raises(ValueError, match="invalid interface 'foo'"):
            storage.get_interface("foo")

    def test_migration_storage_profiles(self):
        # Old name gets translated to current name.
        app = mock.MagicMock()
        app.config = {
            "STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "found"})]
        }

        with pytest.warns(DeprecationWarning, match="STORAGE_PROFILES is deprecated"):
            storage = keg_storage.Storage(app)
        assert "found" in storage._interfaces
        assert storage.interface == "found"

        # If both are there, use the current name.
        app = mock.MagicMock()
        app.config = {
            "STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "ignored"})],
            "KEG_STORAGE_PROFILES": [(keg_storage.backends.StorageBackend, {"name": "found"})],
        }

        with pytest.warns(
            DeprecationWarning,
            match="Found both KEG_STORAGE_PROFILES and deprecated STORAGE_PROFILES",
        ):
            storage = keg_storage.Storage(app)
        assert "ignored" not in storage._interfaces
        assert "found" in storage._interfaces
        assert storage.interface == "found"

    def test_no_storage_profiles(self):
        app = mock.MagicMock()
        app.config = {"KEG_STORAGE_PROFILES": []}
        storage = keg_storage.Storage(app)

        with pytest.raises(ValueError, match="no interface was specified"):
            storage.get_interface()
        with pytest.raises(ValueError, match="invalid interface 'foo'"):
            storage.get_interface("foo")
