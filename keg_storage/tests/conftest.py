from keg_storage_ta.app import KegStorageTestApp


def pytest_configure(config):
    KegStorageTestApp.testing_prep()
