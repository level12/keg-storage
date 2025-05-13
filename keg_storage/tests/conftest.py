import pytest
from keg.testing import ContextManager

from keg_storage_ta.app import KegStorageTestApp


def pytest_configure(config):
    KegStorageTestApp.testing_prep()


@pytest.fixture(scope='class', autouse=True)
def auto_app_context():
    with ContextManager.get_for(KegStorageTestApp).app.app_context():
        yield
