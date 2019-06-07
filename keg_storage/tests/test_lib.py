import flask
from mock import mock

import keg_storage


class TestStorage:
    def test_app_to_init_call_init(self):
        app = mock.MagicMock()
        app.config = {'STORAGE_PROFILES': [(keg_storage.backends.StorageBackend, {'name': 'test'})]}
        storage = keg_storage.Storage(app)
        assert 'test' in storage._interfaces
        assert storage.interface == 'test'

    def test_selects_interface_for_plugin_methods(self):
        class FakeStorage(keg_storage.backends.StorageBackend):
            name = None

            def __init__(self, name):
                self.name = name

            def list(self, *args, **kwargs):
                return 'list-' + self.name

            def get(self, *args, **kwargs):
                return 'get-' + self.name

            def put(self, *args, **kwargs):
                return 'put-' + self.name

            def delete(self, *args, **kwargs):
                return 'delete-' + self.name

        app = flask.Flask('test')
        app.config.update({
            'STORAGE_PROFILES': [
                (FakeStorage, {'name': 'fake1'}),
                (FakeStorage, {'name': 'fake2'}),
            ]
        })
        store = keg_storage.Storage(app)

        assert store.get('/') == 'get-fake1'
        assert store.list('/') == 'list-fake1'
        assert store.put('/') == 'put-fake1'
        assert store.delete('/') == 'delete-fake1'
        assert store.get('/', interface='fake1') == 'get-fake1'
        assert store.get('/', interface='fake2') == 'get-fake2'
