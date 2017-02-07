import keg_storage


class MockApp:
    pass


class TestStorage:

    def test_app_to_init_call_init(self):
        app = MockApp()
        app.config = {'STORAGE_PROFILES': [(keg_storage.StorageBackend, {'name': 'test'})]}
        storage = keg_storage.Storage(app)
        assert storage.app == app
        assert 'test' in storage._interfaces
        assert storage.interface == 'test'


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


