import keg_storage


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
