import collections


class Storage:
    """A proxy and management object for storage backends."""

    _interaces = None

    def __init__(self, app=None):

        if app:
            self.init_app(app)

    def init_app(self, app):

        self.app = app
        if self.app is None:
            raise AttributeError('app can not be none')

        self._interfaces = collections.OrderedDict(
            (params['name'], interface(**params))
            for interface, params in app.config['STORAGE_PROFILES']
        )

        self.interface = next(iter(self._interfaces)) if self._interfaces else None

    def get_interface(self, interface=None):
        return self._interfaces[interface or self.interface]


class StorageBackend:
    name = None

    def __init__(self, *args, **kwargs):
        pass

    def list(self, path):
        """Returns an iterator over the given path"""
        raise NotImplementedError()

    def get(self, path, dest):
        raise NotImplementedError()

    def put(self, path, dest):
        raise NotImplementedError()

    def delete(self, path):
        raise NotImplementedError()
