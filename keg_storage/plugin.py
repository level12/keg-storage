
import collections


class Storage:
    """A proxy and management object for storage backends."""

    _interaces = None

    def __init__(self, app=None):

        if app:
            self.init_app(app)

    def init_app(self, app):
        self._interfaces = collections.OrderedDict(
            (params['name'], interface(**params))
            for interface, params in app.config['STORAGE_PROFILES']
        )

        self.interface = next(iter(self._interfaces)) if self._interfaces else None

    def get_interface(self, interface=None):
        return self._interfaces[interface or self.interface]
