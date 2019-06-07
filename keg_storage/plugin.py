import collections

from keg_storage import cli


class Storage:
    """A proxy and management object for storage backends."""

    _interaces = None

    def __init__(self, app=None, cli_group_name='storage'):
        self.cli_group_name = cli_group_name

        if app:
            self.init_app(app)

    def init_app(self, app):
        app.storage = self

        self._interfaces = collections.OrderedDict(
            (params['name'], interface(**params))
            for interface, params in app.config.get('STORAGE_PROFILES')
        )
        self.interface = next(iter(self._interfaces)) if self._interfaces else None

        self.cli_group = self.init_cli(app)

    def get_interface(self, interface=None):
        iface = interface or self.interface

        return self._interfaces[iface] if iface else None

    def list(self, *args, interface=None, **kwargs):
        return self.get_interface(interface).list(*args, **kwargs)

    def get(self, *args, interface=None, **kwargs):
        return self.get_interface(interface).get(*args, **kwargs)

    def put(self, *args, interface=None, **kwargs):
        return self.get_interface(interface).put(*args, **kwargs)

    def delete(self, *args, interface=None, **kwargs):
        return self.get_interface(interface).delete(*args, **kwargs)

    def init_cli(self, app):
        cli.add_cli_to_app(app, self.cli_group_name)
