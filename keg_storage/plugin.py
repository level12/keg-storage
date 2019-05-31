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
            for interface, params in app.config['STORAGE_PROFILES']
        )
        self.interface = next(iter(self._interfaces)) if self._interfaces else None

        self.cli_group = self.init_cli(app)

    def get_interface(self, interface=None):
        return self._interfaces[interface or self.interface]

    def init_cli(self, app):
        cli.add_cli_to_app(app, self.cli_group_name)
