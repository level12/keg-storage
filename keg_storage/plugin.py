import collections
import logging
import warnings
from typing import Mapping, Optional

import flask

from keg_storage import cli
from keg_storage.backends import StorageBackend


log = logging.getLogger(__name__)


class Storage:
    """A proxy and management object for storage backends."""

    _interfaces: Mapping[str, StorageBackend]
    interface: Optional[str]

    def __init__(self, app: Optional[flask.Flask] = None, cli_group_name='storage'):
        self.cli_group_name = cli_group_name
        self._interfaces = {}

        if app:
            self.init_app(app)

    def _migrate_config(self, app: flask.Flask):
        """Handle changes in configuration variable names."""

        if "STORAGE_PROFILES" in app.config:
            if "KEG_STORAGE_PROFILES" in app.config:
                warnings.warn(
                    "Found both KEG_STORAGE_PROFILES and deprecated STORAGE_PROFILES. "
                    "Please remove the obsolete configuration.",
                    DeprecationWarning,
                )
                return
            warnings.warn(
                "STORAGE_PROFILES is deprecated. Please use KEG_STORAGE_PROFILES instead.",
                DeprecationWarning,
            )
            app.config["KEG_STORAGE_PROFILES"] = app.config.pop("STORAGE_PROFILES")

    def init_app(self, app: flask.Flask):
        self._migrate_config(app)
        app.storage = self

        self._interfaces = collections.OrderedDict(
            (params['name'], interface(**params))
            for interface, params in app.config['KEG_STORAGE_PROFILES']
        )
        self.interface = next(iter(self._interfaces)) if self._interfaces else None

        self.init_cli(app)

    def get_interface(self, interface: Optional[str] = None):
        interface = interface or self.interface
        if interface is None:
            raise ValueError("no interface was specified")
        elif interface not in self._interfaces:
            raise ValueError(f"invalid interface '{interface}'")
        return self._interfaces[interface]

    def init_cli(self, app: flask.Flask) -> None:
        cli.add_cli_to_app(app, self.cli_group_name)
