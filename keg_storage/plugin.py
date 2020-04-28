import collections
import logging
import warnings
from typing import Mapping, Optional

import flask
import itsdangerous

from keg_storage import cli
from keg_storage import backends


log = logging.getLogger(__name__)


class Storage:
    """A proxy and management object for storage backends."""

    _interfaces: Mapping[str, backends.StorageBackend]
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


def _disable_csrf(func):
    if 'csrf' in flask.current_app.extensions:
        return flask.current_app.extensions['csrf'].exempt(func)
    return func


class LinkViewMixin:
    decorators = (_disable_csrf,)

    def get_storage_backend(self) -> backends.InternalLinksStorageBackend:
        raise NotImplementedError

    def get_request_token(self):
        return flask.request.args.get('token')

    def get_token_data(
            self,
            storage: backends.InternalLinksStorageBackend
    ) -> backends.InternalLinkTokenData:
        token = self.get_request_token()
        if not token:
            flask.abort(404)

        try:
            return storage.deserialize_link_token(token=token)
        except itsdangerous.BadData:
            flask.abort(403)

    def get(self):
        storage = self.get_storage_backend()
        token_data = self.get_token_data(storage)

        if not token_data.allow_download:
            flask.abort(403)

        fp = storage.open(token_data.path, backends.FileMode.read)
        return flask.Response(
            fp.iter_chunks(),
            mimetype='application/octet-stream',
        )

    def on_upload_success(self, token_data: backends.InternalLinkTokenData):
        return flask.Response('OK', status=200)

    def post(self):
        storage = self.get_storage_backend()
        token_data = self.get_token_data(storage)

        if not token_data.allow_upload:
            flask.abort(403)

        storage.upload(flask.request.stream, token_data.path)
        return self.on_upload_success(token_data)

    def put(self):
        return self.post()

    def on_delete_success(self, token_data: backends.InternalLinkTokenData):
        return flask.Response('OK', status=200)

    def delete(self):
        storage = self.get_storage_backend()
        token_data = self.get_token_data(storage)

        if not token_data.allow_remove:
            flask.abort(403)
        storage.delete(token_data.path)
        return self.on_delete_success(token_data)
