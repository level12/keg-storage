import collections
import io
import logging
import uuid
import warnings
from enum import Enum
from typing import Mapping, Optional

import arrow
import flask
import wrapt
from authlib import jose
from werkzeug.utils import secure_filename

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
    try:
        if 'csrf' in flask.current_app.extensions:
            return flask.current_app.extensions['csrf'].exempt(func)
    except RuntimeError as exc:
        if 'application context' not in str(exc):
            raise
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
        except (
            jose.errors.BadSignatureError,
            jose.errors.DecodeError,
            jose.errors.ExpiredTokenError,
        ):
            flask.abort(403)

    def get(self):
        storage = self.get_storage_backend()
        token_data = self.get_token_data(storage)

        if not token_data.allow_download:
            flask.abort(403)

        headers = {}
        output_path = flask.request.args.get('output_path')
        if output_path:
            headers['Content-Disposition'] = f'attachment; filename={output_path}'

        fp = storage.open(token_data.path, backends.FileMode.read)
        return flask.Response(
            fp.iter_chunks(),
            mimetype='application/octet-stream',
            headers=headers,
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


def storage_args():
    """Decorator to append storage_location and storage_profile kwargs to the method.

    Args default to the storage_location and storage_profile defined on the class.
    """

    @wrapt.decorator
    def _execute(wrapped, instance, args, kwargs):
        if not instance and args:
            # Check first arg to see if it's related to StorageOperations. We need this
            # for python < 3.9
            if issubclass(args[0], StorageOperations):
                instance = args[0]
        kwargs.setdefault('storage_location', getattr(instance, 'storage_location', None))
        kwargs.setdefault('storage_profile', getattr(instance, 'storage_profile', None))
        result = wrapped(*args, **kwargs)
        return result

    return _execute


class StorageOperations:
    """Ops wrapper for storage operations that will typically occur in a flask app.

    Assumes the storage plugin is being used and configured with storage profiles.

    Class properties storage_location and storage_profile may be assigned defaults in
    a subclass direct any of the operations to that folder path or configured interface.
    storage_location is expected to be an Enum.

    Each method will also take storage_location and storage_profile, so they can be
    provided directly for one-offs. So, this class can be used directly or as a mixin.
    """
    storage_location: Enum = None
    storage_profile: str = None

    @staticmethod
    def storage_prefix_path(location, filename):
        """Join the location path with the filename to get the full object path"""
        if filename.startswith('.'):
            filename = filename[1:]
        return '/'.join([location.value, filename])

    @staticmethod
    def storage_generate_filename(filename):
        """Generate a UUID-based filename for an object, typically for upload to prevent
        path collisions. If the provided original filename has an extension, honor that
        extension."""
        name_parts = filename.rsplit('.', 1)
        new_filename = uuid.uuid4()
        if len(name_parts) > 1:
            extension = name_parts[-1]
            return f'{new_filename}.{extension}'
        return new_filename

    @classmethod
    def storage_get_profile(cls, storage_profile=None):
        """Get configured storage interface. Either specify which interface via the
        storage_profile kwarg, or it will fall back to the first defined profile."""
        storage = flask.current_app.storage
        return storage.get_interface(
            storage_profile or cls.storage_profile or storage.interface
        )

    @classmethod
    @storage_args()
    def storage_download_file(cls, filename, storage_location=None, storage_profile=None):
        """Pull file data from storage, return BytesIO stream."""
        storage_instance = cls.storage_get_profile(storage_profile)
        buffer = io.BytesIO()
        storage_instance.download(
            cls.storage_prefix_path(storage_location, filename),
            buffer
        )
        buffer.seek(0)
        return buffer

    @classmethod
    @storage_args()
    def storage_upload_file(
        cls, file_object, filename, preserve_filename=False, storage_location=None,
        storage_profile=None
    ):
        """Push file data to storage. A UUID-based filename will be generated to prevent
        path collisions unless preserve_filename is set."""
        storage_instance = cls.storage_get_profile(storage_profile)
        filename = secure_filename(filename)
        storage_filename = (
            filename if preserve_filename else cls.storage_generate_filename(filename)
        )
        storage_instance.upload(
            file_object, path=cls.storage_prefix_path(storage_location, storage_filename)
        )
        return storage_filename

    @classmethod
    @storage_args()
    def storage_upload_form_file(
        cls, form_field: str, storage_location=None, storage_profile=None
    ):
        """Shortcut to push file data from posted form to storage."""
        file_storage = flask.request.files[form_field]
        return cls.storage_upload_file(
            file_storage.stream,
            file_storage.filename,
        )

    @classmethod
    @storage_args()
    def storage_delete_file(cls, filename, storage_location=None, storage_profile=None):
        """Remove file data from storage."""
        storage_instance = cls.storage_get_profile(storage_profile)
        storage_instance.delete(path=cls.storage_prefix_path(storage_location, filename))
        return True

    @classmethod
    @storage_args()
    def storage_duplicate_file(cls, filename, storage_location=None, storage_profile=None):
        """Copy file data already in storage to a new file object. Generates the new
        filename using a UUID."""
        storage_instance = cls.storage_get_profile(storage_profile)
        new_copy_filename = cls.storage_generate_filename(filename)
        storage_instance.copy(
            cls.storage_prefix_path(storage_location, filename),
            cls.storage_prefix_path(storage_location, new_copy_filename),
        )
        return new_copy_filename

    @classmethod
    @storage_args()
    def storage_get_download_link(
        cls, filename, expire_minutes, storage_location=None, storage_profile=None
    ):
        """Generate an expiring download link to pass to client for a stored object."""
        storage_instance = cls.storage_get_profile(storage_profile)
        return storage_instance.link_to(
            cls.storage_prefix_path(storage_location, filename),
            backends.ShareLinkOperation.download,
            arrow.utcnow().shift(minutes=expire_minutes),
        )

    @classmethod
    @storage_args()
    def storage_get_upload_link(
        cls, filename, expire_minutes, storage_location=None, storage_profile=None
    ):
        """Generate an expiring upload link to pass to client for data to be stored."""
        storage_instance = cls.storage_get_profile(storage_profile)
        storage_filename = cls.storage_generate_filename(filename)
        url = storage_instance.link_to(
            cls.storage_prefix_path(storage_location, storage_filename),
            backends.ShareLinkOperation.upload,
            arrow.utcnow().shift(minutes=expire_minutes),
        )
        return url, storage_filename
