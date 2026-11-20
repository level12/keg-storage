import enum
import pathlib

import flask
from keg.web import BaseView

import keg_storage


public_bp = flask.Blueprint('public', __name__)


class StorageLocation(enum.Enum):
    folder1 = 'Folder-One'
    folder2 = 'Folder-Two'


def create_local_storage(
    root: str | pathlib.Path,
    secret_key: bytes,
) -> keg_storage.LocalFSStorage:
    return keg_storage.LocalFSStorage(
        root=root,
        linked_endpoint='public.link-view',
        secret_key=secret_key,
        name='local.storage',
    )


class LinkView(keg_storage.LinkViewMixin, BaseView):
    blueprint = public_bp
    url = '/storage'

    def get_storage_backend(self) -> keg_storage.InternalLinksStorageBackend:
        root = flask.current_app.config.get('KEG_STORAGE_TA_ROOT')
        secret_key = flask.current_app.config.get('KEG_STORAGE_TA_SECRET_KEY')
        if root is None or secret_key is None:
            raise RuntimeError(
                'KEG_STORAGE_TA_ROOT and KEG_STORAGE_TA_SECRET_KEY must be configured',
            )
        return create_local_storage(root, secret_key)


class ObjectView(keg_storage.StorageOperations, BaseView):
    blueprint = public_bp
    url = '/object'
    storage_profile = 'storage.s3'
    storage_location = StorageLocation.folder1

    def post(self):
        return self.storage_upload_form_file('my_file')
