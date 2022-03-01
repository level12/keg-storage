import enum
from typing import Union

import flask
import pathlib
from keg.web import BaseView

import keg_storage


public_bp = flask.Blueprint('public', __name__)


class StorageLocation(enum.Enum):
    folder1 = 'Folder-One'
    folder2 = 'Folder-Two'


def create_local_storage(root: Union[str, pathlib.Path]) -> keg_storage.LocalFSStorage:
    return keg_storage.LocalFSStorage(
        root=root,
        linked_endpoint='public.link-view',
        secret_key=b'a' * 32,
        name='local.storage'
    )


class LinkView(keg_storage.LinkViewMixin, BaseView):
    blueprint = public_bp
    url = '/storage'

    def get_storage_backend(self) -> keg_storage.InternalLinksStorageBackend:
        root = flask.request.headers['StorageRoot']
        return create_local_storage(root)


class ObjectView(keg_storage.StorageOperations, BaseView):
    blueprint = public_bp
    url = '/object'
    storage_profile = 'storage.s3'
    storage_location = StorageLocation.folder1

    def post(self):
        return self.storage_upload_form_file('my_file')
