from typing import Union

import flask
import pathlib
from keg.web import BaseView

import keg_storage


public_bp = flask.Blueprint('public', __name__)


def create_local_storage(root: Union[str, pathlib.Path]) -> keg_storage.LocalFSStorage:
    return keg_storage.LocalFSStorage(
        root=root,
        linked_endpoint='public.link-view',
        secret_key=b'a' * 32,
        name='local.storage'
    )


class LinkView(BaseView, keg_storage.LinkViewMixin):
    blueprint = public_bp
    url = '/storage'

    def get_storage_backend(self) -> keg_storage.InternalLinksStorageBackend:
        root = flask.request.headers['StorageRoot']
        return create_local_storage(root)
