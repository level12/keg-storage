from __future__ import absolute_import

from keg.app import Keg

from keg_storage_ta.extensions import storage


class KegStorageTestApp(Keg):
    import_name = 'keg_storage_ta'
    db_enabled = False
    keyring_enable = False

    def on_init_complete(self):
        storage.init_app(self)
        return self


if __name__ == '__main__':
    from keg_storage_ta import app
    app.KegStorageTestApp.cli.main()
