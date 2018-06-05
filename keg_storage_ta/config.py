import keg_storage


class DefaultProfile(object):
    # This just gets rid of warnings on the console.
    KEG_KEYRING_ENABLE = False

    SITE_NAME = 'Keg Storage Demo'
    SITE_ABBR = 'KS Demo'

    STORAGE_PROFILES = [
        (keg_storage.S3Storage, {
            'name': 'storage.s3',
            'bucket': 'storage.test',
            'aws_region': 'us-east-1',
            'aws_access_key_id': 'access-key-id',
            'aws_secret_access_key': 'secret-key',
        }),
        (keg_storage.SFTPStorage, {
            'name': 'storage.sftp',
            'host': 'example.com',
            'username': 'john.doe',
            'key_filename': '/key/path',
            'known_hosts_fpath': '/known/hosts/path',
            'local_base_dpath': '',
            'remote_base_dpath': '',
            'allow_agent': False,
            'look_for_keys': False,
            'crypto_key': 'encryption-key'
        })
    ]
    # KEG_STORAGE_DEFAULT_LOCATION = 'storage.s3'


class TestProfile(object):
    pass
