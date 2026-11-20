import keg_storage


class DefaultProfile:
    # This just gets rid of warnings on the console.
    KEG_KEYRING_ENABLE = False

    SITE_NAME = 'Keg Storage Demo'
    SITE_ABBR = 'KS Demo'

    # Required to enable the local-storage link view. Configure these with a
    # fixed storage directory and an application-specific random secret.
    KEG_STORAGE_TA_ROOT = None
    KEG_STORAGE_TA_SECRET_KEY = None

    KEG_STORAGE_PROFILES = [
        (
            keg_storage.S3Storage,
            {
                'name': 'storage.s3',
                'bucket': 'storage.test',
                'aws_region': 'us-east-1',
            },
        ),
        (
            keg_storage.SFTPStorage,
            {
                'name': 'storage.sftp',
                'host': 'example.com',
                'username': 'john.doe',
                'key_filename': '/key/path',
                'known_hosts_fpath': '/known/hosts/path',
                'allow_agent': False,
                'look_for_keys': False,
            },
        ),
    ]
    # KEG_STORAGE_DEFAULT_LOCATION = 'storage.s3'


class TestProfile:
    pass
