from importlib.util import find_spec

from .base import StorageBackend, FileNotFoundInStorageError, FileMode  # noqa

__all__ = [
    'StorageBackend',
    'FileNotFoundInStorageError',
    'FileMode',
    'base',
]

if find_spec('boto3') is not None:
    from .s3 import S3Storage  # noqa
    __all__.extend(['s3', 'S3Storage'])

if find_spec('paramiko') is not None:
    from .sftp import SFTPStorage  # noqa
    __all__.extend(['sftp', 'SFTPStorage'])

if find_spec('azure') is not None:
    from .azure import AzureStorage  # noqa
    __all__.extend(['azure', 'AzureStorage'])
