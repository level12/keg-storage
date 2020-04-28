from importlib.util import find_spec

from .base import (
    FileMode,
    FileNotFoundInStorageError,
    ProgressCallback,
    RemoteFile,
    StorageBackend,
    InternalLinksStorageBackend,
    InternalLinkTokenData,
    ShareLinkOperation,
)
from .filesystem import (
    LocalFSStorage,
    LocalFSError,
)

__all__ = [
    "FileMode",
    "FileNotFoundInStorageError",
    "ProgressCallback",
    "RemoteFile",
    "StorageBackend",
    "base",
    "filesystem",
    "LocalFSStorage",
    "LocalFSError",
    "InternalLinksStorageBackend",
    "InternalLinkTokenData",
    "ShareLinkOperation",
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
