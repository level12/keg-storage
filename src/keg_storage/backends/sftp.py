import contextlib
import logging
import posixpath
import unicodedata

import arrow
from paramiko import SSHClient

from .base import (
    FileMode,
    InternalLinksStorageBackend,
    ListEntry,
    RemoteFile,
)


log = logging.getLogger(__name__)


class SFTPError(Exception):
    pass


class SFTPRemoteFile(RemoteFile):
    def __init__(self, mode, path, client):
        super().__init__(mode)
        self.path = path
        self.client = client
        self.file = None

        self.sftp = client.open_sftp()
        self.file = self.sftp.open(path, str(mode))

    def read(self, size: int):
        if not (self.mode & FileMode.read):
            raise OSError('File not opened for reading')
        return self.file.read(size)

    def close(self):
        # File may actually be none since the open operation in the constructor may have failed
        # before assigning the value
        if self.file is not None:
            self.file.close()
        self.client.close()

    def write(self, data: bytes):
        if not (self.mode & FileMode.write):
            raise OSError('File not opened for writing')
        return self.file.write(data)


class SFTPStorage(InternalLinksStorageBackend):
    def __init__(
        self,
        host,
        username,
        key_filename,
        known_hosts_fpath,
        port=22,
        allow_agent=False,
        look_for_keys=False,
        linked_endpoint=None,
        secret_key=None,
        name='sftp',
        root=None,
    ):
        super().__init__(name=name, linked_endpoint=linked_endpoint, secret_key=secret_key)
        self.host = host
        self.username = username
        self.key_filename = key_filename
        self.known_hosts_fpath = known_hosts_fpath
        self.port = port
        self.allow_agent = allow_agent
        self.look_for_keys = look_for_keys
        self.root = posixpath.normpath(root) if root is not None else None

    def _resolve_path(self, path: str, *, allow_root: bool = False) -> str:
        if (
            not isinstance(path, str)
            or '\\' in path
            or any(unicodedata.category(char) == 'Cc' for char in path)
            or '..' in path.split('/')
            or posixpath.isabs(path)
        ):
            raise SFTPError('Invalid path')

        normalized_path = posixpath.normpath(path)
        if normalized_path == '.' and not allow_root:
            raise SFTPError('Invalid path')

        if self.root is None:
            return normalized_path

        root_anchor = posixpath.normpath(
            self.root if posixpath.isabs(self.root) else f'/{self.root}',
        )
        resolved_anchor = posixpath.normpath(posixpath.join(root_anchor, normalized_path))
        if posixpath.commonpath((root_anchor, resolved_anchor)) != root_anchor:
            raise SFTPError('Invalid path')

        return posixpath.normpath(posixpath.join(self.root, normalized_path))

    def create_client(self):
        client = SSHClient()
        client.load_system_host_keys(self.known_hosts_fpath)

        client.connect(
            self.host,
            port=self.port,
            username=self.username,
            key_filename=self.key_filename,
            allow_agent=self.allow_agent,
            look_for_keys=self.look_for_keys,
        )

        return client

    @contextlib.contextmanager
    def connection(self):
        with self.create_client() as client:
            sftp = client.open_sftp()
            yield sftp

    def list(self, path: str):
        path = self._resolve_path(path, allow_root=True)
        with self.connection() as conn:
            return [
                ListEntry(name=x.filename, last_modified=arrow.get(x.st_mtime), size=x.st_size)
                for x in conn.listdir_attr(path)
            ]

    def open(self, path: str, mode: FileMode | str, extra_args=None):
        path = self._resolve_path(path)
        mode = FileMode.as_mode(mode)

        # SFTPRemoteFile is responsible for closing the client connection
        client = self.create_client()
        return SFTPRemoteFile(mode, path, client)

    def delete(self, path: str):
        path = self._resolve_path(path)
        log.info("Deleting remote file '%s'", path)

        with self.connection() as conn:
            conn.remove(path)
