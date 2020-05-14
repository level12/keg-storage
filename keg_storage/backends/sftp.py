import contextlib
import logging
import typing

import arrow
from paramiko import SSHClient

from .base import (
    InternalLinksStorageBackend,
    ListEntry,
    FileMode,
    RemoteFile,
)

log = logging.getLogger(__name__)


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
            raise IOError('File not opened for reading')
        return self.file.read(size)

    def close(self):
        # File may actually be none since the open operation in the constructor may have failed
        # before assigning the value
        if self.file is not None:
            self.file.close()
        self.client.close()

    def write(self, data: bytes):
        if not (self.mode & FileMode.write):
            raise IOError('File not opened for writing')
        return self.file.write(data)


class SFTPStorage(InternalLinksStorageBackend):
    def __init__(
            self,
            host,
            username,
            key_filename,
            known_hosts_fpath,
            allow_agent=False,
            look_for_keys=False,
            linked_endpoint=None,
            secret_key=None,
            name='sftp',
    ):
        super().__init__(name=name, linked_endpoint=linked_endpoint, secret_key=secret_key)
        self.host = host
        self.username = username
        self.key_filename = key_filename
        self.known_hosts_fpath = known_hosts_fpath
        self.allow_agent = allow_agent
        self.look_for_keys = look_for_keys

    def create_client(self):
        client = SSHClient()
        client.load_system_host_keys(self.known_hosts_fpath)

        client.connect(
            self.host,
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
        with self.connection() as conn:
            return [
                ListEntry(
                    name=x.filename,
                    last_modified=arrow.get(x.st_mtime),
                    size=x.st_size
                )
                for x in conn.listdir_attr(path)
            ]

    def open(self, path: str, mode: typing.Union[FileMode, str]):
        mode = FileMode.as_mode(mode)

        # SFTPRemoteFile is responsible for closing the client connection
        client = self.create_client()
        return SFTPRemoteFile(mode, path, client)

    def delete(self, path: str):
        log.info("Deleting remote file '%s'", path)

        with self.connection() as conn:
            conn.remove(path)
