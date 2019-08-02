import contextlib
import logging

import arrow
from keg_elements import crypto
from paramiko import SSHClient

from .base import (
    StorageBackend,
    ListEntry,
)

log = logging.getLogger(__name__)


class BatchSFTPStorage(StorageBackend):
    """
    A `StorageBackend` that interacts with a single, pre-connected SFTP client.
    """

    def __init__(self, sftp_client, crypto_key=None, name='sftp'):
        """
        :param sftp_client: is an paramiko.SFTPClient object.
        :param name: is the name of the storage backend.
        """
        self.sftp_client = sftp_client
        self.name = name
        self.crypto_key = crypto_key

    def list(self, path):
        return self.sftp_client.listdir_attr(path)

    def _get_encrypted(self, path, dest):
        chunk_size = 10 * 1024 * 1024
        with self.sftp_client.open(path, 'rb', bufsize=chunk_size) as infile:
            with open(dest, 'wb') as outfile:
                for chunk in crypto.encrypt_fileobj(self.crypto_key, infile, chunk_size):
                    outfile.write(chunk)

    def get(self, path, dest):
        log.info("Getting file from '%s' to '%s'", path, dest)
        if self.crypto_key:
            self._get_encrypted(path, dest)
        else:
            self.sftp_client.get(path, dest)

    def _put_encrypted(self, path, dest):
        chunk_size = 10 * 1024 * 1024
        with open(path, 'rb') as infile:
            with self.sftp_client.open(dest, 'wb', bufsize=chunk_size) as outfile:
                for chunk in crypto.encrypt_fileobj(self.crypto_key, infile, chunk_size):
                    outfile.write(chunk)

    def put(self, path, dest):
        log.info("Uploading file from '%s' to '%s'", path, dest)
        if self.crypto_key:
            self._put_encrypted(path, dest)
        else:
            self.sftp_client.put(path, dest)

    def delete(self, path):
        log.info("Deleting remote file '%s'", path)
        self.sftp_client.remove(path)


class SFTPStorage(StorageBackend):
    """
    A `StorageBackend` that connects to a remote via SFTP for each command.
    """

    def __init__(self,
                 host,
                 username,
                 key_filename,
                 known_hosts_fpath,
                 local_base_dpath,
                 remote_base_dpath='/',
                 allow_agent=False,
                 look_for_keys=False,
                 crypto_key=None,        # Key used to encrypt files copied from the remote.
                 name='sftp'):
        self.host = host
        self.username = username
        self.key_filename = key_filename
        self.known_hosts_fpath = known_hosts_fpath
        self.local_base_dpath = local_base_dpath
        self.remote_base_dpath = remote_base_dpath
        self.allow_agent = allow_agent
        self.look_for_keys = look_for_keys
        self.crypto_key = crypto_key
        self.name = name

    def list(self, path):
        with self.batch() as conn:
            return [
                ListEntry(
                    name=x.filename,
                    last_modified=arrow.get(x.st_mtime),
                    size=x.st_size
                )
                for x in conn.list(path)
            ]

    def get(self, path, dest):
        with self.batch() as conn:
            return conn.get(path, dest)

    def put(self, path, dest):
        with self.batch() as conn:
            return conn.put(path, dest)

    def delete(self, path):
        with self.batch() as conn:
            return conn.delete(path)

    @contextlib.contextmanager
    def batch(self):
        """
        Returns a version of this class that is a context manager and reuses the same connection.
        """
        with SSHClient() as client:
            client.load_system_host_keys(self.known_hosts_fpath)

            client.connect(
                self.host,
                username=self.username,
                key_filename=self.key_filename,
                allow_agent=self.allow_agent,
                look_for_keys=self.look_for_keys,
            )

            sftp = client.open_sftp()
            sftp.chdir(self.remote_base_dpath)
            yield BatchSFTPStorage(sftp, self.crypto_key, name=self.name)
