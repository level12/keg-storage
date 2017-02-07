import contextlib
import logging

from paramiko import SSHClient

# from libs import crypto

import keg_storage


log = logging.getLogger(__name__)


class BatchSFTPStorage(keg_storage.StorageBackend):
    """
    A `StorageBackend` that interacts with a single, pre-connected SFTP client.
    """

    def __init__(self, sftp_client, crypto_key=None, name='sftp'):
        """
        :param sftp_client: is an paramiko.SFTPClient object.
        :param crypto_key: is a crypto key to use for encrypting files copied from the remote.
        :param name: is the name of the storage backend.
        """
        self.sftp_client = sftp_client
        self.crypto_key = crypto_key
        self.name = name

    def list(self, path):
        return self.sftp_client.listdir(path)

    def get(self, path, dest):
        if self.crypto_key:
            self.get_encrypted(path, dest, self.crypto_key)
        else:
            self.get_raw(path, dest)

    def put(self, path, dest):
        log.info("Uploading file from '%s' to '%s'", path, dest)
        self.sftp_client.put(path, dest, log_transfer)

    def delete(self, path):
        log.info("Deleting remote file '%s'", path)
        self.sftp_client.remove(path)

    def get_raw(self, path, dest):
        log.info("Downloading file from '%s' to '%s'", path, dest)
        self.sftp_client.get(path, dest, log_transfer)

    def get_encrypted(self, path, dest, crypto_key):
        """Like `get` but stores the file encrypted locally."""
        stat_info = self.sftp_client.stat(dest)
        log.info("Downloading file with encryption from '%s' to '%s'", path, dest)

        chunk_size = 10*1024*1024  # TODO: Where is this number coming from?
        with self.sftp_client.open(path, 'rb', bufsize=chunk_size) as infile:
            with open(dest, 'wb') as outfile:
                for (chunk_tally, chunk) in enumerate(crypto.encrypt_fileobj(
                    crypto_key, infile, stat_info.st_size, chunk_size
                )):
                    log_transfer(chunk_size * chunk_tally, stat_info.st_size)
                    outfile.write(chunk)


class SFTPStorage(keg_storage.StorageBackend):
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
            return conn.list(path)

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
            yield BatchSFTPStorage(sftp, self.crypto_key)


def log_transfer(bytes_tally, bytes_total, log_fn=log.info):
    log_fn(">> {}/{} ({}%)".format(
        bytes_tally,
        bytes_total,
        round(bytes_tally / bytes_total * 100))
    )
