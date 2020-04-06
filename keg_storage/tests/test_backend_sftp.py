from unittest import mock

import arrow
import pytest
import wrapt
from blazeutils.containers import LazyDict

import keg_storage
from keg_storage.backends.base import FileMode, ListEntry
from keg_storage.backends.sftp import SFTPRemoteFile


def sftp_mocked(**kwargs):
    @wrapt.decorator(adapter=lambda self: None)
    def wrapper(wrapped, instance, args, _kwargs):

        @mock.patch('keg_storage.sftp.log', autospec=True, spec_set=True)
        def run_test(m_log):
            m_client = mock.MagicMock(
                spec=keg_storage.sftp.SSHClient,
                spec_set=keg_storage.sftp.SSHClient
            )

            class FakeSFTPStorage(keg_storage.sftp.SFTPStorage):
                def create_client(self):
                    return m_client

            fake_sftp = FakeSFTPStorage(
                host=kwargs.get('host', 'foo'),
                username=kwargs.get('username', 'bar'),
                key_filename=kwargs.get('key_filename'),
                known_hosts_fpath=kwargs.get('known_hosts_fpath', 'known_hosts'),
            )

            m_sftp = mock.MagicMock()

            m_client.__enter__.return_value = m_client
            m_client.open_sftp.return_value = m_sftp
            wrapped(
                sftp=fake_sftp,
                m_sftp=m_sftp,
                m_log=m_log
            )

        return run_test()
    return wrapper


class TestSFTPStorage:

    @sftp_mocked()
    def test_sftp_list_files(self, sftp, m_sftp, m_log):
        files = [
            LazyDict(filename='a.txt', st_mtime=1564771623, st_size=128),
            LazyDict(filename='b.pdf', st_mtime=1564771638, st_size=32768),
            LazyDict(filename='more.txt', st_mtime=1564771647, st_size=100)
        ]
        m_sftp.listdir_attr.return_value = files
        assert sftp.list('.') == [
            ListEntry(name='a.txt', last_modified=arrow.get(1564771623), size=128),
            ListEntry(name='b.pdf', last_modified=arrow.get(1564771638), size=32768),
            ListEntry(name='more.txt', last_modified=arrow.get(1564771647), size=100),
        ]
        assert m_log.info.mock_calls == []

    @sftp_mocked()
    def test_sftp_delete_file(self, sftp, m_sftp, m_log):
        sftp.delete('/tmp/abc/baz.txt')
        m_sftp.remove.assert_called_once_with('/tmp/abc/baz.txt')
        m_log.info.assert_called_once_with("Deleting remote file '%s'", '/tmp/abc/baz.txt')

    @sftp_mocked()
    def test_open(self, sftp, m_sftp, m_log):
        file = sftp.open('/tmp/foo.txt', FileMode.read)
        assert isinstance(file, SFTPRemoteFile)

        assert file.mode == FileMode.read
        assert file.path == '/tmp/foo.txt'
        assert file.sftp is m_sftp

        m_sftp.open.assert_called_once_with('/tmp/foo.txt', 'rb')

    @sftp_mocked()
    def test_read_operations(self, sftp, m_sftp, m_log):
        m_file = m_sftp.open.return_value
        m_file.read.return_value = b'some data'

        with sftp.open('/tmp/foo.txt', FileMode.read) as file:
            assert file.read(4) == b'some data'
            m_file.read.assert_called_once_with(4)
            m_file.close.assert_not_called()
        m_file.close.assert_called_once_with()

    @sftp_mocked()
    def test_read_not_permitted(self, sftp, m_sftp, m_log):
        with sftp.open('/tmp/foo.txt', FileMode.write) as file:
            with pytest.raises(IOError, match="File not opened for reading"):
                file.read(1)

    @sftp_mocked()
    def test_write_operations(self, sftp, m_sftp, m_log):
        m_file = m_sftp.open.return_value
        with sftp.open('/tmp/foo.txt', FileMode.write) as file:
            file.write(b'some data')
            m_file.write.assert_called_once_with(b'some data')
            m_file.close.assert_not_called()
        m_file.close.assert_called_once_with()

    @sftp_mocked()
    def test_write_not_permitted(self, sftp, m_sftp, m_log):
        with sftp.open("/tmp/foo.txt", FileMode.read) as file:
            with pytest.raises(IOError, match="File not opened for writing"):
                file.write(b"")
