import io
from collections import namedtuple

import arrow
from blazeutils.containers import LazyDict
from keg_elements import crypto
import mock
import wrapt

import keg_storage
from keg_storage.backends.base import ListEntry


def sftp_mocked(**kwargs):
    @wrapt.decorator(adapter=lambda self: None)
    def wrapper(wrapped, instance, args, _kwargs):

        @mock.patch('keg_storage.sftp.SSHClient', autospec=True, spec_set=True)
        @mock.patch('keg_storage.sftp.log', autospec=True, spec_set=True)
        def run_test(m_log, m_SSHC):
            m_sftp = m_SSHC.return_value.__enter__.return_value.open_sftp.return_value
            wrapped(
                sftp=keg_storage.sftp.SFTPStorage(
                    kwargs.get('host', 'foo'),
                    kwargs.get('username', 'bar'),
                    kwargs.get('key_filename'),
                    kwargs.get('known_hosts_fpath', 'known_hosts'),
                    kwargs.get('local_base_dpath', 'data'),
                    kwargs.get('remote_base_dpath', '/home/bar'),
                    crypto_key=kwargs.get('crypto_key')
                ),
                m_sftp=m_sftp,
                m_log=m_log
            )

        return run_test()
    return wrapper


class TestSFTPStorage:

    def test_sftp_list_files(self):

        @sftp_mocked()
        def run_test(sftp, m_sftp, m_log):
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

        run_test()

    def test_sftp_get_file(self):
        @sftp_mocked()
        def run_test(sftp, m_sftp, m_log):
            sftp.get('some-file.txt', '/fake/some-file-dest.txt')
            m_sftp.chdir.assert_called_once_with('/home/bar')
            m_sftp.get.assert_called_once_with(
                'some-file.txt', '/fake/some-file-dest.txt')
            m_log.info.assert_called_once_with(
                "Getting file from '%s' to '%s'",
                'some-file.txt',
                '/fake/some-file-dest.txt'
            )
        run_test()

    def test_sftp_get_file_encrypted(self, tmpdir):
        dstfpath = str(tmpdir.mkdir('output').join('output.txt'))
        crypto_key = b'a' * 32

        @sftp_mocked(crypto_key=crypto_key)
        def check(sftp, m_sftp, m_log):
            m_sftp.open.return_value = io.BytesIO(b'foo-bar')
            m_sftp.stat = mock.MagicMock(return_value=namedtuple('stat', ['st_size'])(7))
            sftp.get('some-file.txt', dstfpath)
            m_sftp.chdir.assert_called_once_with('/home/bar')
            m_sftp.open.assert_called_once_with('some-file.txt', 'rb', bufsize=10 * 1024 * 1024)
            assert m_sftp.get.call_count == 0
            m_log.info.assert_called_once_with(
                "Getting file from '%s' to '%s'",
                'some-file.txt',
                dstfpath
            )
            assert b'foo-bar' not in open(dstfpath, 'rb').read()
            assert crypto.decrypt_bytesio(crypto_key, dstfpath).read() == b'foo-bar'

        check()

    def test_sftp_put_file(self):
        @sftp_mocked()
        def run_test(sftp, m_sftp, m_log):
            sftp.put('/tmp/abc/baz.txt', 'dest/zab.txt')
            m_sftp.put.assert_called_once_with('/tmp/abc/baz.txt', 'dest/zab.txt')
            m_log.info.assert_called_once_with(
                "Uploading file from '%s' to '%s'",
                '/tmp/abc/baz.txt',
                'dest/zab.txt'
            )
        run_test()

    def test_sftp_put_file_encrypted(self, tmpdir):
        srcfpath = str(tmpdir.mkdir('input').join('input.txt'))
        with open(srcfpath, 'wb') as srcfile:
            srcfile.write(b'foo-bar')
        destfpath = str(tmpdir.mkdir('output').join('output.txt'))
        crypto_key = b'a' * 32

        @sftp_mocked(crypto_key=crypto_key)
        def check(sftp, m_sftp, m_log):
            m_sftp.open.return_value = open(destfpath, 'wb')
            m_sftp.stat = mock.MagicMock(return_value=namedtuple('stat', ['st_size'])(7))

            sftp.put(srcfpath, 'dest/some-file.txt')

            m_sftp.chdir.assert_called_once_with('/home/bar')
            m_sftp.open.assert_called_once_with('dest/some-file.txt', 'wb', bufsize=10*1024*1024)
            assert m_sftp.put.call_count == 0
            m_log.info.assert_called_once_with(
                "Uploading file from '%s' to '%s'",
                srcfpath,
                'dest/some-file.txt',
            )
            assert b'foo-bar' not in open(destfpath, 'rb').read()
            assert crypto.decrypt_bytesio(crypto_key, destfpath).read() == b'foo-bar'

        check()

    def test_sftp_delete_file(self):
        @sftp_mocked()
        def run_test(sftp, m_sftp, m_log):
            sftp.delete('/tmp/abc/baz.txt')
            m_sftp.remove.assert_called_once_with('/tmp/abc/baz.txt')
            m_log.info.assert_called_once_with(
                "Deleting remote file '%s'", '/tmp/abc/baz.txt'
            )

        run_test()
