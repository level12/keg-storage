from mock import patch
import wrapt

import keg_storage

from keg_elements.testing import DontCare


@wrapt.decorator(adapter=lambda self: None)
def sftp_mocked(wrapped, instance, args, kwargs):

    @patch('keg_storage.sftp.SSHClient', autospec=True, spec_set=True)
    @patch('keg_storage.sftp.log', autospec=True, spec_set=True)
    def run_test(m_log, m_SSHC):
        m_sftp = m_SSHC.return_value.__enter__.return_value.open_sftp.return_value
        wrapped(
            keg_storage.sftp.SFTPStorage('foo', 'bar', None, 'known_hosts', 'data', '/home/bar'),
            m_sftp,
            m_log
        )

    return run_test()


class TestSFTPStorage:
    @sftp_mocked
    def test_sftp_list_files(self, sftp, m_sftp, m_log):
        files = ['a.txt', 'b.pdf', 'more.txt']
        m_sftp.listdir.return_value = files
        assert sftp.list('.') == files
        assert m_log.info.mock_calls == []

    @sftp_mocked
    def test_sftp_get_file(self, sftp, m_sftp, m_log):
        sftp.get('some-file.txt', '/fake/some-file-dest.txt')
        m_sftp.chdir.assert_called_once_with('/home/bar')
        m_sftp.get.assert_called_once_with(
            'some-file.txt', '/fake/some-file-dest.txt')
        m_log.info.assert_called_once_with(
            "Getting file from '%s' to '%s'",
            'some-file.txt',
            '/fake/some-file-dest.txt'
        )

    @sftp_mocked
    def test_sftp_put_file(self, sftp, m_sftp, m_log):
        sftp.put('/tmp/abc/baz.txt', 'dest/zab.txt')
        m_sftp.put.assert_called_once_with('/tmp/abc/baz.txt', 'dest/zab.txt')
        m_log.info.assert_called_once_with(
            "Uploading file from '%s' to '%s'",
            '/tmp/abc/baz.txt',
            'dest/zab.txt'
        )

    @sftp_mocked
    def test_sftp_delete_file(self, sftp, m_sftp, m_log):
        sftp.delete('/tmp/abc/baz.txt')
        m_sftp.remove.assert_called_once_with('/tmp/abc/baz.txt')
        m_log.info.assert_called_once_with(
            "Deleting remote file '%s'", '/tmp/abc/baz.txt'
        )
