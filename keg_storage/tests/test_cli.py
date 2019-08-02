import click.testing
from blazeutils.containers import LazyDict
from datetime import date
from flask import current_app
from keg.testing import CLIBase, invoke_command
from mock import mock

from keg_storage import FileNotFoundInStorageError


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestCLI(CLIBase):
    def test_no_location(self, m_get_interface):
        results = self.invoke('storage', 'get', 'foo/bar', exit_code=1)
        assert results.output == 'No location given and no default was configured.\nAborted!\n'

        self.invoke('storage', '--location', 'loc', 'get', 'foo/bar')
        m_get_interface.assert_called_once_with('loc')

    def test_bad_location(self, m_get_interface):
        m_get_interface.side_effect = KeyError
        results = self.invoke('storage', '--location', 'loc', 'get', 'foo/bar', exit_code=1)
        assert results.output == 'The location loc does not exist. ' \
            'Pass --location or change your configuration.\nAborted!\n'


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestList(CLIBase):
    cmd_name = 'storage --location loc list'

    def test_list(self, m_get_interface):
        m_list = mock.MagicMock(
            return_value=[LazyDict(last_modified=date(2017, 4, 1), name='foo/bar', size=1024 * 3)]
        )
        m_get_interface.return_value.list = m_list

        results = self.invoke('foo')
        assert results.output == 'Apr 01 2017\t3.0K\tfoo/bar\n'
        m_list.assert_called_once_with('foo')

    def test_list_simple(self, m_get_interface):
        m_list = mock.MagicMock(
            return_value=[LazyDict(last_modified=date(2017, 4, 1), name='foo/bar', size=1024 * 3)]
        )
        m_get_interface.return_value.list = m_list

        results = self.invoke('--simple')
        assert results.output == 'foo/bar\n'
        m_list.assert_called_once_with('/')


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestGet(CLIBase):
    cmd_name = 'storage --location loc get'

    def test_get_default_dest(self, m_get_interface):
        m_get = mock.MagicMock()
        m_get_interface.return_value.get = m_get

        results = self.invoke('foo/bar')
        assert results.output == 'Downloaded foo/bar to bar.\n'
        m_get.assert_called_once_with('foo/bar', 'bar')

    def test_get_given_dest(self, m_get_interface):
        m_get = mock.MagicMock()
        m_get_interface.return_value.get = m_get

        results = self.invoke('foo/bar', 'dest/path')
        assert results.output == 'Downloaded foo/bar to dest/path.\n'
        m_get.assert_called_once_with('foo/bar', 'dest/path')

    def test_get_file_not_found(self, m_get_interface):
        m_get_interface.return_value.get.side_effect = FileNotFoundInStorageError('abc', 'def')

        results = self.invoke('foo/bar', exit_code=1)
        assert results.output == 'Error: Could not open file def: Not found in abc.\n'


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestPut(CLIBase):
    cmd_name = 'storage --location loc put'

    def test_put(self, m_get_interface):
        m_put = mock.MagicMock()
        m_get_interface.return_value.put = m_put

        results = self.invoke('foo/bar', 'baz/bar')
        assert results.output == 'Uploaded foo/bar to baz/bar.\n'
        m_put.assert_called_once_with('foo/bar', 'baz/bar')


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestDelete(CLIBase):
    cmd_name = 'storage --location loc delete'

    def test_delete(self, m_get_interface):
        m_delete = mock.MagicMock()
        m_get_interface.return_value.delete = m_delete

        results = self.invoke('foo/bar')
        assert results.output == 'Deleted foo/bar.\n'
        m_delete.assert_called_once_with('foo/bar')

    def test_delete_file_not_found(self, m_get_interface):
        m_get_interface.return_value.delete.side_effect = FileNotFoundInStorageError('abc', 'def')

        results = self.invoke('foo/bar', exit_code=1)
        assert results.output == 'Error: Could not open file def: Not found in abc.\n'


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestLinkFor(CLIBase):
    cmd_name = 'storage --location loc link_for'

    def test_link_for_default_expiration(self, m_get_interface):
        m_link = mock.MagicMock(return_value='http://example.com/foo/bar')
        m_get_interface.return_value.link_for = m_link

        results = self.invoke('foo/bar')
        assert results.output == 'http://example.com/foo/bar\n'
        m_link.assert_called_once_with('foo/bar', 3600)

    def test_link_for_expiration_given(self, m_get_interface):
        m_link = mock.MagicMock(return_value='http://example.com/foo/bar')
        m_get_interface.return_value.link_for = m_link

        results = self.invoke('-e', '100', 'foo/bar')
        assert results.output == 'http://example.com/foo/bar\n'
        m_link.assert_called_once_with('foo/bar', 100)

    def test_link_for_error(self, m_get_interface):
        m_get_interface.return_value.link_for.side_effect = ValueError('Some error')

        results = self.invoke('foo/bar', exit_code=1)
        assert results.output == 'Some error\nAborted!\n'


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
@mock.patch('keg_storage.utils.reencrypt', autospec=True, spec_set=True)
class TestReencrypt(CLIBase):
    class InputRunner(click.testing.CliRunner):
        def isolation(self, **kwargs):
            kwargs['input'] = 'abc\nxyz'
            return super(self.__class__, self).isolation(**kwargs)

    def test_reencrypt(self, m_reencrypt, m_get_interface):
        m_get_interface.return_value = 'STORAGE'
        app_cls = current_app._get_current_object().__class__
        results = invoke_command(app_cls, 'storage', '--location', 'loc', 'reencrypt', 'foo',
                                 runner=self.InputRunner())

        assert results.output == 'Old Key: \nNew Key: \nRe-encrypted foo\n'
        m_reencrypt.assert_called_once_with('STORAGE', 'foo', b'abc', b'xyz')

    def test_file_not_found(self, m_reencrypt, m_get_interface):
        m_get_interface.return_value = 'STORAGE'
        m_reencrypt.side_effect = FileNotFoundInStorageError('abc', 'def')

        app_cls = current_app._get_current_object().__class__
        results = invoke_command(app_cls, 'storage', '--location', 'loc', 'reencrypt', 'foo',
                                 runner=self.InputRunner(), exit_code=1)

        assert results.output.splitlines() == [
            'Old Key: ',
            'New Key: ',
            'Error: Could not open file def: Not found in abc.'
        ]
