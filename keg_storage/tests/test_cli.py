import contextlib
import io
import os
from datetime import date
from unittest import mock

import arrow
import click.testing
import freezegun
import pytest
from blazeutils.containers import LazyDict
from flask import current_app
from keg.testing import CLIBase, invoke_command

from keg_storage import FileNotFoundInStorageError, ShareLinkOperation


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestCLI(CLIBase):
    def test_no_location(self, m_get_interface):
        results = self.invoke('storage', 'list', 'foo/bar', exit_code=1)
        assert results.output == 'No location given and no default was configured.\nAborted!\n'

        self.invoke('storage', '--location', 'loc', 'list', 'foo/bar')
        m_get_interface.assert_called_once_with('loc')

    def test_bad_location(self, m_get_interface):
        m_get_interface.side_effect = KeyError
        results = self.invoke('storage', '--location', 'loc', 'list', 'foo/bar', exit_code=1)
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


@contextlib.contextmanager
def change_dir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield path
    finally:
        os.chdir(cwd)


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestGet(CLIBase):
    cmd_name = 'storage --location loc get'

    def test_get_default_dest(self, m_get_interface, tmp_path):
        m_download = mock.MagicMock()
        m_get_interface.return_value.download = m_download

        with change_dir(tmp_path):
            results = self.invoke('foo/bar')

        assert results.output == 'Downloaded foo/bar to bar.\n'

        out_path = os.path.join(tmp_path, 'bar')
        assert os.path.exists(out_path)
        args, _ = m_download.call_args
        assert args[0] == 'foo/bar'
        assert args[1].name == 'bar'

    def test_get_given_dest(self, m_get_interface, tmp_path):
        m_download = mock.MagicMock()
        m_get_interface.return_value.download = m_download

        out_path = os.path.join(tmp_path, 'output')
        results = self.invoke('foo/bar', out_path)
        assert results.output == 'Downloaded foo/bar to {}.\n'.format(out_path)

        args, _ = m_download.call_args
        assert args[0] == 'foo/bar'
        assert args[1].name == out_path

    def test_get_to_stdout(self, m_get_interface):
        m_download = mock.MagicMock()
        m_download.side_effect = lambda path, obj: obj.write(b'test output\n')

        m_get_interface.return_value.download = m_download

        results = self.invoke('foo/bar', '-')
        assert results.output == 'test output\nDownloaded foo/bar to -.\n'

    def test_get_file_not_found(self, m_get_interface):
        m_get_interface.return_value.download.side_effect = \
            FileNotFoundInStorageError('abc', 'def')

        results = self.invoke('foo/bar', 'bar', exit_code=1)
        assert results.output == 'Error: Could not open file def: Not found in abc.\n'


@mock.patch.object(current_app.storage, 'get_interface', autospec=True, spec_set=True)
class TestPut(CLIBase):
    cmd_name = 'storage --location loc put'

    def test_put(self, m_get_interface, tmp_path):
        m_upload = mock.MagicMock()
        m_get_interface.return_value.upload = m_upload

        source = os.path.join(tmp_path, 'bar.txt')
        with open(source, 'wb'):
            pass

        results = self.invoke(source, 'baz/bar')
        assert results.output == 'Uploaded {} to baz/bar.\n'.format(source)

        args, _ = m_upload.call_args
        assert args[0].name == source
        assert args[1] == 'baz/bar'

    def test_put_from_stdin(self, m_get_interface):
        m_upload = mock.MagicMock()
        stdin_data = io.BytesIO()
        m_upload.side_effect = lambda obj, path: stdin_data.write(obj.read())
        m_get_interface.return_value.upload = m_upload

        results = self.invoke('-', 'baz/bar', input=b'test data')
        assert results.output == 'Uploaded - to baz/bar.\n'

        args, _ = m_upload.call_args
        assert isinstance(args[0], io.BytesIO)
        assert args[1] == 'baz/bar'
        assert stdin_data.getvalue() == b'test data'


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
class TestLink(CLIBase):
    cmd_name = 'storage --location loc link'

    @freezegun.freeze_time('2020-04-27')
    def test_link_for_default_expiration(self, m_get_interface):
        m_link = mock.MagicMock(return_value='http://example.com/foo/bar')
        m_get_interface.return_value.link_to = m_link

        results = self.invoke('foo/bar')
        assert results.output == 'http://example.com/foo/bar\n'
        m_link.assert_called_once_with(
            path='foo/bar',
            operation=ShareLinkOperation.download,
            expire=arrow.get(2020, 4, 27, 1)
        )

    @freezegun.freeze_time('2020-04-27')
    def test_link_for_expiration_given(self, m_get_interface):
        m_link = mock.MagicMock(return_value='http://example.com/foo/bar')
        m_get_interface.return_value.link_to = m_link

        results = self.invoke('-e', '100', 'foo/bar')
        assert results.output == 'http://example.com/foo/bar\n'
        m_link.assert_called_once_with(
            path='foo/bar',
            operation=ShareLinkOperation.download,
            expire=arrow.get(2020, 4, 27, 0, 1, 40)
        )

    @pytest.mark.parametrize('flags,ops', [
        (['--download'], ShareLinkOperation.download),
        (['--upload'], ShareLinkOperation.download | ShareLinkOperation.upload),
        (['--no-download', '--upload'], ShareLinkOperation.upload),
        (['--no-download', '--delete'], ShareLinkOperation.remove),
        (['--no-download', '--upload', '--delete'],
         ShareLinkOperation.upload | ShareLinkOperation.remove),
        (['--upload', '--delete'],
         ShareLinkOperation.download | ShareLinkOperation.upload | ShareLinkOperation.remove),
    ])
    @freezegun.freeze_time('2020-04-27')
    def test_link_for_operations(self, m_get_interface, flags, ops):
        m_link = mock.MagicMock(return_value='http://example.com/foo/bar')
        m_get_interface.return_value.link_to = m_link

        results = self.invoke(*flags, 'foo/bar')
        assert results.output == 'http://example.com/foo/bar\n'
        m_link.assert_called_once_with(
            path='foo/bar',
            operation=ops,
            expire=arrow.get(2020, 4, 27, 1)
        )

    def test_link_for_error(self, m_get_interface):
        m_get_interface.return_value.link_to.side_effect = ValueError('Some error')

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
