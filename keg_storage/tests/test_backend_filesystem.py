import os
import pathlib
import random
from typing import Callable

import pytest
from blazeutils import randchars

from keg_storage import backends


class TestLocalFSStorage:
    def test_init(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('real')
        root.mkdir()
        tmp_path.joinpath('link').symlink_to(root, target_is_directory=True)

        fs = backends.LocalFSStorage(root)
        assert fs.root == tmp_path / 'real'
        assert fs.name == 'fs-real'

        fs = backends.LocalFSStorage(tmp_path / 'link')
        assert fs.root == tmp_path / 'real'
        assert fs.name == 'fs-link'

    def test_init_not_dir(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('foo')
        root.touch()

        with pytest.raises(backends.LocalFSError) as exc:
            backends.LocalFSStorage(root)
        assert str(exc.value) == 'Storage root does not exist or is not a directory'

    def test_init_does_not_exist(self, tmp_path: pathlib.Path):
        with pytest.raises(backends.LocalFSError) as exc:
            backends.LocalFSStorage(tmp_path / 'foo')
        assert str(exc.value) == 'Storage root does not exist or is not a directory'

    def test_list_success(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')

        dir1 = root.joinpath('dir1')
        dir2 = dir1.joinpath('dir2')
        dir2.mkdir(parents=True)

        dir3 = root.joinpath('dir3')
        dir3.mkdir()

        # empty dir should be skipped
        root.joinpath('dir4').mkdir()

        # create some files that will be listed
        dir1.joinpath('file1.txt').touch()
        dir1.joinpath('file2.txt').touch()
        dir2.joinpath('file1.txt').touch()
        with dir3.joinpath('file3.txt').open('wb') as fp:
            fp.write(os.urandom(100))

        # create links that should be skipped
        dir3.joinpath('link1').symlink_to(dir2, target_is_directory=True)
        dir3.joinpath('link2.txt').symlink_to(dir2 / 'file1.txt', target_is_directory=False)

        # create a FIFO that should be skipped
        os.mkfifo(dir3 / 'pipe')

        # create an link to an external directory that should be skipped
        external = tmp_path.joinpath('external')
        external.mkdir()
        dir3.joinpath('link3').symlink_to(external, target_is_directory=True)

        fs = backends.LocalFSStorage(root)
        results = fs.list('')

        assert len(results) == 4
        assert results[0].name == 'dir1/dir2/file1.txt'
        assert results[0].size == 0

        assert results[1].name == 'dir1/file1.txt'
        assert results[1].size == 0

        assert results[2].name == 'dir1/file2.txt'
        assert results[2].size == 0

        assert results[3].name == 'dir3/file3.txt'
        assert results[3].size == 100

        results = fs.list('dir1')
        assert len(results) == 3
        assert results[0].name == 'dir1/dir2/file1.txt'
        assert results[0].size == 0

        assert results[1].name == 'dir1/file1.txt'
        assert results[1].size == 0

        assert results[2].name == 'dir1/file2.txt'
        assert results[2].size == 0

        results = fs.list('dir1/dir2')
        assert len(results) == 1
        assert results[0].name == 'dir1/dir2/file1.txt'
        assert results[0].size == 0

    def test_list_failures(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')

        ext1 = tmp_path.joinpath('ext1')
        ext2 = ext1.joinpath('ext2')
        ext2.mkdir(parents=True)
        ext2.joinpath('file1.txt').touch()

        dir1 = root.joinpath('dir1')
        dir1.mkdir(parents=True)
        dir1.joinpath('file2.txt').touch()

        link = dir1.joinpath('link')
        link.symlink_to(ext1, target_is_directory=True)

        fs = backends.LocalFSStorage(root)

        # Attempt to break sandbox with a link
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.list('dir1/link')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.list('dir1/link/ext2')

        # Attempt to break sandbox with a relative path
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.list('dir1/../../')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.list('dir1/../../external')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.list('/tmp')

        with pytest.raises(ValueError, match='Unsupported characters in path'):
            fs.list('~/')

        # Attempt to list a file
        with pytest.raises(backends.LocalFSError,
                           match='dir1/file2.txt does not exist or is not a directory'):
            fs.list('dir1/file2.txt')

        # Attempt to list a directory that does not exist
        with pytest.raises(backends.LocalFSError,
                           match='dir2 does not exist or is not a directory'):
            fs.list('dir2')

    def test_open_for_reading(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        file_path = root.joinpath('file.txt')
        file_data = os.urandom(100)
        with file_path.open('wb') as fp:
            fp.write(file_data)

        fs = backends.LocalFSStorage(root)

        with fs.open('file.txt', backends.FileMode.read) as f:
            assert f.read(10) == file_data[:10]
            assert f.read(100) == file_data[10:]

        assert f.fp.closed is True

        with fs.open('file.txt', 'r') as f:
            assert f.read(100) == file_data

        assert f.fp.closed is True

    def test_open_for_writing(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        file_path = root.joinpath('file.txt')

        fs = backends.LocalFSStorage(root)

        # File does not exist yet
        file_data1 = os.urandom(100)
        with fs.open('file.txt', backends.FileMode.write) as f:
            f.write(file_data1)

        with file_path.open('rb') as fp:
            assert fp.read() == file_data1
        assert f.fp.closed is True

        # Existing file
        file_data2 = os.urandom(200)
        with fs.open('file.txt', 'w') as f:
            f.write(file_data2)

        with file_path.open('rb') as fp:
            assert fp.read() == file_data2
        assert f.fp.closed is True

    def test_open_for_writing_creates_directories(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()

        fs = backends.LocalFSStorage(root)
        with fs.open('dir1/dir2/file.txt', backends.FileMode.write):
            pass

        assert root.joinpath('dir1', 'dir2').is_dir()

    def test_open_for_read_write(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        file_path = root.joinpath('file.txt')
        file_data = os.urandom(100)
        with file_path.open('wb') as fp:
            fp.write(file_data)

        fs = backends.LocalFSStorage(root)

        new_data = os.urandom(100)
        with fs.open('file.txt', backends.FileMode.read | backends.FileMode.write) as f:
            assert f.read(10) == file_data[:10]
            f.write(new_data)

        with file_path.open('rb') as fp:
            assert fp.read() == file_data[:10] + new_data
        assert f.fp.closed is True

    def test_open_failures(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        root.joinpath('rootfile.txt').touch()

        ext1 = tmp_path.joinpath('ext1')
        ext1.mkdir(parents=True)
        ext1.joinpath('file1.txt').touch()

        dir1 = root.joinpath('dir1')
        dir1.mkdir(parents=True)
        dir1.joinpath('file2.txt').touch()

        link1 = dir1.joinpath('link')
        link1.symlink_to(ext1, target_is_directory=True)

        link2 = dir1.joinpath('link.txt')
        link2.symlink_to(ext1 / 'file1.txt', target_is_directory=False)

        fs = backends.LocalFSStorage(root)

        # Attempt to break sandbox with a link
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1/link', 'r')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1/link.txt', 'r')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1/link/file1.txt', 'r')

        # Attempt to break sandbox with a relative path
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1/../../rootfile.txt', 'r')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1/../../external/file1.txt', 'r')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('/tmp/file.txt', 'w')

        with pytest.raises(ValueError):
            fs.open('~/file.txt', 'w')

        # Attempt to open non-files
        os.mkfifo(root / 'fifo')
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('fifo', 'r')

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1', 'r')

    def test_delete_success(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        file_path = root.joinpath('file.txt')
        file_path.touch()

        fs = backends.LocalFSStorage(root)

        assert file_path.exists()
        fs.delete('file.txt')
        assert not file_path.exists()

        # None existant file should return early
        fs.delete('file2.txt')

    def test_delete_failures(self, tmp_path: pathlib.Path):
        root = tmp_path.joinpath('root')
        root.mkdir()
        root.joinpath('rootfile.txt').touch()

        ext1 = tmp_path.joinpath('ext1')
        ext1.mkdir(parents=True)
        ext1.joinpath('file1.txt').touch()

        dir1 = root.joinpath('dir1')
        dir1.mkdir(parents=True)
        dir1.joinpath('file2.txt').touch()

        link1 = dir1.joinpath('link')
        link1.symlink_to(ext1, target_is_directory=True)

        link2 = dir1.joinpath('link.txt')
        link2.symlink_to(ext1 / 'file1.txt', target_is_directory=False)

        fs = backends.LocalFSStorage(root)

        # Attempt to break sandbox with a link
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('dir1/link')
        assert link1.exists()

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('dir1/link.txt')
        assert link2.exists()

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('dir1/link/file1.txt')
        assert ext1.joinpath('file1.txt').exists()

        # Attempt to break sandbox with a relative path
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('dir1/../../rootfile.txt')
        assert root.joinpath('rootfile.txt').exists()

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('dir1/../../external/file1.txt')
        assert ext1.joinpath('file1.txt').exists()

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('/tmp/file.txt')

        with pytest.raises(ValueError):
            fs.delete('~/file.txt')

        # Attempt to delete non-files
        os.mkfifo(root / 'fifo')
        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.delete('fifo')
        assert root.joinpath('fifo').exists()

        with pytest.raises(backends.LocalFSError, match='Invalid path'):
            fs.open('dir1', 'r')
        assert dir1.exists()

    @pytest.mark.parametrize('method,args', [
        (backends.LocalFSStorage.list, tuple()),
        (backends.LocalFSStorage.open, ('w',)),
        (backends.LocalFSStorage.delete, tuple()),
    ])
    @pytest.mark.parametrize('char', ['~', '?', '*', '\t', '\n', '\r', '\x0b', '\x0c'])
    def test_validates_path(self, method: Callable, args: tuple, char: str, tmp_path: pathlib.Path):
        fs = backends.LocalFSStorage(tmp_path)
        chars = list(randchars()) + [char]
        random.shuffle(chars)
        file_name = ''.join(chars)

        with pytest.raises(ValueError, match='Unsupported characters in path'):
            method(fs, file_name, *args)
