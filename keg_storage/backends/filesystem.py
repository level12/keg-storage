import os
import string
from operator import attrgetter
from typing import (
    List,
    Optional,
    Union,
)

import pathlib

import arrow

from keg_storage.backends import base


class LocalFSError(Exception):
    pass


class LocalFSFile(base.RemoteFile):
    def __init__(self, path: pathlib.Path, mode: base.FileMode):
        self.path = path
        if mode & base.FileMode.read and mode & base.FileMode.write:
            str_mode = 'r+b'
        else:
            str_mode = str(mode)
        self.fp = path.open(str_mode)
        super().__init__(mode)

    def read(self, size: int) -> bytes:
        return self.fp.read(size)

    def write(self, data: bytes) -> None:
        self.fp.write(data)

    def close(self):
        self.fp.close()


class LocalFSStorage(base.InternalLinksStorageBackend):
    disallowed_path_chars = frozenset(set('~?*' + string.whitespace) - {' '})

    def __init__(
            self,
            root: Union[str, pathlib.Path],
            linked_endpoint: Optional[str] = None,
            secret_key: Optional[bytes] = None,
            name: str = None
    ):
        self.root = pathlib.Path(root).resolve()
        if not self.root.is_dir():
            raise LocalFSError('Storage root does not exist or is not a directory')

        super().__init__(
            linked_endpoint=linked_endpoint,
            secret_key=secret_key,
            name=name if name is not None else f'fs-{root.name}'
        )

    def _is_under_root(self, path: pathlib.Path) -> bool:
        try:
            path.resolve().relative_to(self.root)
        except ValueError:
            return False
        return True

    def _resolve_path(self, path: str) -> pathlib.Path:
        return self.root.joinpath(path).resolve()

    def _is_file(self, path: pathlib.Path) -> bool:
        return path.is_file() and not path.is_symlink()

    def _validate_path(self, path: str):
        if any(c in self.disallowed_path_chars for c in path):
            raise ValueError('Unsupported characters in path')

    def list(self, path: str) -> List[base.ListEntry]:
        self._validate_path(path)
        resolved_path = self._resolve_path(path)
        if not self._is_under_root(resolved_path):
            # prevent escaping the root by including `..` or symlinks in `path`
            raise LocalFSError('Invalid path')

        if not resolved_path.is_dir():
            raise LocalFSError(f'{path} does not exist or is not a directory')

        lst = []
        for dir, _, files in os.walk(resolved_path, followlinks=False):
            for f in files:
                full_path = pathlib.Path(dir).joinpath(f)
                if not self._is_file(full_path):
                    # Skip symlinks and special files like sockets
                    continue

                full_path = full_path.resolve()
                if not self._is_under_root(full_path):
                    continue

                st = full_path.stat()
                rel_path = full_path.relative_to(self.root)

                lst.append(base.ListEntry(
                    name=str(rel_path),
                    size=st.st_size,
                    last_modified=arrow.get(st.st_mtime)
                ))
        return sorted(lst, key=attrgetter('name'))

    def open(self, path: str, mode: Union[base.FileMode, str]):
        self._validate_path(path)
        path = self._resolve_path(path)
        if not self._is_under_root(path):
            raise LocalFSError('Invalid path')
        if path.exists() and not self._is_file(path):
            raise LocalFSError('Invalid path')

        mode = base.FileMode.as_mode(mode)

        if mode & base.FileMode.write:
            path.parent.mkdir(parents=True, exist_ok=True)

        return LocalFSFile(path, mode)

    def delete(self, path: str):
        self._validate_path(path)
        path = self._resolve_path(path)
        if not self._is_under_root(path):
            raise LocalFSError('Invalid path')
        if not path.exists():
            return

        if not self._is_file(path):
            raise LocalFSError('Invalid path')

        path.unlink()
