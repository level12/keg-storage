# Keg-Storage

[![nox](https://github.com/level12/keg-storage/actions/workflows/nox.yaml/badge.svg)](https://github.com/level12/keg-storage/actions/workflows/nox.yaml)
[![codecov](https://codecov.io/gh/level12/keg-storage/branch/master/graph/badge.svg)](https://codecov.io/gh/level12/keg-storage)

Keg-Storage provides a common interface for storing files on the local
filesystem, Amazon S3, Azure Blob Storage, or an SFTP server. It can be used
directly or configured as a Flask/Keg extension with multiple named storage
profiles.


## Installation

Install the base package for local filesystem storage:

```shell
pip install KegStorage
```

Install the extra for each remote backend you use:

| Backend or integration | Extra | Install command |
| --- | --- | --- |
| Amazon S3 | `aws` | `pip install "KegStorage[aws]"` |
| Azure Blob Storage | `azure` | `pip install "KegStorage[azure]"` |
| SFTP | `sftp` | `pip install "KegStorage[sftp]"` |
| Flask/Keg helpers | `keg` | `pip install "KegStorage[keg]"` |

Keg-Storage requires Python 3.12 or newer.


## Usage

All backends implement the same operations, including `list`, `open`,
`upload`, `download`, `copy`, `delete`, and `link_to` where supported.

This example uses the local filesystem backend:

```python
from io import BytesIO
from pathlib import Path

from keg_storage import LocalFSStorage


storage_root = Path("var/storage")
storage_root.mkdir(parents=True, exist_ok=True)

storage = LocalFSStorage(root=storage_root, name="local")
storage.upload(BytesIO(b"Hello from Keg-Storage!"), "examples/hello.txt")

download = BytesIO()
storage.download("examples/hello.txt", download)
assert download.getvalue() == b"Hello from Keg-Storage!"
```


### Flask and Keg integration

Configure one or more named profiles and initialize the extension using the
standard Flask extension pattern:

```python
from pathlib import Path

from flask import Flask
from keg_storage import LocalFSStorage, Storage


storage = Storage()


def create_app():
    app = Flask(__name__)
    storage_root = Path("var/storage")
    storage_root.mkdir(parents=True, exist_ok=True)
    app.config["KEG_STORAGE_PROFILES"] = [
        (
            LocalFSStorage,
            {
                "name": "local",
                "root": storage_root,
            },
        ),
    ]
    storage.init_app(app)
    return app
```

Within an application context, retrieve a configured backend by name:

```python
backend = storage.get_interface("local")
backend.put("report.pdf", "reports/report.pdf")
```

The first configured profile is used when `get_interface()` is called without
a name. Initializing the extension also adds a `storage` command group to the
Flask CLI:

```shell
flask storage --location local list .
flask storage --location local put report.pdf reports/report.pdf
flask storage --location local get reports/report.pdf
```

See the documentation for backend configuration, temporary upload/download
links, and the `StorageOperations` helper for Flask views.


## Links

- [Documentation](https://keg-storage.readthedocs.io/en/stable/index.html)
- [Releases](https://pypi.org/project/KegStorage/)
- [Source and issue tracker](https://github.com/level12/keg-storage)
- [Changelog](changelog.rst)


## Development


### Copier template

Project structure and tooling mostly derive from
[Coppy](https://github.com/level12/coppy). See its documentation for context and
additional instructions. Update this project by following Coppy's
[template update instructions](https://github.com/level12/coppy/wiki#updating-a-project).


### Project setup

1. Install the [host dependencies](https://github.com/level12/coppy/wiki/Mise).
2. Run `uv sync`.
3. Install the hooks with `prek install`.
4. Run the checks with `nox`.


### Versions

Use `mise run version show` to show the current version and
`mise run version bump` to create a version commit and tag.
