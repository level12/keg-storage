from keg_storage.version import VERSION as __version__  # noqa
from importlib.util import find_spec as _find_spec

if _find_spec('keg'):
    from keg_storage.plugin import (  # noqa: F401
        Storage,
        LinkViewMixin,
    )
from keg_storage.backends import *  # noqa
