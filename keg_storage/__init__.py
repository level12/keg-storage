from importlib.util import find_spec as _find_spec

if _find_spec('keg'):
    from keg_storage.plugin import Storage  # noqa
from keg_storage.backends import *  # noqa
