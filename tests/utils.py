"""Testing utilities."""


class DontCare(object):  # pragma: no cover
    """A placeholder object that can conform to almost anything and is always equal to anything it
    is compared against.

    Examples:
        assert [1, 2, 3] == [1, DontCare(), 3]
    """
    __eq__ = lambda *_: True
    __ne__ = lambda *_: False
    __repr__ = lambda *_: '_'

    __getattr__ = lambda *_: DontCare()
    __call__ = lambda *a, **kw: DontCare()