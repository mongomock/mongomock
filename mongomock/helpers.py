import re
import sys
import warnings
from six import (iteritems)


def print_deprecation_warning(old_param_name, new_param_name):
    warnings.warn("'%s' has been deprecated to be in line with pymongo implementation, "
                  "a new parameter '%s' should be used instead. the old parameter will be kept for backward "
                  "compatibility purposes." % old_param_name, new_param_name, DeprecationWarning)


_PY2 = sys.version_info < (3, 0)

try:
    from bson import (ObjectId, RE_TYPE)
except ImportError:
    from mongomock.object_id import ObjectId
    RE_TYPE = type(re.compile(''))

if _PY2:
    from __builtin__ import xrange
else:
    xrange = range

#for Python 3 compatibility
try:
  unicode = unicode
  from __builtin__ import basestring
except NameError:
  unicode = str
  basestring = (str, bytes)


ASCENDING = 1


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.
       Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, basestring):
            return [(key_or_list, ASCENDING)]
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError("if no direction is specified, "
                            "key_or_list must be an instance of list")
    return key_or_list


class hashdict(dict):
    """
    hashable dict implementation, suitable for use as a key into
    other dicts.

    >>> h1 = hashdict({"apples": 1, "bananas":2})
    >>> h2 = hashdict({"bananas": 3, "mangoes": 5})
    >>> h1+h2
    hashdict(apples=1, bananas=3, mangoes=5)
    >>> d1 = {}
    >>> d1[h1] = "salad"
    >>> d1[h1]
    'salad'
    >>> d1[h2]
    Traceback (most recent call last):
    ...
    KeyError: hashdict(bananas=3, mangoes=5)

    based on answers from
    http://stackoverflow.com/questions/1151658/python-hashable-dicts

    """
    def __key(self):
        return frozenset((k,
                          hashdict(v) if type(v) == dict else
                          tuple(v) if type(v) == list else
                          v)
                         for k, v in iteritems(self))

    def __repr__(self):
        return "{0}({1})".format(
            self.__class__.__name__,
            ", ".join("{0}={1}".format(str(i[0]), repr(i[1])) for i in self.__key()))

    def __hash__(self):
        return hash(self.__key())

    def __setitem__(self, key, value):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def __delitem__(self, key):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def clear(self):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def pop(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def popitem(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def setdefault(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def update(self, *args, **kwargs):
        raise TypeError("{0} does not support item assignment"
                        .format(self.__class__.__name__))

    def __add__(self, right):
        result = hashdict(self)
        dict.update(result, right)
        return result


def _fields_list_to_dict(fields):
    """Takes a list of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    as_dict = {}
    for field in fields:
        if not isinstance(field, basestring):
            raise TypeError("fields must be a list of key names, "
                            "each an instance of %s" % (basestring.__name__,))
        as_dict[field] = 1
    return as_dict
