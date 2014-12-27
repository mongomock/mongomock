import sys
import re

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
