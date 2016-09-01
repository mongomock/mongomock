from mongomock import InvalidURI
import re
from six.moves.urllib_parse import unquote_plus
from six import iteritems, PY2
import warnings


try:
    from bson import (ObjectId, RE_TYPE)
except ImportError:
    from mongomock.object_id import ObjectId  # noqa
    RE_TYPE = re._pattern_type

# for Python 3 compatibility
if PY2:
    from __builtin__ import basestring
else:
    basestring = (str, bytes)

ASCENDING = 1


def print_deprecation_warning(old_param_name, new_param_name):
    warnings.warn(
        "'%s' has been deprecated to be in line with pymongo implementation, a new parameter '%s' "
        "should be used instead. the old parameter will be kept for backward compatibility "
        "purposes." % (old_param_name, new_param_name), DeprecationWarning)


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

       It takes such a list, or a single key, or a single key and direction.
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
    """hashable dict implementation, suitable for use as a key into other dicts.

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


def parse_dbase_from_uri(uri):
    """A simplified version of pymongo.uri_parser.parse_uri to get the dbase.

    Returns a string representing the database provided in the URI or None if
    no database is provided in the URI.

    An invalid MongoDB connection URI may raise an InvalidURI exception,
    however, the URI is not fully parsed and some invalid URIs may not result
    in an exception.

    "mongodb://host1/database" becomes "database"

    and

    "mongodb://host1" becomes None
    """
    SCHEME = "mongodb://"

    if not uri.startswith(SCHEME):
        raise InvalidURI("Invalid URI scheme: URI "
                         "must begin with '%s'" % (SCHEME,))

    scheme_free = uri[len(SCHEME):]

    if not scheme_free:
        raise InvalidURI("Must provide at least one hostname or IP.")

    dbase = None

    # Check for unix domain sockets in the uri
    if '.sock' in scheme_free:
        host_part, _, path_part = scheme_free.rpartition('/')
        if not host_part:
            host_part = path_part
            path_part = ""
        if '/' in host_part:
            raise InvalidURI("Any '/' in a unix domain socket must be"
                             " URL encoded: %s" % host_part)
        path_part = unquote_plus(path_part)
    else:
        host_part, _, path_part = scheme_free.partition('/')

    if not path_part and '?' in host_part:
        raise InvalidURI("A '/' is required between "
                         "the host list and any options.")

    if path_part:
        if path_part[0] == '?':
            opts = path_part[1:]
        else:
            dbase, _, opts = path_part.partition('?')
            if '.' in dbase:
                dbase, _ = dbase.split('.', 1)

    if dbase is not None:
        dbase = unquote_plus(dbase)

    return dbase


def embedded_item_getter(*keys):
    """Get items from embedded dictionaries.

    use case:
    d = {"a": {"b": 1}}
    embedded_item_getter("a.b")(d) == 1

    :param keys: keys to get
                 embedded keys are separated with dot in string
    :return: itemgetter function
    """

    def recurse_embedded(obj, key):
        ret = obj
        for k in key.split('.'):
            ret = ret[k]
        return ret

    if len(keys) == 1:
        item = keys[0]

        def g(obj):
            return recurse_embedded(obj, item)

    else:

        def g(obj):
            return tuple(recurse_embedded(obj, item) for item in keys)

    return g
