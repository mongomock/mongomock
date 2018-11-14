from collections import OrderedDict
from datetime import datetime, timedelta, tzinfo
from mongomock import InvalidURI
import re
from six.moves.urllib_parse import unquote_plus
from six import iteritems, string_types
import warnings


# Get ObjectId from bson if available or import a crafted one. This is not used
# in this module but is made available for callers of this module.
try:
    from bson import ObjectId  # pylint: disable=unused-import
except ImportError:
    from mongomock.object_id import ObjectId  # noqa

# Cache the RegExp pattern type.
RE_TYPE = type(re.compile(''))

try:
    from bson.tz_util import utc
except ImportError:
    class _FixedOffset(tzinfo):

        def __init__(self, offset, name):
            self.__offset = timedelta(minutes=offset)
            self.__name = name

        def __getinitargs__(self):
            return self.__offset, self.__name

        def utcoffset(self, dt):
            return self.__offset

        def tzname(self, dt):
            return self.__name

        def dst(self, dt):
            return timedelta(0)
    utc = _FixedOffset(0, 'UTC')


ASCENDING = 1
DESCENDING = -1


def print_deprecation_warning(old_param_name, new_param_name):
    warnings.warn(
        "'%s' has been deprecated to be in line with pymongo implementation, a new parameter '%s' "
        'should be used instead. the old parameter will be kept for backward compatibility '
        'purposes.' % (old_param_name, new_param_name), DeprecationWarning)


def create_index_list(key_or_list):
    """Helper to generate a list of (key, direction) pairs.

       It takes such a list, or a single key, or a single key and direction.
    """
    if isinstance(key_or_list, string_types):
        return [(key_or_list, ASCENDING)]
    if not isinstance(key_or_list, (list, tuple)):
        raise TypeError('if no direction is specified, '
                        'key_or_list must be an instance of list')
    return key_or_list


def gen_index_name(index_list):
    """Generate an index name based on the list of keys with directions."""

    return '_'.join('_'.join([str(i) for i in ix]) for ix in index_list)


class hashdict(dict):
    """hashable dict implementation, suitable for use as a key into other dicts.

    >>> h1 = hashdict({'apples': 1, 'bananas':2})
    >>> h2 = hashdict({'bananas': 3, 'mangoes': 5})
    >>> h1+h2
    hashdict(apples=1, bananas=3, mangoes=5)
    >>> d1 = {}
    >>> d1[h1] = 'salad'
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
                          hashdict(v) if isinstance(v, dict) else
                          tuple(v) if isinstance(v, list) else
                          v)
                         for k, v in iteritems(self))

    def __repr__(self):
        return '{0}({1})'.format(
            self.__class__.__name__,
            ', '.join('{0}={1}'.format(str(i[0]), repr(i[1])) for i in sorted(self.__key())))

    def __hash__(self):
        return hash(self.__key())

    def __setitem__(self, key, value):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def __delitem__(self, key):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def clear(self):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def pop(self, *args, **kwargs):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def popitem(self, *args, **kwargs):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def setdefault(self, *args, **kwargs):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def update(self, *args, **kwargs):
        raise TypeError('{0} does not support item assignment'
                        .format(self.__class__.__name__))

    def __add__(self, right):
        result = hashdict(self)
        dict.update(result, right)
        return result


def fields_list_to_dict(fields):
    """Takes a list of field names and returns a matching dictionary.

    ['a', 'b'] becomes {'a': 1, 'b': 1}

    and

    ['a.b.c', 'd', 'a.c'] becomes {'a.b.c': 1, 'd': 1, 'a.c': 1}
    """
    as_dict = {}
    for field in fields:
        if not isinstance(field, string_types):
            raise TypeError('fields must be a list of key names, '
                            'each an instance of %s' % (string_types[0].__name__,))
        as_dict[field] = 1
    return as_dict


def parse_dbase_from_uri(uri):
    """A simplified version of pymongo.uri_parser.parse_uri to get the dbase.

    Returns a string representing the database provided in the URI or None if
    no database is provided in the URI.

    An invalid MongoDB connection URI may raise an InvalidURI exception,
    however, the URI is not fully parsed and some invalid URIs may not result
    in an exception.

    'mongodb://host1/database' becomes 'database'

    and

    'mongodb://host1' becomes None
    """
    SCHEME = 'mongodb://'

    if not uri.startswith(SCHEME):
        raise InvalidURI('Invalid URI scheme: URI '
                         "must begin with '%s'" % (SCHEME,))

    scheme_free = uri[len(SCHEME):]

    if not scheme_free:
        raise InvalidURI('Must provide at least one hostname or IP.')

    dbase = None

    # Check for unix domain sockets in the uri
    if '.sock' in scheme_free:
        host_part, _, path_part = scheme_free.rpartition('/')
        if not host_part:
            host_part = path_part
            path_part = ''
        if '/' in host_part:
            raise InvalidURI("Any '/' in a unix domain socket must be"
                             ' URL encoded: %s' % host_part)
        path_part = unquote_plus(path_part)
    else:
        host_part, _, path_part = scheme_free.partition('/')

    if not path_part and '?' in host_part:
        raise InvalidURI("A '/' is required between "
                         'the host list and any options.')

    if path_part and path_part[0] != '?':
        dbase, _, _ = path_part.partition('?')
        if '.' in dbase:
            dbase, _ = dbase.split('.', 1)

    if dbase is not None:
        dbase = unquote_plus(dbase)

    return dbase


def embedded_item_getter(*keys):
    """Get items from embedded dictionaries.

    use case:
    d = {'a': {'b': 1}}
    embedded_item_getter('a.b')(d) == 1

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


def patch_datetime_awareness_in_document(value):
    # MongoDB is supposed to stock everything as timezone naive utc date
    # Hence we have to convert incoming datetimes to avoid errors while
    # mixing tz aware and naive.
    # On top of that, MongoDB date precision is up to millisecond, where Python
    # datetime use microsecond, so we must lower the precision to mimic mongo.
    for best_type in (OrderedDict, dict):
        if isinstance(value, best_type):
            return best_type((k, patch_datetime_awareness_in_document(v)) for k, v in value.items())
    if isinstance(value, (tuple, list)):
        return [patch_datetime_awareness_in_document(item) for item in value]
    if isinstance(value, datetime):
        mongo_us = (value.microsecond // 1000) * 1000
        if value.tzinfo:
            return (value - value.utcoffset()).replace(tzinfo=None, microsecond=mongo_us)
        return value.replace(microsecond=mongo_us)
    return value


def make_datetime_timezone_aware_in_document(value):
    # MongoClient support tz_aware=True parameter to return timezone-aware
    # datetime objects. Given the date is stored internally without timezone
    # information, all returned datetime have utc as timezone.
    if isinstance(value, dict):
        return {k: make_datetime_timezone_aware_in_document(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [make_datetime_timezone_aware_in_document(item) for item in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=utc)
    return value


def get_value_by_dot(doc, key):
    """Get dictionary value using dotted key"""
    result = doc
    for key_item in key.split('.'):
        if isinstance(result, dict):
            result = result[key_item]

        elif isinstance(result, (list, tuple)):
            try:
                result = result[int(key_item)]
            except (ValueError, IndexError):
                raise KeyError()

        else:
            raise KeyError()

    return result


def set_value_by_dot(doc, key, value):
    """Set dictionary value using dotted key"""
    try:
        parent_key, child_key = key.rsplit('.', 1)
        parent = get_value_by_dot(doc, parent_key)
    except ValueError:
        child_key = key
        parent = doc

    if isinstance(parent, dict):
        parent[child_key] = value
    elif isinstance(parent, (list, tuple)):
        try:
            parent[int(child_key)] = value
        except (ValueError, IndexError):
            raise KeyError()
    else:
        raise KeyError()

    return doc


def delete_value_by_dot(doc, key):
    """Delete dictionary value using dotted key.

    This function assumes that the value exists.
    """
    try:
        parent_key, child_key = key.rsplit('.', 1)
        parent = get_value_by_dot(doc, parent_key)
    except ValueError:
        child_key = key
        parent = doc

    del parent[child_key]

    return doc
