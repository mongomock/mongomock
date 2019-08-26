import datetime
from platform import python_version
from six import integer_types, string_types, text_type


class _NO_VALUE(object):
    pass


# we don't use NOTHING because it might be returned from various APIs
NO_VALUE = _NO_VALUE()

_SUPPORTED_TYPES = (float, bool, str, datetime.datetime, type(None)) + \
    string_types + integer_types + (text_type, bytes)

if python_version() < '3.0':
    dict_type = dict
else:
    from collections import abc
    dict_type = abc.Mapping


def diff(a, b, path=None):
    path = _make_path(path)
    if isinstance(a, (list, tuple)):
        return _diff_sequences(a, b, path)
    if type(a).__name__ == 'SON':
        a = dict(a)
    if type(b).__name__ == 'SON':
        b = dict(b)
    if isinstance(a, dict_type):
        return _diff_dicts(a, b, path)
    if type(a).__name__ == 'ObjectId':
        a = str(a)
    if type(b).__name__ == 'ObjectId':
        b = str(b)
    if type(a).__name__ == 'Int64':
        a = int(a)
    if type(b).__name__ == 'Int64':
        b = int(b)
    if not isinstance(a, _SUPPORTED_TYPES):
        raise NotImplementedError(
            'Unsupported diff type: {0}'.format(type(a)))  # pragma: no cover
    if not isinstance(b, _SUPPORTED_TYPES):
        raise NotImplementedError(
            'Unsupported diff type: {0}'.format(type(b)))  # pragma: no cover
    if a != b:
        return [(path[:], a, b)]
    return []


def _diff_dicts(a, b, path):
    if not isinstance(a, type(b)):
        return [(path[:], type(a), type(b))]
    returned = []
    for key in set(a) | set(b):
        a_value = a.get(key, NO_VALUE)
        b_value = b.get(key, NO_VALUE)
        path.append(key)
        if a_value is NO_VALUE or b_value is NO_VALUE:
            returned.append((path[:], a_value, b_value))
        else:
            returned.extend(diff(a_value, b_value, path))
        path.pop()
    return returned


def _diff_sequences(a, b, path):
    if len(a) != len(b):
        return [(path[:], a, b)]
    returned = []
    for i, a_i in enumerate(a):
        path.append(i)
        returned.extend(diff(a_i, b[i], path))
        path.pop()
    return returned


def _make_path(path):
    if path is None:
        return []
    return path
