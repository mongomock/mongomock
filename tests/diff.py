from platform import python_version

class _NO_VALUE(object):
    pass
NO_VALUE = _NO_VALUE() # we don't use NOTHING because it might be returned from various APIs

_SUPPORTED_TYPES = set([
    int, float, bool, str
])

if python_version() < "3.0":
    _SUPPORTED_TYPES.update([long, basestring, unicode])

def diff(a, b, path=None):
    path = _make_path(path)
    if type(a) in (list, tuple):
        return _diff_sequences(a, b, path)
    if isinstance(a, dict):
        return _diff_dicts(a, b, path)
    if type(a).__name__ == "ObjectId":
        a = str(a)
    if type(b).__name__ == "ObjectId":
        b = str(b)
    if type(a) not in _SUPPORTED_TYPES:
        raise NotImplementedError("Unsupported diff type: {0}".format(type(a))) # pragma: no cover
    if type(b) not in _SUPPORTED_TYPES:
        raise NotImplementedError("Unsupported diff type: {0}".format(type(b))) # pragma: no cover
    if a != b:
        return [(path[:], a, b)]
    return []

def _diff_dicts(a, b, path):
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
    for i in range(len(a)):
        path.append(i)
        returned.extend(diff(a[i], b[i], path))
        path.pop()
    return returned

def _make_path(path):
    if path is None:
        return []
    return path
