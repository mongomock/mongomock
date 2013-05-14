#for Python 3 compatibility
try:
  unicode = unicode
except NameError:
  unicode = str
  basestring = (str, bytes)

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