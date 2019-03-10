from datetime import datetime

from .helpers import ObjectId, RE_TYPE
from . import OperationFailure

import numbers
import operator
import re
from sentinels import NOTHING
from six import iteritems, iterkeys, string_types
try:
    from types import NoneType
except ImportError:
    NoneType = type(None)

COMPILED_RE_TYPE = type(re.compile('a'))


def filter_applies(search_filter, document):
    """Applies given filter

    This function implements MongoDB's matching strategy over documents in the find() method
    and other related scenarios (like $elemMatch)
    """
    if not isinstance(search_filter, dict):
        raise OperationFailure('the match filter must be an expression in an object')

    for key, search in iteritems(search_filter):
        # Top level operators.
        if key == '$comment':
            continue
        if key in LOGICAL_OPERATOR_MAP:
            if not search:
                raise OperationFailure('BadValue $and/$or/$nor must be a nonempty array')
            if not LOGICAL_OPERATOR_MAP[key](document, search):
                return False
            continue
        if key.startswith('$'):
            raise OperationFailure('unknown top level operator: ' + key)

        is_match = False

        is_checking_negative_match = \
            isinstance(search, dict) and {'$ne', '$nin'} & set(search.keys())
        is_checking_positive_match = \
            not isinstance(search, dict) or (set(search.keys()) - {'$ne', '$nin'})
        has_candidates = False

        if search == {'$exists': False} and not iter_key_candidates(key, document):
            continue

        for doc_val in iter_key_candidates(key, document):
            has_candidates |= doc_val is not NOTHING
            if isinstance(search, dict):
                if '$options' in search and '$regex' in search:
                    search = _combine_regex_options(search)
                is_match = (all(
                    operator_string in OPERATOR_MAP and
                    OPERATOR_MAP[operator_string](doc_val, search_val) or
                    operator_string == '$not' and
                    _not_op(document, key, search_val)
                    for operator_string, search_val in iteritems(search)
                ) and search) or doc_val == search
            elif isinstance(search, RE_TYPE) and isinstance(doc_val, (string_types, list)):
                is_match = _regex(doc_val, search)
            elif key in LOGICAL_OPERATOR_MAP:
                if not search:
                    raise OperationFailure('BadValue $and/$or/$nor must be a nonempty array')
                is_match = LOGICAL_OPERATOR_MAP[key](document, search)
            elif isinstance(doc_val, (list, tuple)):
                is_match = (search in doc_val or search == doc_val)
                if isinstance(search, ObjectId):
                    is_match |= (str(search) in doc_val)
            else:
                is_match = (doc_val == search) or (search is None and doc_val is NOTHING)

            # When checking negative match, all the elements should match.
            if is_checking_negative_match and not is_match:
                return False

            # If not checking negative matches, the first match is enouh for this criteria.
            if is_match and not is_checking_negative_match:
                break

        if not is_match and (has_candidates or is_checking_positive_match):
            return False

    return True


def iter_key_candidates(key, doc):
    """Get possible subdocuments or lists that are referred to by the key in question

    Returns the appropriate nested value if the key includes dot notation.
    """
    if doc is None:
        return ()

    if not key:
        return [doc]

    if isinstance(doc, list):
        return _iter_key_candidates_sublist(key, doc)

    if not isinstance(doc, dict):
        return ()

    key_parts = key.split('.')
    if len(key_parts) == 1:
        return [doc.get(key, NOTHING)]

    sub_key = '.'.join(key_parts[1:])
    sub_doc = doc.get(key_parts[0], {})
    return iter_key_candidates(sub_key, sub_doc)


def _iter_key_candidates_sublist(key, doc):
    """Iterates of candidates

    :param doc: a list to be searched for candidates for our key
    :param key: the string key to be matched
    """
    key_parts = key.split('.')
    sub_key = key_parts.pop(0)
    key_remainder = '.'.join(key_parts)
    try:
        sub_key_int = int(sub_key)
    except ValueError:
        sub_key_int = None

    if sub_key_int is None:
        # subkey is not an integer...
        return [x
                for sub_doc in doc
                if isinstance(sub_doc, dict) and sub_key in sub_doc
                for x in iter_key_candidates(key_remainder, sub_doc[sub_key])]

    # subkey is an index
    if sub_key_int >= len(doc):
        return ()  # dead end
    sub_doc = doc[sub_key_int]
    if key_parts:
        return iter_key_candidates('.'.join(key_parts), sub_doc)
    return [sub_doc]


def _force_list(v):
    return v if isinstance(v, (list, tuple)) else [v]


def _all_op(doc_val, search_val):
    dv = _force_list(doc_val)
    matches = []
    for x in search_val:
        if isinstance(x, dict) and '$elemMatch' in x:
            matches.append(_elem_match_op(doc_val, x['$elemMatch']))
        else:
            matches.append(x in dv)
    return all(matches)


def _in_op(doc_val, search_val):
    if doc_val is NOTHING and None in search_val:
        return True
    doc_val = _force_list(doc_val)
    is_regex_list = [isinstance(x, COMPILED_RE_TYPE) for x in search_val]
    if not any(is_regex_list):
        return any(x in search_val for x in doc_val)
    for x, is_regex in zip(search_val, is_regex_list):
        if (is_regex and _regex(doc_val, x)) or (x in doc_val):
            return True
    return False


def _not_op(d, k, s):
    if isinstance(s, dict):
        for key in s.keys():
            if key == '$regex':
                raise OperationFailure('$not cannot have a regex')
            if key not in OPERATOR_MAP and key not in LOGICAL_OPERATOR_MAP:
                raise OperationFailure('unknown operator: %s' % key)
    elif isinstance(s, type(re.compile(''))):
        pass
    else:
        raise OperationFailure('$not needs a regex or a document')
    return not filter_applies({k: s}, d)


def _not_nothing_and(f):
    """wrap an operator to return False if the first arg is NOTHING"""
    return lambda v, l: v is not NOTHING and f(v, l)


def _compare_objects(op):
    """Wrap an operator to also compare objects following BSON comparison.

    See https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#objects
    """
    def _wrapped(a, b):
        # Do not compare uncomparable types, see Type Bracketing:
        # https://docs.mongodb.com/manual/reference/method/db.collection.find/#type-bracketing
        return bson_compare(op, a, b, can_compare_types=False)

    return _wrapped


def bson_compare(op, a, b, can_compare_types=True):
    """Compare two elements using BSON comparison.

    Args:
        op: the basic operation to compare (e.g. operator.lt, operator.ge).
        a: the first operand
        b: the second operand
        can_compare_types: if True, according to BSON's definition order
            between types is used, otherwise always return False when types are
            different.
    """
    a_type = _get_compare_type(a)
    b_type = _get_compare_type(b)
    if a_type != b_type:
        return can_compare_types and op(a_type, b_type)

    if isinstance(a, dict):
        # MongoDb server compares the type before comparing the keys
        # https://github.com/mongodb/mongo/blob/f10f214/src/mongo/bson/bsonelement.cpp#L516
        # even though the documentation does not say anything about that.
        a = [(_get_compare_type(v), k, v) for k, v in iteritems(a)]
        b = [(_get_compare_type(v), k, v) for k, v in iteritems(b)]

    if isinstance(a, (tuple, list)):
        for item_a, item_b in zip(a, b):
            if item_a != item_b:
                return bson_compare(op, item_a, item_b)
        return bson_compare(op, len(a), len(b))

    if isinstance(a, NoneType):
        return op(0, 0)

    return op(a, b)


def _get_compare_type(val):
    """Get a number representing the base type of the value used for comparison.

    See https://docs.mongodb.com/manual/reference/bson-type-comparison-order/
    also https://github.com/mongodb/mongo/blob/46b28bb/src/mongo/bson/bsontypes.h#L175
    for canonical values.
    """
    if isinstance(val, NoneType):
        return 5
    if isinstance(val, bool):
        return 40
    if isinstance(val, numbers.Number):
        return 10
    if isinstance(val, string_types):
        return 15
    if isinstance(val, dict):
        return 20
    if isinstance(val, (tuple, list)):
        return 25
    if isinstance(val, ObjectId):
        return 35
    if isinstance(val, datetime):
        return 45
    if isinstance(val, RE_TYPE):
        return 50
    raise NotImplementedError(
        "Mongomock does not know how to sort '%s' of type '%s'" %
        (val, type(val)))


def _elem_match_op(doc_val, query):
    if not isinstance(doc_val, list):
        return False
    if not isinstance(query, dict):
        raise OperationFailure('$elemMatch needs an Object')
    return any(filter_applies(query, item) for item in doc_val)


def _regex(doc_val, regex):
    if not (isinstance(doc_val, (string_types, list)) or isinstance(doc_val, RE_TYPE)):
        return False
    return any(
        regex.search(item) for item in _force_list(doc_val)
        if isinstance(item, string_types))


def _size_op(doc_val, search_val):
    if isinstance(doc_val, (list, tuple, dict)):
        return search_val == len(doc_val)
    return search_val == 1 if doc_val else 0


def _list_expand(f):
    def func(doc_val, search_val):
        if isinstance(doc_val, list):
            for val in doc_val:
                if f(val, search_val):
                    return True
            return False
        return f(doc_val, search_val)
    return func


def _type_op(doc_val, search_val):
    if search_val not in TYPE_MAP:
        raise OperationFailure('%r is not a valid $type' % search_val)
    elif TYPE_MAP[search_val] is None:
        raise NotImplementedError('%s is a valid $type but not implemented' % search_val)
    return isinstance(doc_val, TYPE_MAP[search_val])


def _combine_regex_options(search):
    if not isinstance(search['$options'], string_types):
        raise OperationFailure('$options has to be a string')

    options = None
    for option in search['$options']:
        if option not in 'imxs':
            continue
        re_option = getattr(re, option.upper())
        if options is None:
            options = re_option
        else:
            options |= re_option

    search_copy = dict(search)
    del search_copy['$options']

    if options is None:
        return search_copy

    if isinstance(search['$regex'], COMPILED_RE_TYPE):
        keys = [k for k in iterkeys(search) if k in {'$regex', '$options'}]
        if keys == ['$options', '$regex']:
            raise NotImplementedError(
                'Do not use compiled regular expressions with $options until '
                'https://jira.mongodb.org/browse/SERVER-38621 is solved.')
        search_copy['$regex'] = re.compile(
            search['$regex'].pattern, search['$regex'].flags | options)
    else:
        search_copy['$regex'] = re.compile(search['$regex'], options)
    return search_copy


def operator_eq(doc_val, search_val):
    if doc_val is NOTHING and search_val is None:
        return True
    return operator.eq(doc_val, search_val)


SORTING_OPERATOR_MAP = {
    '$gt': operator.gt,
    '$gte': operator.ge,
    '$lt': operator.lt,
    '$lte': operator.le,
}


OPERATOR_MAP = dict({
    '$eq': operator_eq,
    '$ne': lambda dv, sv: not operator_eq(dv, sv),
    '$all': _all_op,
    '$in': _in_op,
    '$nin': lambda dv, sv: not _in_op(dv, sv),
    '$exists': lambda dv, sv: bool(sv) == (dv is not NOTHING),
    '$regex': _not_nothing_and(lambda dv, sv: _regex(dv, re.compile(sv))),
    '$elemMatch': _elem_match_op,
    '$size': _size_op,
    '$type': _type_op
}, **{
    key: _not_nothing_and(_list_expand(_compare_objects(op)))
    for key, op in iteritems(SORTING_OPERATOR_MAP)
})


LOGICAL_OPERATOR_MAP = {
    '$or': lambda d, subq: any(filter_applies(q, d) for q in subq),
    '$and': lambda d, subq: all(filter_applies(q, d) for q in subq),
    '$nor': lambda d, subq: all(not filter_applies(q, d) for q in subq),
}

TYPE_MAP = {
    'double': (float,),
    'string': (str,),
    'object': (dict,),
    'array': (list,),
    'binData': (bytes,),
    'undefined': None,
    'objectId': (ObjectId,),
    'bool': (bool,),
    'date': (datetime,),
    'null': None,
    'regex': None,
    'dbPointer': None,
    'javascript': None,
    'symbol': None,
    'javascriptWithScope': None,
    'int': (int,),
    'timestamp': None,
    'long': (float,),
    'decimal': (float,),
    'minKey': None,
    'maxKey': None,
}


def resolve_key(key, doc):
    return next(iter(iter_key_candidates(key, doc)), NOTHING)


def resolve_sort_key(key, doc):
    value = resolve_key(key, doc)
    # see http://docs.mongodb.org/manual/reference/method/cursor.sort/#ascending-descending-sort
    if value is NOTHING:
        return 0, value

    return 1, value


class BsonComparable(object):
    """Wraps a value in an BSON like object that can be compared one to another."""

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        return bson_compare(operator.lt, self.obj, other.obj)
