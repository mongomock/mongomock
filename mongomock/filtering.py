from .helpers import ObjectId, RE_TYPE
from . import OperationFailure

import operator
import re
from sentinels import NOTHING
from six import iteritems, string_types


def filter_applies(search_filter, document):
    """Applies given filter

    This function implements MongoDB's matching strategy over documents in the find() method
    and other related scenarios (like $elemMatch)
    """
    if search_filter is None:
        return True
    elif isinstance(search_filter, ObjectId):
        search_filter = {'_id': search_filter}

    for key, search in iteritems(search_filter):

        is_match = False

        if (isinstance(search, dict) and
                ('$ne' in search or search == {'$exists': False})
                and len(iter_key_candidates(key, document)) == 0):
            continue

        for doc_val in iter_key_candidates(key, document):
            if isinstance(search, dict):
                is_match = all(
                    operator_string in OPERATOR_MAP and
                    OPERATOR_MAP[operator_string](doc_val, search_val) or
                    operator_string == '$not' and
                    _not_op(document, key, search_val)
                    for operator_string, search_val in iteritems(search)
                ) or doc_val == search
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

            if is_match:
                break

        if not is_match:
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
    """Iterates of cadindates

    :param doc: a list to be searched for candidates for our key
    :param key: the string key to be matched
    """
    key_parts = key.split(".")
    sub_key = key_parts.pop(0)
    key_remainder = ".".join(key_parts)
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
    else:
        # subkey is an index
        if sub_key_int >= len(doc):
            return ()  # dead end
        sub_doc = doc[sub_key_int]
        if key_parts:
            return iter_key_candidates(".".join(key_parts), sub_doc)
        return [sub_doc]


def _force_list(v):
    return v if isinstance(v, (list, tuple)) else [v]


def _all_op(doc_val, search_val):
    dv = _force_list(doc_val)
    return all(x in dv for x in search_val)


def _not_op(d, k, s):
    if isinstance(s, dict):
        for key in s.keys():
            if key == '$regex':
                raise OperationFailure('BadValue $not cannot have a regex')
            if key not in OPERATOR_MAP and key not in LOGICAL_OPERATOR_MAP:
                raise OperationFailure('BadValue $not needs a regex or a document')
    elif isinstance(s, type(re.compile(''))):
        pass
    else:
        raise OperationFailure('BadValue $not needs a regex or a document')
    return not filter_applies({k: s}, d)


def _not_nothing_and(f):
    """wrap an operator to return False if the first arg is NOTHING"""
    return lambda v, l: v is not NOTHING and f(v, l)


def _elem_match_op(doc_val, query):
    if not isinstance(doc_val, list):
        return False
    return any(filter_applies(query, item) for item in doc_val)


def _regex(doc_val, regex):
    if not (isinstance(doc_val, (string_types, list)) or isinstance(doc_val, RE_TYPE)):
        return False
    return any(regex.search(item) for item in _force_list(doc_val))


def _size_op(doc_val, search_val):
    if isinstance(doc_val, (list, tuple, dict)):
        return search_val == len(doc_val)
    else:
        return search_val == 1 if doc_val else 0


OPERATOR_MAP = {
    '$eq': operator.eq,
    '$ne': operator.ne,
    '$gt': _not_nothing_and(operator.gt),
    '$gte': _not_nothing_and(operator.ge),
    '$lt': _not_nothing_and(operator.lt),
    '$lte': _not_nothing_and(operator.le),
    '$all': _all_op,
    '$in': lambda dv, sv: any(x in sv for x in _force_list(dv)),
    '$nin': lambda dv, sv: all(x not in sv for x in _force_list(dv)),
    '$exists': lambda dv, sv: bool(sv) == (dv is not NOTHING),
    '$regex': _not_nothing_and(lambda dv, sv: _regex(dv, re.compile(sv))),
    '$elemMatch': _elem_match_op,
    '$size': _size_op,
}


LOGICAL_OPERATOR_MAP = {
    '$or': lambda d, subq: any(filter_applies(q, d) for q in subq),
    '$and': lambda d, subq: all(filter_applies(q, d) for q in subq),
    '$nor': lambda d, subq: all(not filter_applies(q, d) for q in subq),
}
