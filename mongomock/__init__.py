import operator
import warnings
import re

from sentinels import NOTHING
from six import (
    iteritems,
    itervalues,
    string_types,
    )

from .__version__ import __version__
from .object_id import ObjectId

__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']


RE_TYPE = type(re.compile(''))

def _force_list(v):
    return v if isinstance(v,(list,tuple)) else [v]

def _not_nothing_and(f):
    "wrap an operator to return False if the first arg is NOTHING"
    return lambda v,l: v is not NOTHING and f(v,l)

def _all_op(doc_val, search_val):
    dv = _force_list(doc_val)
    return all(x in dv for x in search_val)

def _print_deprecation_warning(old_param_name, new_param_name):
    warnings.warn("'%s' has been deprecated to be in line with pymongo implementation, "
                  "a new parameter '%s' should be used instead. the old parameter will be kept for backward "
                  "compatibility purposes." % old_param_name, new_param_name, DeprecationWarning)

OPERATOR_MAP = {'$ne': operator.ne,
                '$gt': _not_nothing_and(operator.gt),
                '$gte': _not_nothing_and(operator.ge),
                '$lt': _not_nothing_and(operator.lt),
                '$lte': _not_nothing_and(operator.le),
                '$all':_all_op,
                '$in':lambda dv,sv: any(x in sv for x in _force_list(dv)),
                '$nin':lambda dv,sv: all(x not in sv for x in _force_list(dv)),
                '$exists':lambda dv,sv: bool(sv)==(dv is not NOTHING),
               }


def resolve_key_value(key, doc):
    """Resolve keys to their proper value in a document.
    Returns the appropriate nested value if the key includes dot notation.
    """
    if not doc or not isinstance(doc, dict):
        return NOTHING
    else:
        key_parts = key.split('.')
        if len(key_parts) == 1:
            return doc.get(key, NOTHING)
        else:
            sub_key = '.'.join(key_parts[1:])
            sub_doc = doc.get(key_parts[0], {})
            return resolve_key_value(sub_key, sub_doc)

class Connection(object):
    def __init__(self, *args):
        super(Connection, self).__init__()
        self._databases = {}
    def __getitem__(self, db_name):
        db = self._databases.get(db_name, None)
        if db is None:
            db = self._databases[db_name] = Database(self)
        return db
    def __getattr__(self, attr):
        return self[attr]

class Database(object):
    def __init__(self, conn):
        super(Database, self).__init__()
        self._collections = {'system.indexes' : Collection(self)}
    def __getitem__(self, db_name):
        db = self._collections.get(db_name, None)
        if db is None:
            db = self._collections[db_name] = Collection(self)
        return db
    def __getattr__(self, attr):
        return self[attr]
    def collection_names(self):
        return list(self._collections.keys())

class Collection(object):
    def __init__(self, db):
        super(Collection, self).__init__()
        self._documents = {}
    def insert(self, data):
        if isinstance(data, list):
            return [self._insert(element) for element in data]
        return self._insert(data)
    def _insert(self, data):
        if '_id' in data:
            object_id = data['_id']
        else:
            object_id = ObjectId()
        assert object_id not in self._documents
        self._documents[object_id] = dict(data, _id=object_id)
        return object_id
    def update(self, spec, document):
        """Updates docuemnt(s) in the collection."""
        if '$set' in document:
            document = document['$set']
            clear_first = False
        else:
            clear_first = True

        for existing_document in self._iter_documents(spec):
            document_id = existing_document['_id']
            if clear_first:
                existing_document.clear()
            existing_document.update(document)
            existing_document['_id'] = document_id
    def find(self, spec=None, fields=None, filter=None):
        if filter is not None:
            _print_deprecation_warning('filter', 'spec')
            if spec is None:
                spec = filter
        dataset = (self._copy_only_fields(document, fields) for document in self._iter_documents(spec))
        return Cursor(dataset)
    def _copy_only_fields(self, doc, fields):
        """Copy only the specified fields."""
        if fields is None:
            return doc.copy()
        doc_copy = {}
        if not fields:
            fields = ["_id"]
        for key in fields:
            if key in doc:
                doc_copy[key] = doc[key]
        return doc_copy

    def _iter_documents(self, filter=None):
        return (document for document in itervalues(self._documents) if self._filter_applies(filter, document))
    def find_one(self, filter=None):
        try:
            return next(self.find(filter))
        except StopIteration:
            return None
    def _filter_applies(self, search_filter, document):
        """Returns a boolean indicating whether @search_filter applies
        to @document.
        """
        if search_filter is None:
            return True
        elif isinstance(search_filter, ObjectId):
            search_filter = {'_id': search_filter}

        for key, search in iteritems(search_filter):
            doc_val = resolve_key_value(key, document)

            if isinstance(search, dict):
                is_match = all(
                    OPERATOR_MAP[operator_string] ( doc_val, search_val )
                    for operator_string,search_val in iteritems(search)
                    )
            elif isinstance(search, RE_TYPE) and isinstance(doc_val, string_types):
                is_match = search.match(doc_val) is not None
            else:
                is_match = doc_val == search

            if not is_match:
                return False

        return True

    def remove(self, spec_or_id=None, search_filter=None):
        """Remove objects matching spec_or_id from the collection."""
        if search_filter is not None:
            _print_deprecation_warning('search_filter', 'spec_or_id')
        if spec_or_id is None:
            spec_or_id = search_filter if search_filter else {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        to_delete = list(self.find(spec=spec_or_id))
        for doc in to_delete:
            doc_id = doc['_id']
            del self._documents[doc_id]

    def count(self):
        return len(self._documents)


class Cursor(object):
    def __init__(self, dataset):
        super(Cursor, self).__init__()
        self._dataset = dataset
    def __iter__(self):
        return self
    def __next__(self):
        return next(self._dataset)
    next = __next__
