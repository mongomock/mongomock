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
try:
    from bson import ObjectId
except ImportError:
    from .object_id import ObjectId

__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']


RE_TYPE = type(re.compile(''))

def _force_list(v):
    return v if isinstance(v, (list, tuple)) else [v]

def _not_nothing_and(f):
    "wrap an operator to return False if the first arg is NOTHING"
    return lambda v, l: v is not NOTHING and f(v, l)

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
                '$in':lambda dv, sv: any(x in sv for x in _force_list(dv)),
                '$nin':lambda dv, sv: all(x not in sv for x in _force_list(dv)),
                '$exists':lambda dv, sv: bool(sv) == (dv is not NOTHING),
                '$regex':lambda dv, sv: re.compile(sv).match(dv),
                '$where':lambda db, sv: True  # ignore this complex filter
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
    def __init__(self, host = None, port = None, max_pool_size = 10,
                 network_timeout = None, document_class = dict,
                 tz_aware = False, _connect = True, **kwargs):
        super(Connection, self).__init__()
        self._databases = {}
    def __getitem__(self, db_name):
        db = self._databases.get(db_name, None)
        if db is None:
            db = self._databases[db_name] = Database(self)
        return db
    def __getattr__(self, attr):
        return self[attr]
    def server_info(self):
        return {
            "version" : "2.0.6",
            "sysInfo" : "Mock",
            "versionArray" : [
                              2,
                              0,
                              6,
                              0
                              ],
            "bits" : 64,
            "debug" : False,
            "maxBsonObjectSize" : 16777216,
            "ok" : 1
    }

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
        if not '_id' in data:
            data['_id'] = ObjectId()
        object_id = data['_id']
        assert object_id not in self._documents
        self._documents[object_id] = dict(data)
        return object_id
    def update(self, spec, document, upsert = False, manipulate = False,
               safe = False, multi = False, _check_keys = False, **kwargs):
        """Updates docuemnt(s) in the collection."""
        found = False
        for existing_document in self._iter_documents(spec):
            first = True
            found = True
            for k, v in iteritems(document):
                if k == '$set':
                    existing_document.update(v)
                elif k == '$unset':
                    for field, value in v.iteritems():
                        if value and existing_document.has_key(field):
                            del existing_document[field]
                elif k == '$inc':
                    for field, value in iteritems(v):
                        new_value = existing_document.get(field, 0)
                        new_value = new_value + value
                        existing_document[field] = new_value
                elif k == '$addToSet':
                    for field, value in iteritems(v):
                        new_value = set(existing_document.get(field, []))
                        new_value.add(value)
                        existing_document[field] = list(new_value)
                elif k == '$pull':
                    for field, value in iteritems(v):
                        arr = existing_document[field]
                        existing_document[field] = [obj for obj in arr if not obj == value]
                else:
                    if first:
                        # replace entire document
                        for key in document.keys():
                            if key.startswith('$'):
                                # can't mix modifiers with non-modifiers in update
                                raise ValueError('field names cannot start with $ [{}]'.format(k))
                        _id = spec.get('_id', existing_document.get('_id', None))
                        existing_document.clear()
                        if _id:
                            existing_document['_id'] = _id
                        existing_document.update(document)
                        if existing_document['_id'] != _id:
                            # id changed, fix index
                            del self._documents[_id]
                            self.insert(existing_document)
                        break
                    else:
                        # can't mix modifiers with non-modifiers in update
                        raise ValueError('Invalid modifier specified: {}'.format(k))
                first = False
            if not multi:
                return

        if not found and upsert:
            self.insert(document)

    def find(self, spec = None, fields = None, filter = None):
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

    def _iter_documents(self, filter = None):
        return (document for document in itervalues(self._documents) if self._filter_applies(filter, document))
    def find_one(self, filter = None):
        try:
            return next(self.find(filter))
        except StopIteration:
            return None

    def find_and_modify(self, query = {}, update = None, upsert = False, **kwargs):
        old = self.find_one(query)
        if not old:
            if upsert:
                old = {'_id':self.insert(query)}
            else:
                return None
        self.update({'_id':old['_id']}, update)
        if kwargs.get('new', False):
            return self.find_one({'_id':old['_id']})
        return old

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
                               operator_string in OPERATOR_MAP and OPERATOR_MAP[operator_string] (doc_val, search_val)
                               for operator_string, search_val in iteritems(search)
                               )
            elif isinstance(search, RE_TYPE) and isinstance(doc_val, string_types):
                is_match = search.match(doc_val) is not None
            elif key in OPERATOR_MAP:
                OPERATOR_MAP[key] (doc_val, search)
            else:
                is_match = doc_val == search

            if not is_match:
                return False

        return True
    def save(self, to_save, manipulate = True, safe = False, **kwargs):
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save)
        else:
            self.update({"_id": to_save["_id"]}, to_save, True,
                        manipulate, safe, _check_keys = True, **kwargs)
            return to_save.get("_id", None)
    def remove(self, spec_or_id = None, search_filter = None):
        """Remove objects matching spec_or_id from the collection."""
        if search_filter is not None:
            _print_deprecation_warning('search_filter', 'spec_or_id')
        if spec_or_id is None:
            spec_or_id = search_filter if search_filter else {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        to_delete = list(self.find(spec = spec_or_id))
        for doc in to_delete:
            doc_id = doc['_id']
            del self._documents[doc_id]

    def count(self):
        return len(self._documents)


class Cursor(object):
    def __init__(self, dataset):
        super(Cursor, self).__init__()
        self._dataset = dataset
        self._limit = None
        self._skip = None
    def __iter__(self):
        return self
    def __next__(self):
        if self._skip:
            for i in range(self._skip):
                next(self._dataset)
            self._skip = None
        if self._limit is not None and self._limit <= 0:
            raise StopIteration()
        if self._limit is not None:
            self._limit -= 1
        return next(self._dataset)
    next = __next__
    def sort(self, key, order):
        arr = [x for x in self._dataset]
        arr = sorted(arr, key = lambda x:x[key], reverse = order < 0)
        self._dataset = iter(arr)
        return self
    def count(self):
        arr = [x for x in self._dataset]
        count = len(arr)
        self._dataset = iter(arr)
        return count
    def skip(self, count):
        self._skip = count
        return self
    def limit(self, count):
        self._limit = count
        return self
