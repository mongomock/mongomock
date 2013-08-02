import copy
import itertools
import operator
import re
import time
import warnings
import sys

try:
    # Optional requirements for providing Map-Reduce functionality
    import execjs
    from bson import (json_util, SON)
except ImportError:
    execjs = None
try:
    from bson import (ObjectId, RE_TYPE)
except ImportError:
    from mongomock.object_id import ObjectId
    RE_TYPE = type(re.compile(''))
try:
    import simplejson as json
except ImportError:
    import json

try:
    from pymongo.errors import DuplicateKeyError
except:
    class DuplicateKeyError(Exception):
        pass

from sentinels import NOTHING
from six import (
                 iteritems,
                 itervalues,
                 string_types,
                 )

from mongomock import helpers
from mongomock.__version__ import __version__


__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']


def _force_list(v):
    return v if isinstance(v, (list, tuple)) else [v]

def _not_nothing_and(f):
    "wrap an operator to return False if the first arg is NOTHING"
    return lambda v, l: v is not NOTHING and f(v, l)

def _all_op(doc_val, search_val):
    dv = _force_list(doc_val)
    return all(x in dv for x in search_val)

def _not_op(c, d, k, s):
    return not c._filter_applies({k: s}, d)

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

LOGICAL_OPERATOR_MAP = {'$or':lambda c, d, subq: any(c._filter_applies(q, d) for q in subq),
                        '$and':lambda c, d, subq: all(c._filter_applies(q, d) for q in subq),
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
        self.host = host
        self.port = port
        self._databases = {}
    def __getitem__(self, db_name):
        db = self._databases.get(db_name, None)
        if db is None:
            db = self._databases[db_name] = Database(self, db_name)
        return db
    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        identifier = []
        host = getattr(self,'host','')
        port = getattr(self,'port',None)
        if host is not None:
            identifier = ["'{0}'".format(host)]
            if port is not None:
                identifier.append(str(port))
        return "mongomock.Connection({0})".format(', '.join(identifier))

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

#Connection is now depricated, it's called MongoClient instead
class MongoClient(Connection):
    def stub(self):
        pass

class Database(object):
    def __init__(self, conn, name):
        super(Database, self).__init__()
        self.name = name
        self._Database__connection = conn
        self._collections = {'system.indexes' : Collection(self, 'system.indexes')}

    def __getitem__(self, coll_name):
        coll = self._collections.get(coll_name, None)
        if coll is None:
            coll = self._collections[coll_name] = Collection(self, coll_name)
        return coll

    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        return "Database({0}, '{1}')".format(self._Database__connection, self.name)

    @property
    def connection(self):
        return self._Database__connection

    def collection_names(self):
        return list(self._collections.keys())
    def drop_collection(self, name_or_collection):
        try:
            # FIXME a better way to remove an entry by value ?
            if isinstance(name_or_collection, Collection):
                for collection in self._collections.items():
                    if collection[1] is name_or_collection:
                        del self._collections[collection[0]]
            else:
                del self._collections[name_or_collection]
        except:  # EAFP paradigm (http://en.m.wikipedia.org/wiki/Python_syntax_and_semantics)
            pass

class Collection(object):
    def __init__(self, db, name):
        super(Collection, self).__init__()
        self.name = name
        self._Collection__database = db
        self._documents = {}

    def __repr__(self):
        return "Collection({0}, '{1}')".format(self._Collection__database, self.name)

    def insert(self, data):
        if isinstance(data, list):
            return [self._insert(element) for element in data]
        return self._insert(data)
    def _insert(self, data):
        if not '_id' in data:
            data['_id'] = ObjectId()
        object_id = data['_id']
        if object_id in self._documents:
            raise DuplicateKeyError("Duplicate Key Error", 11000)
        self._documents[object_id] = copy.deepcopy(data)
        return object_id
    def update(self, spec, document, upsert = False, manipulate = False,
               safe = False, multi = False, _check_keys = False, **kwargs):
        """Updates document(s) in the collection."""
        found = False
        for existing_document in itertools.chain(self._iter_documents(spec), [None]):
            # the sentinel document means we should do an upsert
            if existing_document is None:
                if not upsert:
                    continue
                existing_document = self._documents[self._insert(self._discard_operators(spec))]
            first = True
            found = True
            for k, v in iteritems(document):
                if k == '$set':
                    self._update_document_fields(existing_document, v, _set_updater)
                elif k == '$unset':
                    for field, value in iteritems(v):
                        if value and existing_document.has_key(field):
                            del existing_document[field]
                elif k == '$inc':
                    self._update_document_fields(existing_document, v, _inc_updater)
                elif k == '$addToSet':
                    for field, value in iteritems(v):
                        container = existing_document.setdefault(field, [])
                        if value not in container:
                            container.append(value)
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

    def _discard_operators(self, doc):
        # TODO: this looks a little too naive...
        return dict((k, v) for k, v in iteritems(doc) if not k.startswith("$"))

    def find(self, spec = None, fields = None, filter = None, sort = None, timeout = True, limit = None):
        if filter is not None:
            _print_deprecation_warning('filter', 'spec')
            if spec is None:
                spec = filter
        dataset = (self._copy_only_fields(document, fields) for document in self._iter_documents(spec))
        if sort:
            for sortKey, sortDirection in reversed(sort):
                dataset = iter(sorted(dataset, key = lambda x: x[sortKey], reverse = sortDirection < 0))
        return Cursor(dataset, limit=limit)

    def _copy_only_fields(self, doc, fields):
        """Copy only the specified fields."""

        if fields is None:
            return copy.deepcopy(doc)
        else:
            if not fields:
                fields = {"_id": 1}
            if not isinstance(fields, dict):
                fields = helpers._fields_list_to_dict(fields)

            #we can pass in something like {"_id":0, "field":1}, so pull the id value out and hang on to it until later
            id_value = fields.pop('_id', 1)

            #other than the _id field, all fields must be either includes or excludes, this can evaluate to 0
            if len(set(list(fields.values()))) > 1:
                raise ValueError('You cannot currently mix including and excluding fields.')

            #if we have novalues passed in, make a doc_copy based on the id_value
            if len(list(fields.values())) == 0:
                if id_value == 1:
                    doc_copy = {}
                else:
                    doc_copy = copy.deepcopy(doc)
            #if 1 was passed in as the field values, include those fields
            elif  list(fields.values())[0] == 1:
                doc_copy = {}
                for key in fields:
                    if key in doc:
                        doc_copy[key] = doc[key]
            #otherwise, exclude the fields passed in
            else:
                doc_copy = copy.deepcopy(doc)
                for key in fields:
                    if key in doc_copy:
                        del doc_copy[key]

            #set the _id value if we requested it, otherwise remove it
            if id_value == 0:
                if '_id' in doc_copy:
                    del doc_copy['_id']
            else:
                if '_id' in doc:
                    doc_copy['_id'] = doc['_id']

            fields['_id'] = id_value #put _id back in fields
            return doc_copy


    def _update_document_fields(self, doc, fields, updater):
        """Implements the $set behavior on an existing document"""
        for k, v in iteritems(fields):
            self._update_document_single_field(doc, k, v, updater)

    def _update_document_single_field(self, doc, field_name, field_value, updater):
        field_name_parts = field_name.split(".")
        for part in field_name_parts[:-1]:
            if not isinstance(doc, dict):
                return # mongodb skips such cases
            doc = doc.setdefault(part, {})
        updater(doc, field_name_parts[-1], field_value)

    def _iter_documents(self, filter = None):
        return (document for document in itervalues(self._documents) if self._filter_applies(filter, document))

    def find_one(self, spec_or_id=None, *args, **kwargs):
        try:
            return next(self.find(spec_or_id, *args, **kwargs))
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
                               operator_string in OPERATOR_MAP and OPERATOR_MAP[operator_string] (doc_val, search_val) or
                               operator_string == '$not' and _not_op(self, document, key, search_val)
                               for operator_string, search_val in iteritems(search)
                               )
            elif isinstance(search, RE_TYPE) and isinstance(doc_val, string_types):
                is_match = search.match(doc_val) is not None
            elif key in LOGICAL_OPERATOR_MAP:
                is_match = LOGICAL_OPERATOR_MAP[key] (self, document, search)
            elif isinstance(doc_val, (list, tuple)):
                if isinstance(search, ObjectId):
                    is_match = str(search) in doc_val
                else:
                    is_match = search in doc_val
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

    def drop(self):
        del self._documents
        self._documents = {}

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        pass

    def map_reduce(self, map_func, reduce_func, out, full_response=False, query=None, limit=None):
        if execjs is None:
            raise NotImplementedError(
                "PyExecJS is required in order to run Map-Reduce. "
                "Use 'pip install pyexecjs pymongo' to support Map-Reduce mock."
            )
        start_time = time.clock()
        out_collection = None
        reduced_rows = None
        full_dict = {'counts': {'input': 0,
                                'reduce':0,
                                'emit':0,
                                'output':0},
                     'timeMillis': 0,
                     'ok': 1.0,
                     'result': None}
        map_ctx = execjs.compile("""
            function doMap(fnc, docList) {
                var mappedDict = {};
                function emit(key, val) {
                    if(!mappedDict[key]) {
                        mappedDict[key] = [];
                    }
                    mappedDict[key].push(val);
                }
                mapper = eval('('+fnc+')');
                var mappedList = new Array();
                for(var i=0; i<docList.length; i++) {
                    var thisDoc = eval('('+docList[i]+')');
                    var mappedVal = (mapper).call(thisDoc);
                }
                return mappedDict;
            }
        """)
        reduce_ctx = execjs.compile("""
            function doReduce(fnc, docList) {
                var reducedList = new Array();
                reducer = eval('('+fnc+')');
                for(var key in docList) {
                    var reducedVal = {'_id': key,
                            'value': reducer(key, docList[key])};
                    reducedList.push(reducedVal);
                }
                return reducedList;
            }
        """)
        doc_list = [json.dumps(doc, default=json_util.default) for doc in self.find(query)]
        mapped_rows = map_ctx.call('doMap', map_func, doc_list)
        reduced_rows = reduce_ctx.call('doReduce', reduce_func, mapped_rows)[:limit]
        reduced_rows = sorted(reduced_rows, key=lambda x: x['_id'])
        if full_response:
            full_dict['counts']['input'] = len(doc_list)
            for key in mapped_rows.keys():
                emit_count = len(mapped_rows[key])
                full_dict['counts']['emit'] += emit_count
                if emit_count > 1:
                    full_dict['counts']['reduce'] += 1
            full_dict['counts']['output'] = len(reduced_rows)
        if isinstance(out, (str, bytes)):
            out_collection = getattr(self._Collection__database, out)
            out_collection.insert(reduced_rows)
            ret_val = out_collection
            full_dict['result'] = out
        elif isinstance(out, SON) and out.get('replace') and out.get('db'):
            # Must be of the format SON([('replace','results'),('db','outdb')])
            out_db = getattr(self._Collection__database._Database__connection, out['db'])
            out_collection = getattr(out_db, out['replace'])
            out_collection.insert(reduced_rows)
            ret_val = out_collection
            full_dict['result'] = {'db': out['db'], 'collection': out['replace']}
        elif isinstance(out, dict) and out.get('inline'):
            ret_val = reduced_rows
            full_dict['result'] = reduced_rows
        else:
            raise TypeError("'out' must be an instance of string, dict or bson.SON")
        full_dict['timeMillis'] = int(round((time.clock() - start_time) * 1000))
        if full_response:
            ret_val = full_dict
        return ret_val

    def inline_map_reduce(self, map_func, reduce_func, full_response=False, query=None, limit=None):
        return self.map_reduce(map_func, reduce_func, {'inline':1}, full_response, query, limit)


class Cursor(object):
    def __init__(self, dataset, limit = None):
        super(Cursor, self).__init__()
        self._dataset = dataset
        self._limit = limit
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
    def sort(self, key_or_list, direction = None):
        if direction is None:
            direction = 1
        if isinstance(key_or_list, (tuple, list)):
            for sortKey, sortDirection in reversed(key_or_list):
                self._dataset = iter(sorted(self._dataset, key = lambda x: x[sortKey], reverse = sortDirection < 0))
        else:
            self._dataset = iter(sorted(self._dataset, key = lambda x:x[key_or_list], reverse = direction < 0))
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
    def batch_size(self, count):
        return self
    def close(self):
        pass

def _set_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = value

def _inc_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = doc.get(field_name, 0) + value
