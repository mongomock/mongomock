import collections
import copy
import itertools
import time
import warnings
import sys
from .filtering import filter_applies

try:
    # Optional requirements for providing Map-Reduce functionality
    import execjs
    from bson import (json_util, SON)
except ImportError:
    execjs = None
from .helpers import ObjectId
try:
    import simplejson as json
except ImportError:
    import json

try:
    from pymongo.errors import DuplicateKeyError
except:
    class DuplicateKeyError(Exception):
        pass

try:
    from pymongo.errors import OperationFailure
except:
    class OperationFailure(Exception):
        pass

from six import (
                 iteritems,
                 itervalues,
                 iterkeys)

from mongomock import helpers
from mongomock.__version__ import __version__


__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']



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
            subdocument = None
            for k, v in iteritems(document):
                if k == '$set':
                    self._update_document_fields(existing_document, v, _set_updater)
                elif k == '$unset':
                    for field, value in iteritems(v):
                        if value and existing_document.has_key(field):
                            del existing_document[field]
                elif k == '$inc':
                    positional = False
                    for key in iterkeys(v):
                        if '$' in key:
                            positional = True
                            break

                    if positional:
                        subdocument = self._update_document_fields_positional(existing_document, v, spec, _inc_updater, subdocument)
                        continue
                    self._update_document_fields(existing_document, v, _inc_updater)
                elif k == '$addToSet':
                    for field, value in iteritems(v):
                        container = existing_document.setdefault(field, [])
                        if value not in container:
                            container.append(value)
                elif k == '$pull':
                    for field, value in iteritems(v):
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            arr = existing_document[field]
                            existing_document[field] = [obj for obj in arr if not obj == value]
                            continue

                        # nested fields includes a positional element
                        # need to find that element
                        if '$' in nested_field_list:
                            if not subdocument:
                                subdocument = self._get_subdocument(existing_document, spec, nested_field_list)

                            # value should be a dictionary since we're pulling
                            pull_results = []
                            # and the last subdoc should be an array
                            for obj in subdocument[nested_field_list[-1]]:
                                if isinstance(obj, dict):
                                    for pull_key, pull_value in iteritems(value):
                                        if obj[pull_key] != pull_value:
                                            pull_results.append(obj)
                                    continue
                                if obj != value:
                                    pull_results.append(obj)

                            # cannot write to doc directly as it doesn't save to existing_document
                            subdocument[nested_field_list[-1]] = pull_results
                elif k == '$push':
                    for field, value in iteritems(v):
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            # document should be a list
                            # append to it
                            if isinstance(value, dict):
                                if '$each' in value:
                                    # append the list to the field
                                    existing_document[field] += list(value['$each'])
                                    continue
                            existing_document[field].append(value)
                            continue

                        # nested fields includes a positional element
                        # need to find that element
                        if '$' in nested_field_list:
                            if not subdocument:
                                subdocument = self._get_subdocument(existing_document, spec, nested_field_list)

                            # we're pushing a list
                            push_results = []
                            if nested_field_list[-1] in subdocument:
                                # if the list exists, then use that list
                                push_results = subdocument[nested_field_list[-1]]

                            if isinstance(value, dict):
                                # check to see if we have the format
                                # { '$each': [] }
                                if '$each' in value:
                                    push_results += list(value['$each'])
                                else:
                                    push_results.append(value)
                            else:
                                push_results.append(value)

                            # cannot write to doc directly as it doesn't save to existing_document
                            subdocument[nested_field_list[-1]] = push_results
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

    def _get_subdocument(self, existing_document, spec, nested_field_list):
        """
        This method retrieves the subdocument of the existing_document.nested_field_list. It uses the spec to filter
        through the items. It will continue to grab nested documents until it can go no further. It will then return the
        subdocument that was last saved. '$' is the positional operator, so we use the $elemMatch in the spec to find
        the right subdocument in the array.
        """
        # current document in view
        doc = existing_document
        # previous document in view
        subdocument = existing_document
        # current spec in view
        subspec = spec
        # walk down the dictionary
        for subfield in nested_field_list:
            if subfield == '$':
                # positional element should have the equivalent elemMatch in the query
                subspec = subspec['$elemMatch']
                for item in doc:
                    # iterate through
                    if filter_applies(subspec, item):
                        # found the matching item
                        # save the parent
                        subdocument = doc
                        # save the item
                        doc = item
                        break
                continue

            subdocument = doc
            doc = doc[subfield]
            if not subfield in subspec:
                break
            subspec = subspec[subfield]

        return subdocument

    def _discard_operators(self, doc):
        # TODO: this looks a little too naive...
        return dict((k, v) for k, v in iteritems(doc) if not k.startswith("$"))

    def find(self, spec = None, fields = None, filter = None, sort = None, timeout = True, limit = 0, snapshot = False):
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

    def _update_document_fields_positional(self, doc, fields, spec, updater, subdocument=None):
        """Implements the $set behavior on an existing document"""
        for k, v in iteritems(fields):
            if '$' in k:
                field_name_parts = k.split('.')
                if not subdocument:
                    current_doc = doc
                    subspec = spec
                    for part in field_name_parts[:-1]:
                        if part == '$':
                            subspec = subspec['$elemMatch']
                            for item in current_doc:
                                if filter_applies(subspec, item):
                                    current_doc = item
                                    break
                            continue

                        subspec = subspec[part]
                        current_doc = current_doc[part]
                    subdocument = current_doc
                updater(subdocument, field_name_parts[-1], v)
                continue
            # otherwise, we handle it the standard way
            self._update_document_single_field(doc, k, v, updater)

        return subdocument

    def _update_document_single_field(self, doc, field_name, field_value, updater):
        field_name_parts = field_name.split(".")
        for part in field_name_parts[:-1]:
            if not isinstance(doc, dict) and not isinstance(doc, list):
                return # mongodb skips such cases
            if isinstance(doc, list):
                try:
                    doc = doc[int(part)]
                    continue
                except ValueError:
                    pass
            doc = doc.setdefault(part, {})
        updater(doc, field_name_parts[-1], field_value)

    def _iter_documents(self, filter = None):
        return (document for document in itervalues(self._documents) if filter_applies(filter, document))

    def find_one(self, spec_or_id=None, *args, **kwargs):
        # Allow calling find_one with a non-dict argument that gets used as
        # the id for the query.
        if not isinstance(spec_or_id, collections.Mapping):
            spec_or_id = {'_id':spec_or_id}

        try:
            return next(self.find(spec_or_id, *args, **kwargs))
        except StopIteration:
            return None

    def find_and_modify(self, query = {}, update = None, upsert = False, **kwargs):
        remove = kwargs.get("remove", False)
        if kwargs.get("new", False) and remove:
            raise OperationFailure("remove and returnNew can't co-exist") # message from mongodb

        if remove and update is not None:
            raise ValueError("Can't do both update and remove")

        old = self.find_one(query)
        if not old:
            if upsert:
                old = {'_id':self.insert(query)}
            else:
                return None

        if remove:
            self.remove({"_id": old["_id"]})
        else:
            self.update({'_id':old['_id']}, update)

        if kwargs.get('new', False):
            return self.find_one({'_id':old['_id']})
        return old

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

    def ensure_index(self, key_or_list, cache_for = 300, **kwargs):
        pass

    def map_reduce(self, map_func, reduce_func, out, full_response=False, query=None, limit=0):
        if execjs is None:
            raise NotImplementedError(
                "PyExecJS is required in order to run Map-Reduce. "
                "Use 'pip install pyexecjs pymongo' to support Map-Reduce mock."
            )
        if limit == 0:
            limit = None
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

    def inline_map_reduce(self, map_func, reduce_func, full_response=False, query=None, limit=0):
        return self.map_reduce(map_func, reduce_func, {'inline':1}, full_response, query, limit)


class Cursor(object):
    def __init__(self, dataset, limit=0):
        super(Cursor, self).__init__()
        self._dataset = dataset
        self._limit = limit if limit != 0 else None #pymongo limit defaults to 0, returning everything
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
    def __getitem__(self, index):
        arr = [x for x in self._dataset]
        count = len(arr)
        self._dataset = iter(arr)
        return arr[index]

def _set_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = value

def _inc_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = doc.get(field_name, 0) + value
