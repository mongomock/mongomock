import collections
import copy
import functools
import itertools
import json
import time
import warnings
from sentinels import NOTHING
from .filtering import filter_applies, resolve_key_value
from . import ObjectId, OperationFailure, DuplicateKeyError
from .helpers import basestring, xrange

try:
    # Optional requirements for providing Map-Reduce functionality
    import execjs
except ImportError:
    execjs = None

try:
    from bson import (json_util, SON)
except ImportError:
    json_utils = SON = None

from six import (
    string_types,
    text_type,
    iteritems,
    itervalues,
    iterkeys)
from mongomock import helpers


class Collection(object):
    def __init__(self, db, name):
        super(Collection, self).__init__()
        self.name = name
        self.full_name = "{0}.{1}".format(db.name, name)
        self._Collection__database = db
        self._documents = {}

    def __repr__(self):
        return "Collection({0}, '{1}')".format(self._Collection__database, self.name)

    def __getitem__(self, name):
        return self._Collection__database[self.name + '.' + name]

    def __getattr__(self, name):
        return self.__getitem__(name)

    def insert(self, data):
        if isinstance(data, list):
            return [self._insert(element) for element in data]
        return self._insert(data)

    def _insert(self, data):

        if not all(isinstance(k, string_types) for k in data):
            raise ValueError("Document keys must be strings")

        if '_id' not in data:
            data['_id'] = ObjectId()
        object_id = data['_id']
        if object_id in self._documents:
            raise DuplicateKeyError("Duplicate Key Error", 11000)
        self._documents[object_id] = self._internalize_dict(data)
        return object_id

    def _internalize_dict(self, d):
        return dict((k, copy.deepcopy(v)) for k, v in iteritems(d))

    def _has_key(self, doc, key):
        return key in doc

    def update(self, spec, document, upsert = False, manipulate = False,
               safe = False, multi = False, _check_keys = False, **kwargs):
        """Updates document(s) in the collection."""
        found = False
        updated_existing = False
        num_updated = 0
        for existing_document in itertools.chain(self._iter_documents(spec), [None]):
            # the sentinel document means we should do an upsert
            if existing_document is None:
                if not upsert:
                    continue
                existing_document = self._documents[self._insert(self._discard_operators(spec))]
            else:
                updated_existing = True
            num_updated += 1
            first = True
            found = True
            subdocument = None
            for k, v in iteritems(document):
                if k == '$set':
                    self._update_document_fields(existing_document, v, _set_updater)
                elif k == '$unset':
                    for field, value in iteritems(v):
                        if self._has_key(existing_document, field):
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
                            if field not in existing_document:
                                existing_document[field] = []
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
                        existing_document.update(self._internalize_dict(document))
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
                break

        return {
            text_type("connectionId"): self._Collection__database.connection._id,
            text_type("err"): None,
            text_type("ok"): 1.0,
            text_type("n"): num_updated,
            text_type("updatedExisting"): updated_existing,
        }

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

    def find(self, spec = None, fields = None, filter = None, sort = None, timeout = True, limit = 0, snapshot = False, as_class = None, skip = 0):
        if filter is not None:
            _print_deprecation_warning('filter', 'spec')
            if spec is None:
                spec = filter
        if as_class is None:
            as_class = dict
        return Cursor(self, functools.partial(self._get_dataset, spec, sort, fields, as_class, skip), limit=limit)

    def _get_dataset(self, spec, sort, fields, as_class, skip):
        dataset = (self._copy_only_fields(document, fields, as_class) for document in self._iter_documents(spec))
        if sort:
            for sortKey, sortDirection in reversed(sort):
                dataset = iter(sorted(dataset, key = lambda x: resolve_key_value(sortKey, x), reverse = sortDirection < 0))

        for i in xrange(skip):
            try:
                unused = next(dataset)
            except StopIteration:
                pass

        return dataset

    def _copy_field(self, obj, container):
        if isinstance(obj, list):
            new = []
            for item in obj:
                new.append(self._copy_field(item, container))
            return new
        if isinstance(obj, dict):
            new = container()
            for key, value in obj.items():
                new[key] = self._copy_field(value, container)
            return new
        else:
            return copy.copy(obj)

    def _copy_only_fields(self, doc, fields, container):
        """Copy only the specified fields."""

        if fields is None:
            return self._copy_field(doc, container)
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
                    doc_copy = container()
                else:
                    doc_copy = self._copy_field(doc, container)
            #if 1 was passed in as the field values, include those fields
            elif  list(fields.values())[0] == 1:
                doc_copy = container()
                for key in fields:
                    if key in doc:
                        doc_copy[key] = doc[key]
            #otherwise, exclude the fields passed in
            else:
                doc_copy = self._copy_field(doc, container)
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
        if spec_or_id is None:
            spec_or_id = {}
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

        return {
            "connectionId": self._Collection__database.connection._id,
            "n": len(to_delete),
            "ok": 1.0,
            "err": None,
        }

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

    def distinct(self, key):
        return self.find().distinct(key)


class Cursor(object):
    def __init__(self, collection, dataset_factory, limit=0):
        super(Cursor, self).__init__()
        self.collection = collection
        self._factory = dataset_factory
        self._dataset = self._factory()
        self._limit = limit if limit != 0 else None #pymongo limit defaults to 0, returning everything
        self._skip = None

    def __iter__(self):
        return self

    def clone(self):
        return Cursor(self.collection, self._factory, self._limit)

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
                self._dataset = iter(sorted(self._dataset, key = lambda x:resolve_key_value(sortKey, x), reverse = sortDirection < 0))
        else:
            self._dataset = iter(sorted(self._dataset, key = lambda x:resolve_key_value(key_or_list, x), reverse = direction < 0))
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

    def distinct(self, key):
        if not isinstance(key, basestring):
            raise TypeError('cursor.distinct key must be a string')
        unique = set()
        for x in iter(self._dataset):
            value = resolve_key_value(key, x)
            if value == NOTHING: continue
            unique.add(value)
        return list(unique)

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
