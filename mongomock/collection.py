import collections
import copy
import functools
import itertools
import json
import time
import warnings
from sentinels import NOTHING
from .filtering import filter_applies, iter_key_candidates
from . import ObjectId, OperationFailure, DuplicateKeyError
from .helpers import basestring, xrange, print_deprecation_warning, hashdict

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

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
        self._documents = OrderedDict()
        self._uniques = []

    def __repr__(self):
        return "Collection({0}, '{1}')".format(self._Collection__database, self.name)

    def __getitem__(self, name):
        return self._Collection__database[self.name + '.' + name]

    def __getattr__(self, name):
        return self.__getitem__(name)

    def insert(self, data, manipulate=True,
               safe=None, check_keys=True, continue_on_error=False, **kwargs):
        if isinstance(data, list):
            return [self._insert(element) for element in data]
        return self._insert(data)

    def _insert(self, data):

        if not all(isinstance(k, string_types) for k in data):
            raise ValueError("Document keys must be strings")

        if '_id' not in data:
            data['_id'] = ObjectId()
        object_id = data['_id']
        if type(object_id) == dict:
            object_id = hashdict(object_id)
        if object_id in self._documents:
            raise DuplicateKeyError("Duplicate Key Error", 11000)
        for unique in self._uniques:
            find_kwargs = {}
            for key, direction in unique:
                if key in data:
                    find_kwargs[key] = data[key]
            answer = self.find(spec=find_kwargs)
            if answer.count() > 0:
                raise DuplicateKeyError("Duplicate Key Error", 11000)

        self._documents[object_id] = self._internalize_dict(data)
        return data['_id']

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
            # we need was_insert for the setOnInsert update operation
            was_insert = False
            # the sentinel document means we should do an upsert
            if existing_document is None:
                if not upsert:
                    continue
                existing_document = self._documents[self._insert(self._discard_operators(spec))]
                was_insert = True
            else:
                updated_existing = True
            num_updated += 1
            first = True
            found = True
            subdocument = None
            for k, v in iteritems(document):
                if k == '$set':
                    positional = False
                    for key in iterkeys(v):
                        if '$' in key:
                            positional = True
                            break
                    if positional:
                        subdocument = self._update_document_fields_positional(existing_document,v, spec, _set_updater, subdocument)
                        continue

                    self._update_document_fields(existing_document, v, _set_updater)
                elif k == '$setOnInsert':
                    if not was_insert:
                        continue
                    positional = any('$' in key for key in iterkeys(v))
                    if positional:
                        # we use _set_updater
                        subdocument = self._update_document_fields_positional(existing_document,v, spec, _set_updater, subdocument)
                    else:
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
                            if field in existing_document:
                                arr = existing_document[field]
                                if isinstance(value, dict):
                                    existing_document[field] = [obj for obj in arr if not filter_applies(value, obj)]
                                else:
                                    existing_document[field] = [obj for obj in arr if not value == obj]
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
                        elif '$' in nested_field_list:
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
                        # push to array in a nested attribute
                        else:
                            # create nested attributes if they do not exist
                            subdocument = existing_document
                            for field in nested_field_list[:-1]:
                                if field not in subdocument:
                                    subdocument[field] = {}

                                subdocument = subdocument[field]

                            # we're pushing a list
                            push_results = []
                            if nested_field_list[-1] in subdocument:
                                # if the list exists, then use that list
                                push_results = subdocument[nested_field_list[-1]]

                            push_results.append(value)

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
                            raise OperationFailure(
                                "The _id field cannot be changed from {0} to {1}".format(
                                    existing_document['_id'], _id))
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

    def find(self, spec = None, fields = None, filter = None, sort = None, timeout = True, limit = 0, snapshot = False, as_class = None, skip = 0, slave_okay=False):
        if filter is not None:
            print_deprecation_warning('filter', 'spec')
            if spec is None:
                spec = filter
        if as_class is None:
            as_class = dict
        return Cursor(self, functools.partial(self._get_dataset, spec, sort, fields, as_class, skip), limit=limit)

    def _get_dataset(self, spec, sort, fields, as_class, skip):
        dataset = (self._copy_only_fields(document, fields, as_class) for document in self._iter_documents(spec))
        if sort:
            for sortKey, sortDirection in reversed(sort):
                dataset = iter(sorted(dataset, key = lambda x: _resolve_key(sortKey, x), reverse = sortDirection < 0))
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
                            subspec = subspec.get('$elemMatch', subspec)
                            for item in current_doc:
                                if filter_applies(subspec, item):
                                    current_doc = item
                                    break
                            continue

                        new_spec = {}
                        for el in subspec:
                            if el.startswith(part):
                                if len(el.split(".")) > 1:
                                    new_spec[".".join(el.split(".")[1:])] = subspec[el]
                                else:
                                    new_spec = subspec[el]
                        subspec = new_spec
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
            if isinstance(doc, list):
                try:
                    if part == '$':
                        doc = doc[0]
                    else:
                        doc = doc[int(part)]
                    continue
                except ValueError:
                    pass
            elif isinstance(doc, dict):
                doc = doc.setdefault(part, {})
            else:
                return
        field_name = field_name_parts[-1]
        if isinstance(doc, list):
            try:
                doc[int(field_name)] = field_value
            except:
                pass
        else:
            updater(doc, field_name, field_value)

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

    def find_and_modify(self, query = {}, update = None, upsert = False, sort = None, **kwargs):
        remove = kwargs.get("remove", False)
        if kwargs.get("new", False) and remove:
            raise OperationFailure("remove and returnNew can't co-exist") # message from mongodb

        if remove and update is not None:
            raise ValueError("Can't do both update and remove")

        old = self.find_one(query, sort=sort)
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
            print_deprecation_warning('search_filter', 'spec_or_id')
        if spec_or_id is None:
            spec_or_id = search_filter if search_filter else {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        to_delete = list(self.find(spec = spec_or_id))
        for doc in to_delete:
            doc_id = doc['_id']
            if type(doc_id) == dict:
                doc_id = hashdict(doc_id)
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
        if 'unique' in kwargs and kwargs['unique']:
            self._uniques.append(helpers._index_list(key_or_list))

    def drop_index(self, index_or_name):
        pass

    def index_information(self):
        return {}

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
                    if (key['$oid']) {
                        mapped_key = '$oid' + key['$oid'];
                    }
                    else {
                        mapped_key = key;
                    }
                    if(!mappedDict[mapped_key]) {
                        mappedDict[mapped_key] = [];
                    }
                    mappedDict[mapped_key].push(val);
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
        for reduced_row in reduced_rows:
            if reduced_row['_id'].startswith('$oid'):
                reduced_row['_id'] = ObjectId(reduced_row['_id'][4:])
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
            out_collection.drop()
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

    def group(self, key, condition, initial, reduce, finalize=None):
        if execjs is None:
            raise NotImplementedError(
                "PyExecJS is required in order to use group. "
                "Use 'pip install pyexecjs pymongo' to support group mock."
            )
        reduce_ctx = execjs.compile("""
            function doReduce(fnc, docList) {
                reducer = eval('('+fnc+')');
                for(var i=0, l=docList.length; i<l; i++) {
                    try {
                        reducedVal = reducer(docList[i-1], docList[i]);
                    }
                    catch (err) {
                        continue;
                    }
                }
            return docList[docList.length - 1];
            }
        """)

        ret_array = []
        doc_list_copy = []
        ret_array_copy = []
        reduced_val = {}
        doc_list = [doc for doc in self.find(condition)]
        for doc in doc_list:
            doc_copy = copy.deepcopy(doc)
            for k in doc:
                if isinstance(doc[k], ObjectId):
                    doc_copy[k] = str(doc[k])
                if k not in key and k not in reduce:
                    del doc_copy[k]
            for initial_key in initial:
                if initial_key in doc.keys():
                    pass
                else:
                    doc_copy[initial_key] = initial[initial_key]
            doc_list_copy.append(doc_copy)
        doc_list = doc_list_copy
        for k in key:
            doc_list = sorted(doc_list, key=lambda x: _resolve_key(k, x))
        for k in key:
            if not isinstance(k, basestring):
                raise TypeError("Keys must be a list of key names, "
                                "each an instance of %s" % (basestring.__name__,))
            for k2, group in itertools.groupby(doc_list, lambda item: item[k]):
                group_list = ([x for x in group])
                reduced_val = reduce_ctx.call('doReduce', reduce, group_list)
                ret_array.append(reduced_val)
        for doc in ret_array:
            doc_copy = copy.deepcopy(doc)
            for k in doc:
                if k not in key and k not in initial.keys():
                    del doc_copy[k]
            ret_array_copy.append(doc_copy)
        ret_array = ret_array_copy
        return ret_array

    def aggregate(self, pipeline, **kwargs):
        pipeline_operators =       ['$project','$match','$redact','$limit','$skip','$unwind','$group','$sort','$geoNear','$out']
        group_operators =          ['$addToSet', '$first','$last','$max','$min','$avg','$push','$sum']
        boolean_operators =        ['$and','$or', '$not']
        set_operators =            ['$setEquals', '$setIntersection', '$setDifference', '$setUnion', '$setIsSubset', '$anyElementTrue', '$allElementsTrue']
        compairison_operators =    ['$cmp','$eq','$gt','$gte','$lt','$lte','$ne']
        aritmetic_operators =      ['$add','$divide','$mod','$multiply','$subtract']
        string_operators =         ['$concat','$strcasecmp','$substr','$toLower','$toUpper']
        text_search_operators =    ['$meta']
        array_operators =          ['$size']
        projection_operators =     ['$map', '$let', '$literal']
        date_operators =           ['$dayOfYear','$dayOfMonth','$dayOfWeek','$year','$month','$week','$hour','$minute','$second','$millisecond']
        conditional_operators =    ['$cond', '$ifNull']

        out_collection = [doc for doc in self.find()]
        grouped_collection = []
        for expression in pipeline:
            for k, v in iteritems(expression):
                if k == '$match':
                    out_collection = [doc for doc in out_collection if filter_applies(v, doc)]
                elif k == '$group':
                    group_func_keys = expression['$group']['_id'][1:]
                    for group_key in reversed(group_func_keys):
                        out_collection = sorted(out_collection, key=lambda x: _resolve_key(group_key, x))
                    for field, value in iteritems(v):
                        if field != '_id':
                            for func, key in iteritems(value):
                                if func == "$sum" or "$avg":
                                    for group_key in group_func_keys:
                                        for ret_value, group in itertools.groupby(out_collection, lambda item: item[group_key]):
                                            doc_dict = {}
                                            group_list = ([x for x in group])
                                            doc_dict['_id'] = ret_value
                                            current_val = 0
                                            if func == "$sum":
                                                for doc in group_list:
                                                    current_val = sum([current_val, doc[field]])
                                                doc_dict[field] = current_val
                                            else:
                                                for doc in group_list:
                                                    current_val = sum([current_val, doc[field]])
                                                    avg = current_val / len(group_list)
                                                doc_dict[field] = current_val
                                            grouped_collection.append(doc_dict)
                                else:
                                    if func in group_operators:
                                        raise NotImplementedError(
                                            "Although %s is a valid group operator for the aggregation pipeline, "
                                            "%s is currently not implemented in Mongomock."
                                        )
                                    else:
                                        raise NotImplementedError(
                                            "%s is not a valid group operator for the aggregation pipeline. "
                                            "See http://docs.mongodb.org/manual/meta/aggregation-quick-reference/ "
                                            "for a complete list of valid operators."
                                        )
                    out_collection = grouped_collection
                elif k == '$sort':
                    sort_array = []
                    for x, y in v.items():
                        sort_array.append({x:y})
                    for sort_pair in reversed(sort_array):
                        for sortKey, sortDirection in sort_pair.items():
                            out_collection = sorted(out_collection, key = lambda x: _resolve_key(sortKey, x), reverse = sortDirection < 0)
                elif k == '$skip':
                    out_collection = out_collection[v:]
                elif k == '$limit':
                    out_collection = out_collection[:v]
                elif k == '$unwind':
                    if not isinstance(v, basestring) and v[0] != '$':
                        raise ValueError("$unwind failed: exception: field path references must be prefixed with a '$' ('%s'"%str(v))
                    if len(v.split('.')) > 1:
                        raise NotImplementedError('Mongmock does not currently support nested field paths in the $unwind implementation. ("%s"'%v)
                    unwound_collection = []
                    for doc in out_collection:
                        array_value = doc.get(v[1:])
                        if array_value in (None, []):
                            continue
                        elif not isinstance(array_value, list):
                            raise TypeError('$unwind must specify an array field, field: "%s", value found: %s'%(str(v),str(array_value)))
                        for field_item in array_value:
                            unwound_collection.append(copy.deepcopy(doc))
                            unwound_collection[-1][v[1:]] = field_item
                    out_collection = unwound_collection
                else:
                    if k in pipeline_operators:
                        raise NotImplementedError(
                            "Although %s is a valid operator for the aggregation pipeline, "
                            "%s is currently not implemented in Mongomock."
                        )
                    else:
                        raise NotImplementedError(
                            "%s is not a valid operator for the aggregation pipeline. "
                            "See http://docs.mongodb.org/manual/meta/aggregation-quick-reference/ "
                            "for a complete list of valid operators."
                        )
        return {'ok':1.0, 'result':out_collection}

def _resolve_key(key, doc):
    return next(iter(iter_key_candidates(key, doc)), NOTHING)

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
                self._dataset = iter(sorted(self._dataset, key = lambda x: _resolve_key(sortKey, x), reverse = sortDirection < 0))
        else:
            self._dataset = iter(sorted(self._dataset, key = lambda x: _resolve_key(key_or_list, x), reverse = direction < 0))
        return self

    def count(self, with_limit_and_skip=False):
        arr = [x for x in self._dataset]
        count = len(arr)
        if with_limit_and_skip:
            if self._skip:
                count -= self._skip
            if self._limit and count > self._limit:
                count = self._limit
        self._dataset = iter(arr)
        return count

    def skip(self, count):
        self._skip = count
        return self
    def limit(self, count):
        self._limit = count if count != 0 else None
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
            value = _resolve_key(key, x)
            if value == NOTHING: continue
            unique.update(value if isinstance(value, (tuple, list)) else [value])
        return list(unique)

    def __getitem__(self, index):
        arr = [x for x in self._dataset]
        count = len(arr)
        self._dataset = iter(arr)
        return arr[index]

def _set_updater(doc, field_name, value):
    if isinstance(value, (tuple, list)):
        value = copy.deepcopy(value)
    if isinstance(doc, dict):
        doc[field_name] = value

def _inc_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = doc.get(field_name, 0) + value

def _sum_updater(doc, field_name, current, result):
    if isinstance(doc, dict):
        result = current + doc.get[field_name, 0]
        return result
