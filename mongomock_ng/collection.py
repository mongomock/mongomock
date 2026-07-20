import collections
import copy
import functools
import itertools
import math
import re
import threading
import warnings
from collections import OrderedDict
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import MutableMapping
from typing import Any
from typing import Union


try:
    from bson import BSON
    from bson import SON
    from bson.codec_options import CodecOptions
    from bson.errors import InvalidDocument
except ImportError:
    json_utils = SON = BSON = None
    CodecOptions = None
try:
    import execjs
except ImportError:
    execjs = None

try:
    from pymongo import ReadPreference
    from pymongo import ReturnDocument
    from pymongo.collation import Collation
    from pymongo.collation import validate_collation_or_none
    from pymongo.operations import IndexModel

    _READ_PREFERENCE_PRIMARY = ReadPreference.PRIMARY
except ImportError:

    class _FallbackIndexModel:
        pass

    class _FallbackReturnDocument:
        BEFORE = False
        AFTER = True

    IndexModel = _FallbackIndexModel
    ReturnDocument = _FallbackReturnDocument

    class Collation:  # type: ignore[no-redef]
        pass

    def validate_collation_or_none(  # type: ignore[no-redef]
        value: Union[Mapping[str, Any], 'Collation', None],
    ) -> Union[Mapping[str, Any], 'Collation', None]:
        return value

    from .read_preferences import PRIMARY as _READ_PREFERENCE_PRIMARY

from sentinels import NOTHING

from . import aggregate
from . import BulkWriteError
from . import codec_options as mongomock_codec_options
from . import ConfigurationError
from . import DuplicateKeyError
from . import filtering
from . import helpers
from . import InvalidOperation
from . import ObjectId
from . import OperationFailure
from . import WriteError
from .filtering import filter_applies
from .geospatial import extract_near_specs
from .geospatial import near_filter
from .geospatial import parse_geojson
from .geospatial import validate_geojson
from .helpers import _clone_document
from .not_implemented import raise_for_feature as raise_not_implemented
from .profiler import get_profiler
from .results import BulkWriteResult
from .results import DeleteResult
from .results import InsertManyResult
from .results import InsertOneResult
from .results import UpdateResult
from .session import ClientSession
from .write_concern import WriteConcern


_profiler_ctx = threading.local()

try:
    from pymongo.read_concern import ReadConcern
except ImportError:
    from .read_concern import ReadConcern


def _enroll_session(session, collection_store):
    if session is None:
        return
    if not isinstance(session, ClientSession):
        raise TypeError('session must be an instance of ClientSession')
    if session.has_ended:
        raise InvalidOperation('Cannot use a session that has ended')
    session._ensure_collection_in_transaction(collection_store)


def _profiler_set_operation(op: str):
    _profiler_ctx.operation = op


def _profiler_get_operation() -> str:
    return getattr(_profiler_ctx, 'operation', 'find')


def _profiler_set_sort(sort):
    _profiler_ctx.sort = sort


def _profiler_get_sort():
    return getattr(_profiler_ctx, 'sort', None)


_KwargOption = collections.namedtuple('_KwargOption', ['typename', 'default', 'attrs'])

_WITH_OPTIONS_KWARGS = {
    'read_preference': _KwargOption(
        'pymongo.read_preference.ReadPreference',
        _READ_PREFERENCE_PRIMARY,
        ('document', 'mode', 'mongos_mode', 'max_staleness'),
    ),
    'write_concern': _KwargOption(
        'pymongo.write_concern.WriteConcern', WriteConcern(), ('acknowledged', 'document')
    ),
}

VALID_UPDATE_PIPELINE_STAGES = (
    '$addFields',
    '$set',
    '$project',
    '$unset',
    '$replaceRoot',
    '$replaceWith',
)


def _add_to_set_contains(arr, value):
    return any(type(item) is type(value) and item == value for item in arr)


def validate_list_or_mapping(option, value):
    if not isinstance(value, (Mapping, list)):
        raise TypeError(
            f'{option} must either be a list or an instance of dict, '
            'bson.son.SON, or any other type that inherits from '
            'collections.Mapping'
        )


def _bson_encode(document, check_keys, codec_options):
    if codec_options:
        if isinstance(codec_options, mongomock_codec_options.CodecOptions):
            codec_options = codec_options.to_pymongo()
        if isinstance(codec_options, CodecOptions):
            BSON.encode(document, check_keys=check_keys, codec_options=codec_options)
    else:
        BSON.encode(document, check_keys=check_keys)


def validate_is_mapping(option, value):
    if not isinstance(value, Mapping):
        raise TypeError(
            f'{option} must be an instance of dict, bson.son.SON, or '
            'other type that inherits from '
            'collections.Mapping'
        )


def validate_is_mutable_mapping(option, value):
    if not isinstance(value, MutableMapping):
        raise TypeError(
            f'{option} must be an instance of dict, bson.son.SON, or '
            'other type that inherits from '
            'collections.MutableMapping'
        )


def validate_ok_for_replace(replacement):
    validate_is_mapping('replacement', replacement)
    if replacement:
        first = next(iter(replacement))
        if first.startswith('$'):
            raise ValueError('replacement can not include $ operators')


def validate_ok_for_update(update):
    validate_list_or_mapping('update', update)
    if not update:
        raise ValueError('update cannot be empty')
    is_document = not isinstance(update, list)
    first = next(iter(update))
    if is_document and not first.startswith('$'):
        raise ValueError('update only works with $ operators')


def validate_write_concern_params(**params):
    if params:
        WriteConcern(**params)


def validate_against_validator(document, options):
    validator = options.get('validator')
    validation_level = options.get('validationLevel', 'strict')
    validation_action = options.get('validationAction', 'error')

    if not validator or validation_level == 'off' or validation_action == 'warn':
        return

    from .filtering import filter_applies

    if not filter_applies(validator, document):
        raise WriteError('Document failed validation', code=121)


class BulkWriteOperation:
    def __init__(self, builder, selector, is_upsert=False):
        self.builder = builder
        self.selector = selector
        self.is_upsert = is_upsert

    def upsert(self):
        assert not self.is_upsert
        return BulkWriteOperation(self.builder, self.selector, is_upsert=True)

    def register_remove_op(self, multi, hint=None):
        collection = self.builder.collection
        selector = self.selector

        def exec_remove():
            if multi:
                op_result = collection.delete_many(selector, hint=hint).raw_result
            else:
                op_result = collection.delete_one(selector, hint=hint).raw_result
            if op_result.get('ok'):
                return {'nRemoved': op_result.get('n')}
            err = op_result.get('err')
            if err:
                return {'writeErrors': [err]}
            return {}

        self.builder.executors.append(exec_remove)

    def remove(self):
        assert not self.is_upsert
        self.register_remove_op(multi=True)

    def remove_one(
        self,
    ):
        assert not self.is_upsert
        self.register_remove_op(multi=False)

    def register_update_op(self, document, multi, **extra_args):
        if not extra_args.get('remove'):
            validate_ok_for_update(document)

        collection = self.builder.collection
        selector = self.selector

        def exec_update():
            result = collection._update(
                spec=selector, document=document, multi=multi, upsert=self.is_upsert, **extra_args
            )
            ret_val = {}
            if result.get('upserted'):
                ret_val['upserted'] = result.get('upserted')
                ret_val['nUpserted'] = result.get('n')
            else:
                matched = result.get('n')
                if matched is not None:
                    ret_val['nMatched'] = matched
            modified = result.get('nModified')
            if modified is not None:
                ret_val['nModified'] = modified
            if result.get('err'):
                ret_val['err'] = result.get('err')
            return ret_val

        self.builder.executors.append(exec_update)

    def update(self, document, hint=None, sort=None):
        self.register_update_op(document, multi=True, hint=hint, sort=sort)

    def update_one(self, document, hint=None, sort=None):
        self.register_update_op(document, multi=False, hint=hint, sort=sort)

    def replace_one(self, document, hint=None, sort=None):
        self.register_update_op(document, multi=False, remove=True, hint=hint, sort=sort)


def _combine_projection_spec(projection_fields_spec):
    """Re-format a projection fields spec into a nested dictionary.

    e.g: {'a': 1, 'b.c': 1, 'b.d': 1} => {'a': 1, 'b': {'c': 1, 'd': 1}}
    """

    tmp_spec = OrderedDict()
    for f, v in projection_fields_spec.items():
        if '.' not in f:
            if isinstance(tmp_spec.get(f), dict):
                if not v:
                    raise NotImplementedError(
                        f'Mongomock-ng does not support overriding excluding '
                        f'projection: {projection_fields_spec}'
                    )
                raise OperationFailure(f'Path collision at {f}')
            tmp_spec[f] = v
        else:
            split_field = f.split('.', 1)
            base_field, new_field = tuple(split_field)
            if not isinstance(tmp_spec.get(base_field), dict):
                if base_field in tmp_spec:
                    raise OperationFailure(f'Path collision at {f} remaining portion {new_field}')
                tmp_spec[base_field] = OrderedDict()
            tmp_spec[base_field][new_field] = v

    combined_spec = OrderedDict()
    for f, v in tmp_spec.items():
        if isinstance(v, dict):
            combined_spec[f] = _combine_projection_spec(v)
        else:
            combined_spec[f] = v

    return combined_spec


def _project_by_spec(doc, combined_projection_spec, is_include, container):
    if '$' in combined_projection_spec:
        if is_include:
            raise NotImplementedError('Positional projection is not implemented in mongomock-ng')
        raise OperationFailure('Cannot exclude array elements with the positional operator')

    doc_copy = container()

    for key, val in doc.items():
        spec = combined_projection_spec.get(key, NOTHING)
        if isinstance(spec, dict):
            if isinstance(val, (list, tuple)):
                doc_copy[key] = [
                    _project_by_spec(sub_doc, spec, is_include, container) for sub_doc in val
                ]
            elif isinstance(val, dict):
                doc_copy[key] = _project_by_spec(val, spec, is_include, container)
        elif (is_include and spec is not NOTHING) or (not is_include and spec is NOTHING):
            doc_copy[key] = _copy_field(val, container)

    return doc_copy


def _copy_field(obj, container):
    if isinstance(obj, list):
        new = []
        for item in obj:
            new.append(_copy_field(item, container))
        return new
    if isinstance(obj, Mapping):
        new = container()
        for key, value in obj.items():
            new[key] = _copy_field(value, container)
        return new
    return copy.copy(obj)


def _recursive_key_check_null_character(data):
    for key, value in data.items():
        if '\0' in key:
            raise InvalidDocument(f'Field names cannot contain the null character (found: {key})')
        if isinstance(value, Mapping):
            _recursive_key_check_null_character(value)


def _validate_data_fields(data):
    _recursive_key_check_null_character(data)
    for key in data:
        if key.startswith('$'):
            raise InvalidDocument(
                f'Top-level field names cannot start with the "$" sign (found: {key})'
            )


def _validate_document_stages(document):
    for stage in document:
        for stage_name in stage:
            aggregate.validate_stage_name(stage_name)
            if stage_name not in VALID_UPDATE_PIPELINE_STAGES:
                raise WriteError(f'{stage_name} is not allowed to be used within an update')


class BulkOperationBuilder:
    def __init__(self, collection, ordered=False, bypass_document_validation=False):
        self.collection = collection
        self.ordered = ordered
        self.results = {}
        self.executors = []
        self.done = False
        self._insert_returns_nModified = True
        self._update_returns_nModified = True
        self._bypass_document_validation = bypass_document_validation

    def find(self, selector):
        return BulkWriteOperation(self, selector)

    def insert(self, doc):
        def exec_insert():
            self.collection.insert_one(
                doc, bypass_document_validation=self._bypass_document_validation
            )
            return {'nInserted': 1}

        self.executors.append(exec_insert)

    def __aggregate_operation_result(self, total_result, key, value, operation_index=None):
        agg_val = total_result.get(key)
        assert agg_val is not None, f'Unknow operation result {key}={value} (unrecognized key)'
        if isinstance(agg_val, int):
            total_result[key] += value
        elif isinstance(agg_val, list):
            if key == 'upserted':
                new_element = {'index': operation_index, '_id': value}
                agg_val.append(new_element)
            else:
                agg_val.append(value)
        else:
            raise AssertionError(
                f'Fixme: missed aggreation rule for type: {type(agg_val)} for key {key}={agg_val}'
            )

    def _set_nModified_policy(self, insert, update):  # noqa: N802
        self._insert_returns_nModified = insert
        self._update_returns_nModified = update

    def execute(self, write_concern=None):
        if not self.executors:
            raise InvalidOperation('Bulk operation empty!')
        if self.done:
            raise InvalidOperation('Bulk operation already executed!')
        self.done = True
        result = {
            'nModified': 0,
            'nUpserted': 0,
            'nMatched': 0,
            'writeErrors': [],
            'upserted': [],
            'writeConcernErrors': [],
            'nRemoved': 0,
            'nInserted': 0,
        }

        has_update = False
        has_insert = False
        broken_nModified_info = False  # noqa: N806
        for index, execute_func in enumerate(self.executors):
            exec_name = execute_func.__name__
            try:
                op_result = execute_func()
            except WriteError as error:
                result['writeErrors'].append(
                    {
                        'index': index,
                        'code': error.code,
                        'errmsg': str(error),
                    }
                )
                if self.ordered:
                    break
                continue
            for key, value in op_result.items():
                self.__aggregate_operation_result(result, key, value, operation_index=index)
            if exec_name == 'exec_update':
                has_update = True
                if 'nModified' not in op_result:
                    broken_nModified_info = True  # noqa: N806
            has_insert |= exec_name == 'exec_insert'

        if broken_nModified_info:
            result.pop('nModified')
        elif (
            (has_insert and self._insert_returns_nModified)
            or (has_update and self._update_returns_nModified)
            or (self._update_returns_nModified and self._insert_returns_nModified)
        ):
            pass
        else:
            result.pop('nModified')

        if result.get('writeErrors'):
            raise BulkWriteError(result)

        return result

    def add_insert(self, doc):
        self.insert(doc)

    def add_update(
        self,
        selector,
        doc,
        multi=False,
        upsert=False,
        collation=None,
        array_filters=None,
        hint=None,
        sort=None,
    ):
        if array_filters:
            raise_not_implemented(
                'array_filters', 'Array filters are not implemented in mongomock-ng yet.'
            )
        write_operation = BulkWriteOperation(self, selector, is_upsert=upsert)
        write_operation.register_update_op(doc, multi, hint=hint, sort=sort)

    def add_replace(self, selector, doc, upsert, collation=None, hint=None, sort=None):
        write_operation = BulkWriteOperation(self, selector, is_upsert=upsert)
        write_operation.replace_one(doc, hint=hint, sort=sort)

    def add_delete(self, selector, just_one, collation=None, hint=None):
        write_operation = BulkWriteOperation(self, selector, is_upsert=False)
        write_operation.register_remove_op(not just_one, hint=hint)


class Collection:
    def __bool__(self):
        raise NotImplementedError(
            f'{type(self).__name__} objects do not implement truth '
            'value testing or bool(). Please compare '
            'with None instead: collection is not None'
        )

    def __init__(
        self,
        database,
        name,
        _db_store,
        write_concern=None,
        read_concern=None,
        read_preference=None,
        codec_options=None,
    ):
        self.database = database
        self._name = name
        self._db_store = _db_store
        self._write_concern = write_concern if write_concern is not None else WriteConcern()
        if read_concern is not None and not isinstance(read_concern, ReadConcern):
            raise TypeError('read_concern must be an instance of pymongo.read_concern.ReadConcern')
        self._read_concern = read_concern if read_concern is not None else ReadConcern()
        self._read_preference = (
            read_preference if read_preference is not None else _READ_PREFERENCE_PRIMARY
        )
        self._codec_options = (
            codec_options if codec_options is not None else mongomock_codec_options.CodecOptions()
        )

    def __repr__(self):
        return f"Collection({self.database}, '{self.name}')"

    def __getitem__(self, name):
        return self.database[self.name + '.' + name]

    def __getattr__(self, attr):
        if attr.startswith('_'):
            raise AttributeError(
                f"{self.__class__.__name__} has no attribute '{attr}'. "
                f"To access the {self.name}.{attr} collection, use database['{self.name}.{attr}']."
            )
        return self.__getitem__(attr)

    def __call__(self, *args, **kwargs):
        name = self._name if '.' not in self._name else self._name.split('.')[-1]
        raise TypeError(
            f"'Collection' object is not callable. If you meant to call the '{name}' method on a "
            "'Collection' object it is failing because no such method exists."
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.database == other.database and self.name == other.name
        return NotImplemented

    def __hash__(self):
        return hash((self.database, self.name))

    @property
    def full_name(self):
        return f'{self.database.name}.{self._name}'

    @property
    def name(self):
        return self._name

    @property
    def write_concern(self):
        return self._write_concern

    @property
    def read_concern(self):
        return self._read_concern

    @property
    def read_preference(self):
        return self._read_preference

    @property
    def codec_options(self):
        return self._codec_options

    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        warnings.warn(
            'Collection.initialize_unordered_bulk_op() is deprecated. '
            'Use Collection.bulk_write() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        return BulkOperationBuilder(
            self, ordered=False, bypass_document_validation=bypass_document_validation
        )

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        warnings.warn(
            'Collection.initialize_ordered_bulk_op() is deprecated. '
            'Use Collection.bulk_write() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        return BulkOperationBuilder(
            self, ordered=True, bypass_document_validation=bypass_document_validation
        )

    def insert_one(self, document, bypass_document_validation=False, session=None, comment=None):
        if comment:
            raise_not_implemented('comment', 'comment not implemented, but accepts')

        if not bypass_document_validation:
            validate_is_mutable_mapping('document', document)
        return InsertOneResult(
            self._insert(document, session, validate=not bypass_document_validation),
            acknowledged=True,
        )

    def insert_many(self, documents, ordered=True, bypass_document_validation=False, session=None):
        if not isinstance(documents, Iterable) or not documents:
            raise TypeError('documents must be a non-empty list')
        documents = list(documents)
        if not bypass_document_validation:
            for document in documents:
                validate_is_mutable_mapping('document', document)
        return InsertManyResult(
            self._insert(
                documents, session, ordered=ordered, validate=not bypass_document_validation
            ),
            acknowledged=True,
        )

    @property
    def _store(self):
        return self._db_store[self._name]

    def _insert(self, data, session=None, ordered=True, validate=True):
        _enroll_session(session, self._store)
        if not isinstance(data, Mapping):
            results = []
            write_errors = []
            num_inserted = 0
            for index, item in enumerate(data):
                try:
                    results.append(self._insert(item, validate=validate))
                except WriteError as error:
                    write_errors.append(
                        {
                            'index': index,
                            'code': error.code,
                            'errmsg': str(error),
                            'op': item,
                        }
                    )
                    if ordered:
                        break
                    else:
                        continue
                num_inserted += 1
            if write_errors:
                raise BulkWriteError(
                    {
                        'writeErrors': write_errors,
                        'nInserted': num_inserted,
                    }
                )
            return results

        if not all(isinstance(k, str) for k in data):
            raise ValueError('Document keys must be strings')

        if BSON:
            # bson validation
            check_keys = False
            if not check_keys:
                _validate_data_fields(data)

            _bson_encode(data, check_keys=check_keys, codec_options=self._codec_options)

        # Like pymongo, we should fill the _id in the inserted dict (odd behavior,
        # but we need to stick to it), so we must patch in-place the data dict
        if '_id' not in data:
            data['_id'] = ObjectId()

        object_id = data['_id']
        if isinstance(object_id, dict):
            object_id = helpers.hashdict(object_id)
        if object_id in self._store:
            raise DuplicateKeyError(
                'E11000 Duplicate Key Error',
                11000,
                {'keyPattern': {'_id': 1}, 'keyValue': {'_id': data['_id']}},
            )

        data = helpers.patch_datetime_awareness_in_document(data)

        if validate:
            validate_against_validator(data, self.options())

        self._store[object_id] = data
        try:
            self._ensure_uniques(data)
        except DuplicateKeyError:
            # Rollback
            del self._store[object_id]
            raise
        return data['_id']

    def _ensure_uniques(self, new_data):
        for index in self._store.indexes.values():
            if not index.get('unique'):
                continue
            unique = index.get('key')
            is_sparse = index.get('sparse')
            partial_filter_expression = index.get('partialFilterExpression')

            new_entries = self._compute_index_entries(new_data, unique, is_sparse)
            if new_entries is None:
                continue

            for doc in self._store.documents:
                if doc['_id'] == new_data.get('_id'):
                    continue
                if partial_filter_expression is not None and not filter_applies(
                    partial_filter_expression, doc
                ):
                    continue
                doc_entries = self._compute_index_entries(doc, unique, is_sparse)
                if doc_entries is None:
                    continue
                if self._entries_overlap(new_entries, doc_entries):
                    raise DuplicateKeyError(
                        'E11000 Duplicate Key Error',
                        11000,
                        {
                            'keyPattern': dict(unique),
                            'keyValue': {
                                k: helpers.get_value_by_dot(new_data, k) for k, _ in unique
                            },
                        },
                    )

    @staticmethod
    def _compute_index_entries(doc, unique, is_sparse):
        values = []
        has_array = False
        for key, _order in unique:
            value = helpers.get_value_by_dot(doc, key)
            if value is NOTHING:
                if is_sparse:
                    return None
                values.append(None)
            else:
                values.append(value)
                if isinstance(value, (list, tuple)):
                    has_array = True
        if is_sparse and all(v is None for v in values):
            return None
        if has_array:
            expanded = [list(v) if isinstance(v, (list, tuple)) else [v] for v in values]
            return [tuple(combo) for combo in itertools.product(*expanded)]
        return [tuple(values)]

    @staticmethod
    def _entries_overlap(a, b):
        try:
            return bool(set(a) & set(b))
        except TypeError:
            return any(e in b for e in a)

    @staticmethod
    def _raise_if_duplicate_index(index, indexed, indexed_list, documents_gen, details=None):
        try:
            if index in indexed:
                documents_gen.throw(
                    DuplicateKeyError('E11000 Duplicate Key Error', 11000, details), None, None
                )
            indexed.add(index)
        except TypeError:
            if index in indexed_list:
                documents_gen.throw(
                    DuplicateKeyError('E11000 Duplicate Key Error', 11000, details), None, None
                )
            indexed_list.append(index)

    def _internalize_dict(self, d):
        return {k: _clone_document(v) for k, v in d.items()}

    def options(self):
        return copy.deepcopy(self._store.options)

    def update_one(
        self,
        filter,
        update,
        upsert=False,
        bypass_document_validation=False,
        collation=None,
        array_filters=None,
        hint=None,
        session=None,
        let=None,
        sort=None,
        comment=None,
    ):
        if not bypass_document_validation:
            validate_ok_for_update(update)
        return UpdateResult(
            self._update(
                filter,
                update,
                upsert=upsert,
                hint=hint,
                session=session,
                collation=collation,
                array_filters=array_filters,
                let=let,
                sort=sort,
                validate=not bypass_document_validation,
            ),
            acknowledged=True,
        )

    def update_many(
        self,
        filter,
        update,
        upsert=False,
        array_filters=None,
        bypass_document_validation=False,
        collation=None,
        hint=None,
        session=None,
        let=None,
    ):
        if not bypass_document_validation:
            validate_ok_for_update(update)
        return UpdateResult(
            self._update(
                filter,
                update,
                upsert=upsert,
                multi=True,
                hint=hint,
                session=session,
                collation=collation,
                array_filters=array_filters,
                let=let,
                validate=not bypass_document_validation,
            ),
            acknowledged=True,
        )

    def replace_one(
        self,
        filter,
        replacement,
        upsert=False,
        bypass_document_validation=False,
        session=None,
        hint=None,
        sort=None,
    ):
        if not bypass_document_validation:
            validate_ok_for_replace(replacement)
        return UpdateResult(
            self._update(
                filter,
                replacement,
                upsert=upsert,
                hint=hint,
                session=session,
                sort=sort,
                validate=not bypass_document_validation,
            ),
            acknowledged=True,
        )

    def _update(
        self,
        spec,
        document,
        upsert=False,
        manipulate=False,
        multi=False,
        check_keys=False,
        hint=None,
        session=None,
        collation=None,
        let=None,
        array_filters=None,
        sort=None,
        validate=True,
        **kwargs,
    ):
        _enroll_session(session, self._store)
        if hint:
            raise NotImplementedError(
                'The hint argument of update is valid but has not been implemented in '
                'mongomock-ng yet'
            )
        if collation:
            raise_not_implemented(
                'collation',
                'The collation argument of update is valid but has not been implemented in '
                'mongomock-ng yet',
            )
        if let:
            raise_not_implemented(
                'let',
                'The let argument of update is valid but has not been implemented in mongomock-ng '
                'yet',
            )
        validate_is_mapping('spec', spec)
        validate_list_or_mapping('document', document)
        spec = helpers.patch_datetime_awareness_in_document(spec)
        document = helpers.patch_datetime_awareness_in_document(document)

        if isinstance(document, list):
            _validate_document_stages(document)

        if self.database.client.server_info()['versionArray'] < [5]:
            for operator in _updaters:
                if not document.get(operator, True):
                    raise WriteError(
                        f"'{operator}' is empty. You must specify a field like so: "
                        '{' + operator + ': {<field>: ...}}'
                    )

        self._current_array_filters = array_filters or []
        try:
            return self._update_documents(
                spec, document, upsert, multi, sort, session, validate=validate
            )
        finally:
            self._current_array_filters = None

    def _update_documents(self, spec, document, upsert, multi, sort, session=None, validate=True):
        _profiler_set_operation('update')
        updated_existing = False
        upserted_id = None
        num_updated = 0
        num_matched = 0

        if sort:
            documents = list(self._get_dataset(spec, sort, None, dict))
        else:
            documents = list(self._iter_documents(spec))

        for existing_document in itertools.chain(documents, [None]):
            was_insert = False
            if existing_document is None:
                if not upsert or num_matched:
                    continue
                if spec.get('_id') is not None:
                    _id = spec['_id']
                elif not isinstance(document, list) and document.get('_id') is not None:
                    _id = document['_id']
                else:
                    _id = ObjectId()
                to_insert = dict(spec, _id=_id)
                to_insert = self._expand_dots(to_insert)
                to_insert, _ = self._discard_operators(to_insert)
                existing_document = to_insert
                was_insert = True
            else:
                original_document_snapshot = _clone_document(existing_document)
                updated_existing = True
            num_matched += 1

            if isinstance(document, list):
                self._apply_update_pipeline(existing_document, document, session)
            else:
                self._apply_update_document(existing_document, spec, document, was_insert)

            if was_insert:
                upserted_id = self._insert(existing_document, validate=validate)
                num_updated += 1
            elif existing_document != original_document_snapshot:
                if original_document_snapshot.get('_id') != existing_document.get('_id'):
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise WriteError(
                        "After applying the update, the (immutable) field '_id' was found to have "
                        'been altered to _id: {}'.format(existing_document.get('_id'))
                    )
                if validate:
                    options = self.options()
                    validation_level = options.get('validationLevel', 'strict')
                    if validation_level == 'moderate':
                        try:
                            validate_against_validator(original_document_snapshot, options)
                        except WriteError:
                            validate = False

                if validate:
                    try:
                        validate_against_validator(existing_document, options)
                    except WriteError:
                        self._store[original_document_snapshot['_id']] = original_document_snapshot
                        raise
                try:
                    self._ensure_uniques(existing_document)
                    self._store[existing_document['_id']] = existing_document
                    num_updated += 1
                except DuplicateKeyError:
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise

            if not multi:
                break

        return {
            'connectionId': self.database.client._id,
            'err': None,
            'n': num_matched,
            'nModified': num_updated if updated_existing else 0,
            'ok': 1,
            'upserted': upserted_id,
            'updatedExisting': updated_existing,
        }

    def _apply_update_pipeline(self, existing_document, pipeline, session):
        """Apply the aggregation pipeline to a single document.

        This method updates existing_document in-place.
        """

        [new_document] = aggregate.process_pipeline(
            [existing_document], self.database, pipeline, session
        )
        existing_document.clear()
        existing_document.update(new_document)

    def _apply_update_document(self, existing_document, spec, document, was_insert):
        """Apply document, which is an update document, to existing_document.

        This method updates existing_document in-place.
        """
        first = True
        subdocument = None
        for k, v in document.items():
            if k in _updaters:
                updater = _updaters[k]
                subdocument = self._update_document_fields_with_positional_awareness(
                    existing_document, v, spec, updater, subdocument
                )
            elif k == '$rename':
                for src, dst in v.items():
                    src_parts = src.split('.')
                    dst_parts = dst.split('.')

                    src_doc = existing_document
                    for part in src_parts[:-1]:
                        if isinstance(src_doc, MutableMapping) and part in src_doc:
                            src_doc = src_doc[part]
                        else:
                            break
                    else:
                        if isinstance(src_doc, MutableMapping) and src_parts[-1] in src_doc:
                            value = src_doc.pop(src_parts[-1])
                            dst_doc = existing_document
                            for part in dst_parts[:-1]:
                                if isinstance(dst_doc, MutableMapping):
                                    dst_doc = dst_doc.setdefault(part, {})
                                else:
                                    break
                            else:
                                if isinstance(dst_doc, MutableMapping):
                                    dst_doc[dst_parts[-1]] = value
            elif k == '$setOnInsert':
                if not was_insert:
                    continue
                subdocument = self._update_document_fields_with_positional_awareness(
                    existing_document, v, spec, _set_updater, subdocument
                )
            elif k == '$currentDate':
                subdocument = self._update_document_fields_with_positional_awareness(
                    existing_document, v, spec, _current_date_updater, subdocument
                )
            elif k == '$addToSet':
                self._apply_add_to_set_operator(existing_document, spec, v)
            elif k == '$pull':
                self._apply_pull_operator(existing_document, spec, v)
            elif k == '$pullAll':
                self._apply_pull_all_operator(existing_document, spec, v)
            elif k == '$push':
                subdocument = self._apply_push_operator(existing_document, v, spec, subdocument)
            elif first:
                self._apply_replace_document(existing_document, spec, document)
                break
            else:
                raise ValueError(f'Invalid modifier specified: {k}')
            first = False
        if not document:
            self._apply_empty_document(existing_document, spec)

    def _apply_add_to_set_operator(self, existing_document, spec, v):
        """Apply $addToSet operator."""
        for field, value in v.items():
            nested_field_list = field.rsplit('.')
            if len(nested_field_list) == 1:
                if field not in existing_document:
                    existing_document[field] = []
                if isinstance(value, dict) and '$each' in value:
                    each_values = value['$each']
                    if not isinstance(each_values, list):
                        each_values = [each_values]
                    for obj in each_values:
                        if not _add_to_set_contains(existing_document[field], obj):
                            existing_document[field].append(obj)
                    continue
                if not _add_to_set_contains(existing_document[field], value):
                    existing_document[field].append(value)
                continue
            else:
                subdocument = existing_document
                for field_part in nested_field_list[:-1]:
                    if field_part == '$':
                        break
                    if field_part not in subdocument:
                        subdocument[field_part] = {}
                    subdocument = subdocument[field_part]
                subdocument, _ = self._get_subdocument(existing_document, spec, nested_field_list)
                push_results = []
                if nested_field_list[-1] in subdocument:
                    push_results = subdocument[nested_field_list[-1]]
                if isinstance(value, dict) and '$each' in value:
                    each_values = value['$each']
                    if not isinstance(each_values, list):
                        each_values = [each_values]
                    for obj in each_values:
                        if not _add_to_set_contains(push_results, obj):
                            push_results.append(obj)
                elif not _add_to_set_contains(push_results, value):
                    push_results.append(value)
                subdocument[nested_field_list[-1]] = push_results

    def _apply_pull_operator(self, existing_document, spec, v):
        """Apply $pull operator."""
        for field, value in v.items():
            nested_field_list = field.rsplit('.')
            if '$' in nested_field_list:
                subdocument, _ = self._get_subdocument(existing_document, spec, nested_field_list)
                pull_results = []
                for obj in subdocument[nested_field_list[-1]]:
                    if isinstance(obj, dict):
                        for pull_key, pull_value in value.items():
                            if obj[pull_key] != pull_value:
                                pull_results.append(obj)
                        continue
                    if obj != value:
                        pull_results.append(obj)
                subdocument[nested_field_list[-1]] = pull_results
            else:
                arr = existing_document
                for field_part in nested_field_list:
                    if field_part not in arr:
                        break
                    arr = arr[field_part]
                if not isinstance(arr, list):
                    continue
                arr_copy = _clone_document(arr)
                if isinstance(value, dict):
                    for obj in arr_copy:
                        try:
                            is_matching = filter_applies(value, obj)
                        except OperationFailure:
                            is_matching = False
                        if is_matching:
                            arr.remove(obj)
                            continue
                        if filter_applies({'field': value}, {'field': obj}):
                            arr.remove(obj)
                else:
                    for obj in arr_copy:
                        if value == obj:
                            arr.remove(obj)

    def _apply_pull_all_operator(self, existing_document, spec, v):
        """Apply $pullAll operator."""
        for field, value in v.items():
            nested_field_list = field.rsplit('.')
            if len(nested_field_list) == 1:
                if field in existing_document:
                    arr = existing_document[field]
                    existing_document[field] = [obj for obj in arr if obj not in value]
                continue
            else:
                subdocument, _ = self._get_subdocument(existing_document, spec, nested_field_list)
                if nested_field_list[-1] in subdocument:
                    arr = subdocument[nested_field_list[-1]]
                    subdocument[nested_field_list[-1]] = [obj for obj in arr if obj not in value]

    def _apply_push_operator(self, existing_document, v, spec, subdocument):
        """Apply $push operator."""
        for field, value in v.items():
            nested_field_list = field.rsplit('.')
            subdocument, field = self._get_subdocument(existing_document, spec, nested_field_list)
            if isinstance(subdocument, dict) and field not in subdocument:
                subdocument[field] = []
            push_results = subdocument[field]
            if isinstance(value, dict) and '$each' in value:
                if '$position' in value:
                    push_results = (
                        push_results[0 : value['$position']]
                        + list(value['$each'])
                        + push_results[value['$position'] :]
                    )
                else:
                    push_results += list(value['$each'])
                if '$sort' in value:
                    sort_spec = value['$sort']
                    if isinstance(sort_spec, dict):
                        sort_key = set(sort_spec.keys()).pop()
                        push_results = sorted(
                            push_results,
                            key=lambda d: helpers.get_value_by_dot(d, sort_key),
                            reverse=set(sort_spec.values()).pop() < 0,
                        )
                    else:
                        push_results = sorted(push_results, reverse=sort_spec < 0)
                if '$slice' in value:
                    slice_value = value['$slice']
                    if slice_value < 0:
                        push_results = push_results[slice_value:]
                    elif slice_value == 0:
                        push_results = []
                    else:
                        push_results = push_results[:slice_value]
                unused_modifiers = set(value.keys()) - {
                    '$each',
                    '$slice',
                    '$position',
                    '$sort',
                }
                if unused_modifiers:
                    raise WriteError('Unrecognized clause in $push: ' + unused_modifiers.pop())
            else:
                push_results.append(value)
            subdocument[field] = push_results
        return subdocument

    def _apply_replace_document(self, existing_document, spec, document):
        """Replace entire document."""
        for key in document:
            if key.startswith('$'):
                raise ValueError(f'field names cannot start with $ [{key}]')
        _id = spec.get('_id', existing_document.get('_id'))
        existing_document.clear()
        if _id is not None:
            existing_document['_id'] = _id
        if BSON:
            check_keys = False
            if not check_keys:
                _validate_data_fields(document)
            _bson_encode(document, check_keys=check_keys, codec_options=self.codec_options)
        existing_document.update(self._internalize_dict(document))
        if existing_document['_id'] != _id:
            raise OperationFailure(
                'The _id field cannot be changed from {} to {}'.format(
                    existing_document['_id'], _id
                )
            )

    def _apply_empty_document(self, existing_document, spec):
        """Handle empty document case."""
        _id = spec.get('_id', existing_document.get('_id'))
        existing_document.clear()
        if _id:
            existing_document['_id'] = _id

    def _get_subdocument(self, existing_document, spec, nested_field_list):
        """This method retrieves the subdocument of the existing_document.nested_field_list.

        It uses the spec to filter through the items. It will continue to grab nested documents
        until it can go no further. It will then return the subdocument that was last saved.
        '$' is the positional operator, so we use the $elemMatch in the spec to find the right
        subdocument in the array.
        """
        # Current document in view.
        doc = existing_document
        # Previous document in view.
        parent_doc = existing_document
        # Current spec in view.
        subspec = spec
        # Whether spec is following the document.
        is_following_spec = True
        # Walk down the dictionary.
        for index, subfield in enumerate(nested_field_list):
            if subfield == '$':
                if not is_following_spec:
                    raise WriteError(
                        'The positional operator did not find the match needed from the query'
                    )
                # Positional element should have the equivalent elemMatch in the query.
                subspec = subspec['$elemMatch']
                is_following_spec = False
                # Iterate through.
                for spec_index, item in enumerate(doc):
                    if filter_applies(subspec, item):
                        subfield = spec_index
                        break
                else:
                    raise WriteError(
                        'The positional operator did not find the match needed from the query'
                    )

            parent_doc = doc
            if isinstance(parent_doc, list):
                subfield = int(subfield)
                if is_following_spec and (subfield < 0 or subfield >= len(subspec)):
                    is_following_spec = False

            if index == len(nested_field_list) - 1:
                return parent_doc, subfield

            if not isinstance(parent_doc, list):
                if subfield not in parent_doc:
                    parent_doc[subfield] = {}
                if is_following_spec and subfield not in subspec:
                    is_following_spec = False

            doc = parent_doc[subfield]
            if is_following_spec:
                subspec = subspec[subfield]

    def _expand_dots(self, doc):
        expanded = {}
        paths = {}
        for k, v in doc.items():

            def _raise_incompatible(subkey):
                raise WriteError(
                    f"cannot infer query fields to set, both paths '{k}' and "  # noqa: B023
                    f"'{paths[subkey]}' are matched"
                )

            if k in paths:
                _raise_incompatible(k)

            key_parts = k.split('.')
            sub_expanded = expanded

            paths[k] = k
            for i, key_part in enumerate(key_parts[:-1]):
                if key_part not in sub_expanded:
                    sub_expanded[key_part] = {}
                sub_expanded = sub_expanded[key_part]
                key = '.'.join(key_parts[: i + 1])
                if not isinstance(sub_expanded, dict):
                    _raise_incompatible(key)
                paths[key] = k
            sub_expanded[key_parts[-1]] = v
        return expanded

    def _discard_operators(self, doc):
        if not doc or not isinstance(doc, dict):
            return doc, False
        new_doc = OrderedDict()
        for k, v in doc.items():
            if k == '$eq':
                return v, False
            if k.startswith('$'):
                continue
            new_v, discarded = self._discard_operators(v)
            if not discarded:
                new_doc[k] = new_v
        return new_doc, not bool(new_doc)

    def find(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        no_cursor_timeout=False,
        cursor_type=None,
        sort=None,
        allow_partial_results=False,
        oplog_replay=False,
        modifiers=None,
        batch_size=0,
        manipulate=True,
        collation=None,
        session=None,
        max_time_ms=None,
        allow_disk_use=False,
        comment=None,
        hint=None,
        **kwargs,
    ):
        spec = filter
        if spec is None:
            spec = {}
        validate_is_mapping('filter', spec)
        for kwarg, value in kwargs.items():
            if value:
                raise OperationFailure(f"Unrecognized field '{kwarg}'")
        return (
            Cursor(self, spec, sort, projection, skip, limit, collation=collation)
            .max_time_ms(max_time_ms)
            .allow_disk_use(allow_disk_use)
        )

    def _get_dataset(self, spec, sort, fields, as_class):
        _profiler_set_sort(sort)
        near_specs, clean_spec = extract_near_specs(spec) if spec else ([], spec)
        if near_specs:
            for field_path, _near_info in near_specs:
                if not self._has_2dsphere_index_on(field_path):
                    raise OperationFailure(
                        f'unable to find index for $near query on field "{field_path}"'
                    )
        dataset = self._iter_documents(clean_spec if near_specs else spec)
        if near_specs and not sort:
            docs_with_dist = []
            for doc in dataset:
                distances = []
                for field_path, near_info in near_specs:
                    doc_val = filtering.resolve_key(field_path, doc)
                    if doc_val is NOTHING:
                        continue
                    try:
                        doc_geo = parse_geojson(doc_val)
                        validate_geojson(doc_geo)
                    except Exception:  # noqa: S112
                        continue
                    is_match, dist = near_filter(
                        doc_geo,
                        near_info['query_point'],
                        max_distance=near_info.get('max_distance'),
                        min_distance=near_info.get('min_distance'),
                        spherical=near_info.get('spherical', False),
                    )
                    if is_match and dist is not None:
                        distances.append(dist)
                if distances:
                    # When multiple $near specs exist, sort by min(distance).
                    # This makes the first $near field primary for sort order.
                    docs_with_dist.append((min(distances), doc))
            dataset = (doc for _, doc in sorted(docs_with_dist, key=lambda x: x[0]))
        if sort:
            if isinstance(sort, dict):
                sort = sort.items()

            normalized_sort = []
            for item in sort:
                if isinstance(item, str):
                    sort_key = item
                    sort_direction = 1
                else:
                    sort_key, sort_direction = item
                normalized_sort.append((sort_key, sort_direction))

            for sort_key, sort_direction in reversed(normalized_sort):
                if sort_key == '$natural':
                    if sort_direction < 0:
                        dataset = iter(reversed(list(dataset)))
                    continue
                if isinstance(sort_key, str) and sort_key.startswith('$'):
                    raise NotImplementedError(
                        f'Sorting by {sort_key} is not implemented in mongomock-ng yet'
                    )
                dataset = iter(
                    sorted(
                        dataset,
                        key=lambda x: filtering.resolve_sort_key(sort_key, x),
                        reverse=sort_direction < 0,
                    )
                )
        for document in dataset:
            yield self._copy_only_fields(document, fields, as_class)

    def _extract_projection_operators(self, fields):
        """Removes and returns fields with projection operators."""
        result = {}
        allowed_projection_operators = {'$elemMatch', '$slice'}
        for key, value in fields.items():
            if isinstance(value, dict):
                for op in value:
                    if op not in allowed_projection_operators:
                        raise ValueError(f'Unsupported projection option: {op}')
                result[key] = value

        for key in result:
            del fields[key]

        return result

    def _apply_projection_operators(self, ops, doc, doc_copy):
        """Applies projection operators to copied document."""
        for field, op in ops.items():
            if field not in doc_copy:
                if field in doc:
                    # field was not copied yet (since we are in include mode)
                    doc_copy[field] = doc[field]
                else:
                    # field doesn't exist in original document, no work to do
                    continue

            if '$slice' in op:
                if not isinstance(doc_copy[field], list):
                    raise OperationFailure(
                        f'Unsupported type {type(doc_copy[field])} for slicing operation: {op}'
                    )
                op_value = op['$slice']
                slice_ = None
                if isinstance(op_value, list):
                    if len(op_value) != 2:
                        raise OperationFailure(
                            f'Unsupported slice format {op_value} for slicing operation: {op}'
                        )
                    skip, limit = op_value
                    if skip < 0:
                        skip = len(doc_copy[field]) + skip
                    last = min(skip + limit, len(doc_copy[field]))
                    slice_ = slice(skip, last)
                elif isinstance(op_value, int):
                    count = op_value
                    start = 0
                    end = len(doc_copy[field])
                    if count < 0:
                        start = max(0, len(doc_copy[field]) + count)
                    else:
                        end = min(count, len(doc_copy[field]))
                    slice_ = slice(start, end)

                if slice_:
                    doc_copy[field] = doc_copy[field][slice_]
                else:
                    raise OperationFailure(
                        f'Unsupported slice value {op_value} for slicing operation: {op}'
                    )

            if '$elemMatch' in op:
                if isinstance(doc_copy[field], list):
                    # find the first item that matches
                    matched = False
                    for item in doc_copy[field]:
                        if filter_applies(op['$elemMatch'], item):
                            matched = True
                            doc_copy[field] = [item]
                            break

                    # nothing have matched
                    if not matched:
                        del doc_copy[field]

                else:
                    # remove the field since there is nothing to iterate
                    del doc_copy[field]

    def _copy_only_fields(self, doc, fields, container):
        """Copy only the specified fields."""

        # https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#collection-find-returns-entire-document-with-empty-projection
        if fields is None or fields == []:
            return _copy_field(doc, container)

        if not fields:
            fields = {'_id': 1}
        if not isinstance(fields, dict):
            fields = helpers.fields_list_to_dict(fields)

        # we can pass in something like {'_id':0, 'field':1}, so pull the id
        # value out and hang on to it until later
        id_value = fields.pop('_id', 1)

        # filter out fields with projection operators, we will take care of them later
        projection_operators = self._extract_projection_operators(fields)

        # other than the _id field, all fields must be either includes or
        # excludes, this can evaluate to 0
        if len(set(fields.values())) > 1:
            raise ValueError('You cannot currently mix including and excluding fields.')

        # if we have novalues passed in, make a doc_copy based on the
        # id_value
        if not fields:
            doc_copy = container() if id_value == 1 else _copy_field(doc, container)
        else:
            doc_copy = _project_by_spec(
                doc,
                _combine_projection_spec(fields),
                is_include=next(iter(fields.values())),
                container=container,
            )

        # set the _id value if we requested it, otherwise remove it
        if id_value == 0:
            doc_copy.pop('_id', None)
        elif '_id' in doc:
            doc_copy['_id'] = doc['_id']

        fields['_id'] = id_value  # put _id back in fields

        # time to apply the projection operators and put back their fields
        self._apply_projection_operators(projection_operators, doc, doc_copy)
        for field, op in projection_operators.items():
            fields[field] = op
        return doc_copy

    def _update_document_fields(self, doc, fields, updater):
        """Implements the $set behavior on an existing document"""
        for k, v in fields.items():
            self._update_document_single_field(doc, k, v, updater)

    def _update_document_fields_positional(self, doc, fields, spec, updater, subdocument=None):
        for k, v in fields.items():
            if '$' not in k:
                self._update_document_single_field(doc, k, v, updater)
                continue

            field_name_parts = k.split('.')
            if subdocument:
                updater(subdocument, field_name_parts[-1], v, codec_options=self.codec_options)
                continue

            current_doc = doc
            subspec = spec
            _handle_all_positional = False
            for idx, part in enumerate(field_name_parts[:-1]):
                if part == '$':
                    subspec_dollar = subspec.get('$elemMatch', subspec)
                    for item in current_doc:
                        if filter_applies(subspec_dollar, item):
                            current_doc = item
                            break
                    continue

                if part == '$[]':
                    remaining = field_name_parts[idx + 1 :]
                    if not remaining:
                        for i in range(len(current_doc)):
                            updater(current_doc, str(i), v, codec_options=self.codec_options)
                    else:
                        sub_k = '.'.join(remaining)
                        for item in current_doc:
                            self._update_document_single_field(item, sub_k, v, updater)
                    _handle_all_positional = True
                    break

                filter_id = _parse_array_filter_id(part)
                if filter_id is not None:
                    filter_spec = _lookup_array_filter(self._current_array_filters, filter_id, k)

                    remaining = field_name_parts[idx + 1 :]
                    if not remaining:
                        for i, item in enumerate(current_doc):
                            if _array_filter_applies(filter_spec, filter_id, item):
                                updater(current_doc, str(i), v, codec_options=self.codec_options)
                    else:
                        sub_k = '.'.join(remaining)
                        for item in current_doc:
                            if _array_filter_applies(filter_spec, filter_id, item):
                                self._update_document_single_field(item, sub_k, v, updater)
                    _handle_all_positional = True
                    break

                new_spec = {}
                for el in subspec:
                    if el.startswith(part):
                        if len(el.split('.')) > 1:
                            new_spec['.'.join(el.split('.')[1:])] = subspec[el]
                        else:
                            new_spec = subspec[el]
                subspec = new_spec
                current_doc = current_doc[part]

            if _handle_all_positional:
                continue

            target_doc = current_doc
            if field_name_parts[-1] == '$' and isinstance(target_doc, list):
                for i, doc in enumerate(target_doc):
                    subspec_dollar = subspec.get('$elemMatch', subspec)
                    if filter_applies(subspec_dollar, doc):
                        target_doc[i] = v
                        break
                continue

            if field_name_parts[-1] == '$[]' and isinstance(target_doc, list):
                for i in range(len(target_doc)):
                    updater(target_doc, str(i), v, codec_options=self.codec_options)
                continue

            filter_id = _parse_array_filter_id(field_name_parts[-1])
            if filter_id is not None and isinstance(target_doc, list):
                filter_spec = _lookup_array_filter(self._current_array_filters, filter_id, k)
                for i, item in enumerate(target_doc):
                    if _array_filter_applies(filter_spec, filter_id, item):
                        updater(target_doc, str(i), v, codec_options=self.codec_options)
                continue

            updater(target_doc, field_name_parts[-1], v, codec_options=self.codec_options)

        return subdocument

    def _update_document_fields_with_positional_awareness(
        self, existing_document, v, spec, updater, subdocument
    ):
        positional = any('$' in key for key in v)

        if positional:
            return self._update_document_fields_positional(
                existing_document, v, spec, updater, subdocument
            )
        self._update_document_fields(existing_document, v, updater)
        return subdocument

    def _update_document_single_field(self, doc, field_name, field_value, updater):
        field_name_parts = field_name.split('.')
        for part in field_name_parts[:-1]:
            if isinstance(doc, list):
                try:
                    doc = doc[int(part)]
                    continue
                except (ValueError, IndexError):
                    pass
            elif isinstance(doc, MutableMapping):
                if updater is _unset_updater and part not in doc:
                    return
                doc = doc.setdefault(part, {})
            else:
                return
        field_name = field_name_parts[-1]
        updater(doc, field_name, field_value, codec_options=self.codec_options)

    def _iter_documents(self, filter):
        get_profiler().record(
            filter,
            self.full_name,
            _profiler_get_operation(),
            dict(self._list_all_indexes()) if self._store.is_created else {},
            sort=_profiler_get_sort(),
        )

        if self._store.is_empty:
            filter_applies(filter, {})

        return (
            document for document in list(self._store.documents) if filter_applies(filter, document)
        )

    def find_one(self, filter=None, *args, **kwargs):  # pylint: disable=keyword-arg-before-vararg
        # Allow calling find_one with a non-dict argument that gets used as
        # the id for the query.
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}

        try:
            return next(self.find(filter, *args, **kwargs))
        except StopIteration:
            return None

    def find_one_and_delete(self, filter, projection=None, sort=None, **kwargs):
        kwargs['remove'] = True
        validate_is_mapping('filter', filter)
        return self._find_and_modify(filter, projection, sort=sort, **kwargs)

    def find_one_and_replace(
        self,
        filter,
        replacement,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        **kwargs,
    ):
        validate_is_mapping('filter', filter)
        validate_ok_for_replace(replacement)
        return self._find_and_modify(
            filter, projection, replacement, upsert, sort, return_document, **kwargs
        )

    def find_one_and_update(
        self,
        filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        **kwargs,
    ):
        validate_is_mapping('filter', filter)
        validate_ok_for_update(update)
        return self._find_and_modify(
            filter, projection, update, upsert, sort, return_document, **kwargs
        )

    def _find_and_modify(
        self,
        query,
        projection=None,
        update=None,
        upsert=False,
        sort=None,
        return_document=ReturnDocument.BEFORE,
        session=None,
        **kwargs,
    ):
        _enroll_session(session, self._store)
        remove = kwargs.get('remove', False)
        if kwargs.get('new', False) and remove:
            # message from mongodb
            raise OperationFailure("remove and returnNew can't co-exist")

        if not (remove or update):
            raise ValueError('Must either update or remove')

        if remove and update:
            raise ValueError("Can't do both update and remove")

        old = self.find_one(query, projection=projection, sort=sort)
        if not old and not upsert:
            return

        if old and '_id' in old:
            query = {'_id': old['_id']}

        if remove:
            self.delete_one(query)
        else:
            updated = self._update(query, update, upsert)
            if updated['upserted']:
                query = {'_id': updated['upserted']}

        if return_document is ReturnDocument.AFTER or kwargs.get('new'):
            return self.find_one(query, projection)
        return old

    def delete_one(self, filter, collation=None, hint=None, session=None, comment=None, let=None):
        validate_is_mapping('filter', filter)
        return DeleteResult(
            self._delete(filter, collation=collation, hint=hint, session=session), True
        )

    def delete_many(self, filter, collation=None, hint=None, session=None):
        validate_is_mapping('filter', filter)
        return DeleteResult(
            self._delete(filter, collation=collation, hint=hint, multi=True, session=session), True
        )

    def _delete(self, filter, collation=None, hint=None, multi=False, session=None):
        if hint:
            raise NotImplementedError(
                'The hint argument of delete is valid but has not been implemented in '
                'mongomock-ng yet'
            )
        if collation:
            raise_not_implemented(
                'collation',
                'The collation argument of delete is valid but has not been '
                'implemented in mongomock-ng yet',
            )
        _enroll_session(session, self._store)
        _profiler_set_operation('delete')
        filter = helpers.patch_datetime_awareness_in_document(filter)
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}
        to_delete = list(self.find(filter))
        deleted_count = 0
        for doc in to_delete:
            doc_id = doc['_id']
            if isinstance(doc_id, dict):
                doc_id = helpers.hashdict(doc_id)
            del self._store[doc_id]
            deleted_count += 1
            if not multi:
                break

        return {
            'connectionId': self.database.client._id,
            'n': deleted_count,
            'ok': 1.0,
            'err': None,
        }

    def count_documents(self, filter, comment=None, **kwargs):
        if comment:
            raise_not_implemented('comment', 'comment not implemented, but accepts')

        if kwargs.pop('hint', None):
            raise_not_implemented('hint', 'hint not implemented, but accepts')

        if kwargs.pop('collation', None):
            raise_not_implemented(
                'collation',
                'The collation argument of count_documents is valid but has not been '
                'implemented in mongomock-ng yet',
            )
        if kwargs.pop('session', None):
            raise_not_implemented('session', 'Mongomock-ng does not handle sessions yet')
        skip = kwargs.pop('skip', 0)
        if 'limit' in kwargs:
            limit = kwargs.pop('limit')
            if not isinstance(limit, (int, float)):
                raise OperationFailure('the limit must be specified as a number')
            if limit <= 0:
                raise OperationFailure('the limit must be positive')
            limit = math.floor(limit)
        else:
            limit = None
        unknown_kwargs = set(kwargs) - {'maxTimeMS', 'hint'}
        if unknown_kwargs:
            raise OperationFailure(f"unrecognized field '{next(iter(unknown_kwargs))}'")

        _profiler_set_operation('count')
        spec = helpers.patch_datetime_awareness_in_document(filter)
        doc_num = len(list(self._iter_documents(spec)))
        count = max(doc_num - skip, 0)
        return count if limit is None else min(count, limit)

    def estimated_document_count(self, comment=None, **kwargs):
        if comment:
            raise_not_implemented('comment', 'comment not implemented, but accepts')

        if kwargs.pop('session', None):
            raise ConfigurationError('estimated_document_count does not support sessions')
        unknown_kwargs = set(kwargs) - {'limit', 'maxTimeMS', 'hint', 'comment'}

        if self.database.client.server_info()['versionArray'] < [5]:
            unknown_kwargs -= {'skip'}

        unknown_kwargs -= {'skip'}

        if unknown_kwargs:
            raise OperationFailure(
                f"BSON field 'count.{next(iter(unknown_kwargs))}' is an unknown field."
            )
        return self.count_documents({}, **kwargs)

    def drop(self, session=None):
        _enroll_session(session, self._store)
        self.database.drop_collection(self.name)

    def create_index(self, keys, cache_for=300, session=None, **kwargs):
        _enroll_session(session, self._store)
        index_list = helpers.create_index_list(keys)
        is_unique = kwargs.pop('unique', False)
        is_sparse = kwargs.pop('sparse', False)

        index_name = kwargs.pop('name', helpers.gen_index_name(index_list))
        config = {'key': index_list}
        if is_sparse:
            config['sparse'] = True
        if is_unique:
            config['unique'] = True
        if 'expireAfterSeconds' in kwargs and kwargs['expireAfterSeconds'] is not None:
            config['expireAfterSeconds'] = kwargs.pop('expireAfterSeconds')
        if 'partialFilterExpression' in kwargs and kwargs['partialFilterExpression'] is not None:
            config['partialFilterExpression'] = kwargs.pop('partialFilterExpression')

        is_2dsphere = any(direction == '2dsphere' for _, direction in index_list)
        if is_2dsphere:
            if is_unique:
                raise OperationFailure('cannot create a unique index on a 2dsphere index')
            config['key'] = index_list

        existing_index = self._store.indexes.get(index_name)
        if existing_index and config != existing_index:
            raise OperationFailure(
                f'Index with name: {index_name} already exists with different options'
            )

        # Check that documents already verify the uniquess of this new index.
        if is_unique and not is_2dsphere:
            indexed = set()
            indexed_list = []
            documents_gen = self._store.documents
            index_partial_filter = config.get('partialFilterExpression')
            for doc in documents_gen:
                if index_partial_filter is not None and not filter_applies(
                    index_partial_filter, doc
                ):
                    continue
                values = []
                has_array = False
                skip_doc = False
                for key, _order in index_list:
                    value = helpers.get_value_by_dot(doc, key)
                    if value is NOTHING:
                        if is_sparse:
                            skip_doc = True
                            break
                        values.append(None)
                    else:
                        values.append(value)
                        if isinstance(value, (list, tuple)):
                            has_array = True
                if skip_doc:
                    continue
                if is_sparse and all(v is None for v in values):
                    continue
                _dup_details = {
                    'keyPattern': dict(index_list),
                    'keyValue': {k: helpers.get_value_by_dot(doc, k) for k, _ in index_list},
                }
                if has_array:
                    expanded = [list(v) if isinstance(v, (list, tuple)) else [v] for v in values]
                    for combo in itertools.product(*expanded):
                        index = tuple(combo)
                        self._raise_if_duplicate_index(
                            index, indexed, indexed_list, documents_gen, details=_dup_details
                        )
                else:
                    index = tuple(values)
                    self._raise_if_duplicate_index(
                        index, indexed, indexed_list, documents_gen, details=_dup_details
                    )

        self._store.create_index(index_name, config)

        return index_name

    def create_indexes(self, indexes, session=None):
        for index in indexes:
            if not isinstance(index, IndexModel):
                raise TypeError(f'{index} is not an instance of pymongo.operations.IndexModel')

        return [
            self.create_index(
                index.document['key'].items(),
                session=session,
                expireAfterSeconds=index.document.get('expireAfterSeconds'),
                unique=index.document.get('unique', False),
                sparse=index.document.get('sparse', False),
                name=index.document.get('name'),
                partialFilterExpression=index.document.get('partialFilterExpression'),
            )
            for index in indexes
        ]

    def drop_index(self, index_or_name, session=None):
        _enroll_session(session, self._store)
        if isinstance(index_or_name, list):
            name = helpers.gen_index_name(index_or_name)
        else:
            name = index_or_name
        try:
            self._store.drop_index(name)
        except KeyError as err:
            raise OperationFailure(f'index not found with name [{name}]') from err

    def drop_indexes(self, session=None):
        _enroll_session(session, self._store)
        self._store.indexes = {}

    def _list_all_indexes(self):
        if not self._store.is_created:
            return
        yield '_id_', {'key': [('_id', 1)]}
        yield from self._store.indexes.items()

    def list_indexes(self, session=None):
        _enroll_session(session, self._store)
        for name, information in self._list_all_indexes():
            yield dict(information, key=dict(information['key']), name=name, v=2)

    def index_information(self, session=None):
        _enroll_session(session, self._store)
        return {name: dict(index, v=2) for name, index in self._list_all_indexes()}

    def _has_2dsphere_index_on(self, field: str) -> bool:
        for _, info in self._store.indexes.items():
            for key_field, direction in info.get('key', []):
                if key_field == field and direction == '2dsphere':
                    return True
        return False

    def distinct(self, key, filter=None, session=None, comment=None, hint=None):
        _enroll_session(session, self._store)
        return self.find(filter, comment=comment, hint=hint).distinct(key)

    def aggregate(self, pipeline, session=None, **unused_kwargs):
        in_collection = list(self.find())
        return aggregate.process_pipeline(in_collection, self.database, pipeline, session)

    def with_options(
        self, codec_options=None, read_preference=None, write_concern=None, read_concern=None
    ):
        has_changes = False
        for key, options in _WITH_OPTIONS_KWARGS.items():
            value = locals()[key]
            if value is None or value == getattr(self, '_' + key):
                continue
            has_changes = True
            for attr in options.attrs:
                if not hasattr(value, attr):
                    raise TypeError(f'{key} must be an instance of {options.typename}')

        mongomock_codec_options.is_supported(codec_options)
        if codec_options != self.codec_options:
            has_changes = True

        if not has_changes:
            return self

        return Collection(
            self.database,
            self.name,
            write_concern=write_concern or self._write_concern,
            read_concern=read_concern or self._read_concern,
            read_preference=read_preference or self._read_preference,
            codec_options=codec_options or self._codec_options,
            _db_store=self._db_store,
        )

    def rename(self, new_name, session=None, **kwargs):
        _enroll_session(session, self._store)
        return self.database.rename_collection(self.name, new_name, **kwargs)

    def bulk_write(
        self, requests, ordered=True, bypass_document_validation=False, session=None, comment=None
    ):
        if comment:
            warnings.warn('comment is ignored on mongomock-ng.', stacklevel=2)

        if bypass_document_validation:
            raise NotImplementedError(
                'Skipping document validation is a valid MongoDB operation;'
                ' however Mongomock-ng does not support it yet.'
            )
        _enroll_session(session, self._store)
        bulk = BulkOperationBuilder(self, ordered=ordered)
        for operation in requests:
            operation._add_to_bulk(bulk)
        return BulkWriteResult(bulk.execute(), True)

    def find_raw_batches(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        no_cursor_timeout=False,
        cursor_type=None,
        sort=None,
        allow_partial_results=False,
        oplog_replay=False,
        modifiers=None,
        batch_size=0,
        manipulate=True,
        collation=None,
        hint=None,
        max_scan=None,
        max_time_ms=None,
        max=None,
        min=None,
        return_key=False,
        how_record_id=False,
        snapshot=False,
        comment=None,
        allow_disk_use=False,
    ):
        raise NotImplementedError('find_raw_batches method is not implemented in mongomock-ng yet')

    def aggregate_raw_batches(self, pipeline, **kwargs):
        raise NotImplementedError(
            'aggregate_raw_batches method is not implemented in mongomock-ng yet'
        )

    # Deprecated methods — available with DeprecationWarning for pymongo 4.x compat.

    def remove(self, filter=None, **kwargs):
        warnings.warn(
            'Collection.remove() is deprecated. Use delete_one() or delete_many() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        if filter is None:
            return self.delete_many({}, **kwargs)
        return self.delete_many(filter, **kwargs)

    def save(self, to_save, **kwargs):
        warnings.warn(
            'Collection.save() is deprecated. Use insert_one() or replace_one() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        if not isinstance(to_save, dict):
            raise TypeError(f'cannot save {type(to_save)} object')
        if '_id' in to_save:
            result = self.replace_one({'_id': to_save['_id']}, to_save, upsert=True, **kwargs)
            return result.upserted_id or to_save['_id']
        else:
            result = self.insert_one(to_save, **kwargs)
            return result.inserted_id

    def count(self, filter=None, **kwargs):
        warnings.warn(
            'Collection.count() is deprecated. Use count_documents() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        return self.count_documents(filter or {}, **kwargs)

    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        warnings.warn(
            'Collection.find_and_modify() is deprecated. Use find_one_and_update() or '
            'find_one_and_replace() or find_one_and_delete() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        if update is not None:
            return self.find_one_and_update(query or {}, update, upsert=upsert, **kwargs)
        return self.find_one_and_replace(query or {}, upsert=upsert, **kwargs)

    def update(self, filter, update, **kwargs):
        warnings.warn(
            'Collection.update() is deprecated. Use update_one() or update_many() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        multi = kwargs.pop('multi', True)
        if multi:
            return self.update_many(filter, update, **kwargs)
        return self.update_one(filter, update, **kwargs)

    def insert(self, doc_or_docs, **kwargs):
        warnings.warn(
            'Collection.insert() is deprecated. Use insert_one() or insert_many() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        if isinstance(doc_or_docs, list):
            return self.insert_many(doc_or_docs, **kwargs)
        return self.insert_one(doc_or_docs, **kwargs)

    def ensure_index(self, key_or_list, **kwargs):
        warnings.warn(
            'Collection.ensure_index() is deprecated. Use create_index() instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        return self.create_index(key_or_list, **kwargs)


class Cursor:
    def __init__(
        self,
        collection,
        spec=None,
        sort=None,
        projection=None,
        skip=0,
        limit=0,
        collation=None,
        no_cursor_timeout=False,
        batch_size=0,
        session=None,
    ):
        super().__init__()
        self.collection = collection
        spec = helpers.patch_datetime_awareness_in_document(spec)
        self._spec = spec
        self._sort = sort
        self._projection = projection
        self._skip = skip
        self._factory_last_generated_results = None
        self._results = None
        self._factory = functools.partial(collection._get_dataset, spec, sort, projection, dict)
        # pymongo limit defaults to 0, returning everything
        self._limit = limit if limit != 0 else None
        self._collation = collation
        self.session = session
        self.rewind()

    def _compute_results(self, with_limit_and_skip=False):
        # Recompute the result only if the query has changed
        if not self._results or self._factory_last_generated_results != self._factory:
            if self.collection.codec_options.tz_aware:
                results = [
                    helpers.make_datetime_timezone_aware_in_document(x) for x in self._factory()
                ]
            else:
                results = list(self._factory())
            self._factory_last_generated_results = self._factory
            self._results = results
        if with_limit_and_skip:
            results = self._results[self._skip :]
            if self._limit:
                results = results[: abs(self._limit)]
        else:
            results = self._results
        return results

    def __iter__(self):
        return self

    def clone(self):
        cursor = Cursor(
            self.collection, self._spec, self._sort, self._projection, self._skip, self._limit
        )
        cursor._factory = self._factory
        return cursor

    def __next__(self):
        try:
            doc = self._compute_results(with_limit_and_skip=True)[self._emitted]
            self._emitted += 1
            return doc
        except IndexError as err:
            raise StopIteration from err

    next = __next__

    def rewind(self):
        self._emitted = 0

    def sort(self, key_or_list, direction=None):
        sort = helpers.create_index_list(key_or_list, direction)
        if not sort:
            raise ValueError('key_or_list must not be the empty list')
        self._sort = sort
        self._factory = functools.partial(
            self.collection._get_dataset, self._spec, self._sort, self._projection, dict
        )
        return self

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

    def hint(self, unused_hint):
        if self._emitted:
            raise InvalidOperation('cannot set options after executing query')
        # TODO(pascal): Once we implement $text indexes and queries, raise an
        # exception if hint is used on a $text query.
        # https://docs.mongodb.com/manual/reference/method/cursor.hint/#behavior
        return self

    def distinct(self, key, session=None):
        _enroll_session(session, self.collection._store)
        if not isinstance(key, str):
            raise TypeError('cursor.distinct key must be a string')
        unique = set()
        for x in self._compute_results():
            for values in filtering.iter_key_candidates(key, x):
                if values == NOTHING:
                    continue
                if not isinstance(values, (tuple, list)):
                    values = [values]
                for value in values:
                    if isinstance(value, dict):
                        unique.add(helpers.hashdict(value))
                    else:
                        unique.add(value)
        return [dict(v) if isinstance(v, helpers.hashdict) else v for v in unique]

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError('Cursor instances do not support slice steps')

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError('Cursor instances do not supportnegative indices')
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError(
                        f'stop index must be greater than start index for slice {index!r}'
                    )
                if limit == 0:
                    self.__empty = True
            else:
                limit = 0

            self._skip = skip
            self._limit = limit
            return self
        if not isinstance(index, int):
            raise TypeError(f"index '{index}' cannot be applied to Cursor instances")
        if index < 0:
            raise IndexError('Cursor instances do not support negativeindices')
        return self._compute_results(with_limit_and_skip=True)[index]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def alive(self):
        return self._emitted != len(self._compute_results(with_limit_and_skip=False))

    def collation(self, collation: Union['Collation', Mapping[str, Any]]) -> 'Cursor':
        """Adds a :class:`~pymongo.collation.Collation` to this query.

        Raises :exc:`TypeError` if `collation` is not an instance of
        :class:`~pymongo.collation.Collation` or a ``dict``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. Only the last collation applied to this cursor has
        any effect.
        :param collation: An instance of :class:`~pymongo.collation.Collation`.
        """
        self._collation = validate_collation_or_none(collation)
        return self

    def max_time_ms(self, max_time_ms):
        if max_time_ms is not None and not isinstance(max_time_ms, int):
            raise TypeError('max_time_ms must be an integer or None')
        # Currently the value is ignored as mongomock-ng never times out.
        return self

    def allow_disk_use(self, allow_disk_use=False):
        if allow_disk_use is not None and not isinstance(allow_disk_use, bool):
            raise TypeError('allow_disk_use must be a bool')
        return self

    def explain(self):
        from mongomock_ng import SERVER_VERSION

        results_limit = self._compute_results(with_limit_and_skip=True)
        results_all = self._compute_results(with_limit_and_skip=False)
        n_returned = len(results_limit)
        n_examined = len(results_all)
        namespace = f'{self.collection.database.name}.{self.collection.name}'
        parsed = dict(self._spec or {})
        return {
            'queryPlanner': {
                'plannerVersion': 1,
                'namespace': namespace,
                'indexFilterSet': False,
                'parsedQuery': parsed,
                'winningPlan': {
                    'stage': 'COLLSCAN',
                    'filter': parsed,
                    'direction': 'forward',
                },
                'rejectedPlans': [],
            },
            'executionStats': {
                'executionSuccess': True,
                'nReturned': n_returned,
                'executionTimeMillis': 0,
                'totalKeysExamined': 0,
                'totalDocsExamined': n_examined,
                'executionStages': {
                    'stage': 'COLLSCAN',
                    'nReturned': n_returned,
                    'executionTimeMillisEstimate': 0,
                    'works': max(n_returned, 1),
                    'advanced': n_returned,
                    'needTime': 0,
                    'needFetch': 0,
                    'isEOF': 1,
                    'docsExamined': n_examined,
                    'keysExamined': 0,
                },
            },
            'serverInfo': {
                'host': self.collection.database.client.address[0],
                'port': self.collection.database.client.address[1],
                'version': SERVER_VERSION,
                'gitVersion': 'mock',
            },
            'ok': 1.0,
        }


_ARRAY_FILTER_PATTERN = re.compile(r'^\$\[(\w+)\]$')


def _parse_array_filter_id(part):
    m = _ARRAY_FILTER_PATTERN.match(part)
    return m.group(1) if m else None


def _strip_filter_prefix(filter_dict, filter_id):
    result = {}
    for k, v in filter_dict.items():
        if k.startswith(f'{filter_id}.'):
            result[k[len(filter_id) + 1 :]] = v
        elif k == filter_id:
            result.update(v if isinstance(v, Mapping) else {k: v})
        elif k in ('$and', '$or'):
            result[k] = [_strip_filter_prefix(sub, filter_id) for sub in v]
        else:
            result[k] = v
    return result


def _lookup_array_filter(array_filters, filter_id, path):
    for af in array_filters:
        for key in af:
            if key == filter_id or key.startswith(f'{filter_id}.'):
                return af
        if '$and' in af:
            for sub in af['$and']:
                result = _lookup_array_filter([sub], filter_id, path)
                if result is not None:
                    return af
        if '$or' in af:
            for sub in af['$or']:
                result = _lookup_array_filter([sub], filter_id, path)
                if result is not None:
                    return af
    raise WriteError(f"No array filter found for identifier '{filter_id}' in path '{path}'")


def _array_filter_applies(filter_spec, filter_id, item):
    if isinstance(item, Mapping):
        stripped = {}
        for k, v in filter_spec.items():
            if k.startswith(f'{filter_id}.'):
                stripped[k[len(filter_id) + 1 :]] = v
            elif k == filter_id:
                stripped.update(v if isinstance(v, dict) else {k: v})
            elif k in ('$and', '$or'):
                stripped[k] = [_strip_filter_prefix(sub, filter_id) for sub in v]
            else:
                stripped[k] = v
        return filter_applies(stripped, item) if stripped else True
    try:
        return filter_applies({filter_id: filter_spec[filter_id]}, {filter_id: item})
    except (TypeError, OperationFailure):
        return False


def _set_updater(doc, field_name, value, codec_options=None):
    if isinstance(value, (tuple, list)):
        value = _clone_document(value)
    if BSON:
        # bson validation
        check_keys = False
        if (not check_keys and '\0' in field_name) or field_name.startswith('$'):
            raise InvalidDocument(
                f'Field name cannot contain the null character and top-level field name '
                f'cannot start with "$" (found: {field_name})'
            )
        _bson_encode({field_name: value}, check_keys=check_keys, codec_options=codec_options)
    if isinstance(doc, MutableMapping):
        doc[field_name] = value
    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        len_diff = field_index - (len(doc) - 1)
        if len_diff > 0:
            doc += [None] * len_diff
        doc[field_index] = value


def _unset_updater(doc, field_name, value, codec_options=None):
    if isinstance(doc, MutableMapping):
        doc.pop(field_name, None)


def _inc_updater(doc, field_name, value, codec_options=None):
    if isinstance(doc, MutableMapping):
        doc[field_name] = doc.get(field_name, 0) + value

    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        try:
            doc[field_index] += value
        except IndexError:
            len_diff = field_index - (len(doc) - 1)
            doc += [None] * len_diff
            doc[field_index] = value


def _max_updater(doc, field_name, value, codec_options=None):
    if isinstance(doc, MutableMapping):
        doc[field_name] = max(doc.get(field_name, value), value)


def _min_updater(doc, field_name, value, codec_options=None):
    if isinstance(doc, MutableMapping):
        doc[field_name] = min(doc.get(field_name, value), value)


def _pop_updater(doc, field_name, value, codec_options=None):
    if value not in {1, -1}:
        raise WriteError('$pop expects 1 or -1, found: ' + str(value))

    if isinstance(doc, MutableMapping):
        if isinstance(doc[field_name], (tuple, list)):
            doc[field_name] = list(doc[field_name])
            _pop_from_list(doc[field_name], value)
            return
        raise WriteError('Path contains element of non-array type')

    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        if field_index >= len(doc):
            return
        _pop_from_list(doc[field_index], value)


def _pop_from_list(list_instance, mongo_pop_value, codec_options=None):
    if not list_instance:
        return

    if mongo_pop_value == 1:
        list_instance.pop()
    elif mongo_pop_value == -1:
        list_instance.pop(0)


def _bit_updater(doc, field_name, value, codec_options=None):
    doc_value = doc.get(field_name, 0)
    if 'and' in value:
        doc[field_name] = doc_value & value['and']
    if 'or' in value:
        doc[field_name] = doc_value | value['or']
    if 'xor' in value:
        doc[field_name] = doc_value ^ value['xor']


def _current_date_updater(doc, field_name, value, codec_options=None):
    if isinstance(doc, MutableMapping):
        if value == {'$type': 'timestamp'}:
            # TODO(juannyg): get_current_timestamp should also be using helpers utcnow,
            # as it currently using time.time internally
            doc[field_name] = helpers.get_current_timestamp()
        else:
            doc[field_name] = helpers.utcnow()


_updaters = {
    '$bit': _bit_updater,
    '$set': _set_updater,
    '$unset': _unset_updater,
    '$inc': _inc_updater,
    '$max': _max_updater,
    '$min': _min_updater,
    '$pop': _pop_updater,
}
