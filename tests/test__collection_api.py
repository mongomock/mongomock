import collections
import copy
import platform
import random
import re
import sys
import time
import warnings
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from datetime import tzinfo
from unittest import mock
from unittest import skipIf
from unittest import TestCase

from packaging import version

import mongomock_ng as mongomock
from mongomock_ng import BulkWriteError
from mongomock_ng import helpers
from mongomock_ng import WriteError
from tests.diff import diff


try:
    import pymongo
    from bson import codec_options
    from bson import DBRef
    from bson import decimal128
    from bson import ObjectId
    from bson import Regex
    from bson import Timestamp
    from bson import tz_util
    from bson.errors import InvalidDocument
    from pymongo import ReturnDocument
    from pymongo.collation import Collation
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import ReadPreference
    from pymongo.write_concern import WriteConcern
except ImportError:
    from mongomock_ng import ObjectId
    from mongomock_ng.collection import Collation
    from mongomock_ng.collection import ReturnDocument
    from mongomock_ng.read_concern import ReadConcern
    from mongomock_ng.write_concern import WriteConcern
    from tests.utils import DBRef


warnings.simplefilter('ignore', DeprecationWarning)
IS_PYPY = platform.python_implementation() != 'CPython'
SERVER_VERSION = version.parse(mongomock.SERVER_VERSION)


class UTCPlus2(tzinfo):
    def fromutc(self, dt):
        return dt + self.utcoffset(dt)

    def tzname(self, dt):
        return '<dummy UTC+2>'

    def utcoffset(self, dt):
        return timedelta(hours=2)

    def dst(self, dt):
        return timedelta()


class CollectionAPITest(TestCase):
    def setUp(self):
        super().setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test__get_subcollections(self):
        self.db.create_collection('a.b')
        self.assertEqual(self.db.a.b.full_name, 'somedb.a.b')
        self.assertEqual(self.db.a.b.name, 'a.b')

        self.assertEqual(set(self.db.list_collection_names()), {'a.b'})

    def test__get_subcollections_by_attribute_underscore(self):
        with self.assertRaises(AttributeError) as err_context:
            self.db.a._b  # noqa: B018

        self.assertIn("Collection has no attribute '_b'", str(err_context.exception))

        # No problem accessing it through __get_item__.
        self.db.a['_b'].insert_one({'a': 1})
        self.assertEqual(1, self.db.a['_b'].find_one().get('a'))

    def test__get_sibling_collection(self):
        self.assertEqual(self.db.a.database.b.full_name, 'somedb.b')
        self.assertEqual(self.db.a.database.b.name, 'b')

    def test__get_collection_read_concern_option(self):
        """Ensure read_concern option isn't rejected."""
        self.assertIsNotNone(self.db.get_collection('new_collection', read_concern=None))

    def test__get_collection_full_name(self):
        self.assertEqual(self.db.coll.name, 'coll')
        self.assertEqual(self.db.coll.full_name, 'somedb.coll')

    def test__cursor_collection(self):
        self.assertIs(self.db.a.find().collection, self.db.a)

    def test__cursor_alive(self):
        self.db.collection.insert_one({'foo': 'bar'})
        cursor = self.db.collection.find()
        self.assertTrue(cursor.alive)
        next(cursor)
        self.assertFalse(cursor.alive)

    def test__cursor_explain(self):
        self.db.collection.insert_one({'x': 1})
        self.db.collection.insert_one({'x': 2})
        cursor = self.db.collection.find({'x': {'$gt': 1}})
        result = cursor.explain()
        self.assertIn('queryPlanner', result)
        self.assertIn('executionStats', result)
        self.assertIn('serverInfo', result)
        self.assertEqual(result['ok'], 1.0)
        self.assertEqual(result['executionStats']['nReturned'], 1)

    def test__cursor_explain_empty(self):
        self.db.collection.insert_one({'x': 1})
        cursor = self.db.collection.find({'x': {'$gt': 999}})
        result = cursor.explain()
        self.assertEqual(result['ok'], 1.0)
        self.assertEqual(result['executionStats']['nReturned'], 0)
        self.assertEqual(result['executionStats']['totalDocsExamined'], 0)

    def test__cursor_explain_no_filter(self):
        self.db.collection.insert_one({'x': 1})
        cursor = self.db.collection.find()
        result = cursor.explain()
        self.assertEqual(result['ok'], 1.0)
        self.assertEqual(result['executionStats']['nReturned'], 1)
        self.assertEqual(result['queryPlanner']['parsedQuery'], {})

    def test__cursor_explain_after_exhaustion(self):
        self.db.collection.insert_one({'x': 1})
        cursor = self.db.collection.find()
        list(cursor)
        self.assertFalse(cursor.alive)
        result = cursor.explain()
        self.assertEqual(result['ok'], 1.0)
        self.assertEqual(result['executionStats']['nReturned'], 1)

    def test__cursor_collation_directly_in_find(self):
        self.db.collection.insert_one({'foo': 'bar'})
        cursor = self.db.collection.find(collation={'locale': 'fr'})
        self.assertEqual('fr', cursor._collation.get('locale'))

    def test__cursor__collation(self):
        cursor = self.db.collection.find()
        collation = Collation(locale='en_US', strength=1)
        cursor.collation(collation)
        self.assertEqual(cursor._collation.get('strength'), 1)

    def test__drop_collection(self):
        self.db.create_collection('a')
        self.db.create_collection('b')
        self.db.create_collection('c')
        self.db.drop_collection('b')
        self.db.drop_collection(self.db.c)
        self.assertEqual(set(self.db.list_collection_names()), {'a'})

        col = self.db.a
        r = col.insert_one({'aa': 'bb'}).inserted_id
        self.assertEqual(col.count_documents({'_id': r}), 1)

        self.db.drop_collection('a')
        self.assertEqual(col.count_documents({'_id': r}), 0)

        col = self.db.a
        r = col.insert_one({'aa': 'bb'}).inserted_id
        self.assertEqual(col.count_documents({'_id': r}), 1)

        self.assertIsInstance(col._store._documents, collections.OrderedDict)
        self.db.drop_collection(col)
        self.assertIsInstance(col._store._documents, collections.OrderedDict)
        self.assertEqual(col.count_documents({'_id': r}), 0)

    def test__drop_collection_indexes(self):
        col = self.db.a
        col.create_index('simple')
        col.create_index([('value', 1)], unique=True)
        col.create_index([('sparsed', 1)], unique=True, sparse=True)

        self.db.drop_collection(col)

        # Make sure indexes' rules no longer apply
        col.insert_one({'value': 'not_unique_but_ok', 'sparsed': 'not_unique_but_ok'})
        col.insert_one({'value': 'not_unique_but_ok'})
        col.insert_one({'sparsed': 'not_unique_but_ok'})
        self.assertEqual(col.count_documents({}), 3)

    def test__drop_n_recreate_collection(self):
        col_a = self.db.create_collection('a')
        col_a2 = self.db.a
        col_a.insert_one({'foo': 'bar'})
        self.assertEqual(col_a.count_documents({}), 1)
        self.assertEqual(col_a2.count_documents({}), 1)
        self.assertEqual(self.db.a.count_documents({}), 1)

        self.db.drop_collection('a')
        self.assertEqual(col_a.count_documents({}), 0)
        self.assertEqual(col_a2.count_documents({}), 0)
        self.assertEqual(self.db.a.count_documents({}), 0)

        col_a2.insert_one({'foo2': 'bar2'})
        self.assertEqual(col_a.count_documents({}), 1)
        self.assertEqual(col_a2.count_documents({}), 1)
        self.assertEqual(self.db.a.count_documents({}), 1)

    def test__cursor_hint(self):
        self.db.collection.insert_one({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()

        self.assertEqual(cursor, cursor.hint(None))

        cursor.hint('unknownIndex')
        self.assertEqual([{'f2': 'v'}], [d['f1'] for d in cursor])

        with self.assertRaises(mongomock.InvalidOperation):
            cursor.hint(None)

    def test__distinct_nested_field(self):
        self.db.collection.insert_one({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1.f2'), ['v'])

    def test__distinct_array_field(self):
        self.db.collection.insert_many([{'f1': ['v1', 'v2', 'v1']}, {'f1': ['v2', 'v3']}])
        cursor = self.db.collection.find()
        self.assertEqual(set(cursor.distinct('f1')), {'v1', 'v2', 'v3'})

    def test__distinct_array_nested_field(self):
        self.db.collection.insert_one({'f1': [{'f2': 'v'}, {'f2': 'w'}]})
        cursor = self.db.collection.find()
        self.assertEqual(set(cursor.distinct('f1.f2')), {'v', 'w'})

    def test__distinct_document_field(self):
        self.db.collection.insert_many(
            [{'f1': {'f2': 'v2', 'f3': 'v3'}}, {'f1': {'f2': 'v2', 'f3': 'v3'}}]
        )
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1'), [{'f2': 'v2', 'f3': 'v3'}])

    def test__distinct_array_field_with_dicts(self):
        self.db.collection.insert_many(
            [
                {'f1': [{'f2': 'v2'}, {'f3': 'v3'}]},
                {'f1': [{'f3': 'v3'}, {'f4': 'v4'}]},
            ]
        )
        cursor = self.db.collection.find()
        self.assertCountEqual(cursor.distinct('f1'), [{'f2': 'v2'}, {'f3': 'v3'}, {'f4': 'v4'}])

    def test__distinct_filter_field(self):
        self.db.collection.insert_many(
            [{'f1': 'v1', 'k1': 'v1'}, {'f1': 'v2', 'k1': 'v1'}, {'f1': 'v3', 'k1': 'v2'}]
        )
        self.assertEqual(set(self.db.collection.distinct('f1', {'k1': 'v1'})), {'v1', 'v2'})

    def test__distinct_error(self):
        with self.assertRaises(TypeError):
            self.db.collection.distinct({'f1': 1})

    def test__cursor_clone(self):
        self.db.collection.insert_many([{'a': 'b'}, {'b': 'c'}, {'c': 'd'}])
        cursor1 = self.db.collection.find()
        iterator1 = iter(cursor1)
        first_item = next(iterator1)
        cursor2 = cursor1.clone()
        iterator2 = iter(cursor2)
        self.assertEqual(next(iterator2), first_item)
        for item in iterator1:
            self.assertEqual(item, next(iterator2))

        with self.assertRaises(StopIteration):
            next(iterator2)

    def test__cursor_clone_keep_limit_skip(self):
        self.db.collection.insert_many([{'a': 'b'}, {'b': 'c'}, {'c': 'd'}])
        cursor1 = self.db.collection.find()[1:2]
        cursor2 = cursor1.clone()
        result1 = list(cursor1)
        result2 = list(cursor2)
        self.assertEqual(result1, result2)

        cursor3 = self.db.collection.find(skip=1, limit=1)
        cursor4 = cursor3.clone()
        result3 = list(cursor3)
        result4 = list(cursor4)
        self.assertEqual(result3, result4)

    def test_cursor_returns_document_copies(self):
        obj = {'a': 1, 'b': 2}
        self.db.collection.insert_one(obj)
        fetched_obj = self.db.collection.find_one({'a': 1})
        self.assertEqual(fetched_obj, obj)
        fetched_obj['b'] = 3
        refetched_obj = self.db.collection.find_one({'a': 1})
        self.assertNotEqual(fetched_obj, refetched_obj)

    def test_cursor_with_projection_returns_value_copies(self):
        self.db.collection.insert_one({'a': ['b']})
        fetched_list = self.db.collection.find_one(projection=['a'])['a']
        self.assertEqual(fetched_list, ['b'])
        fetched_list.append('c')
        refetched_list = self.db.collection.find_one(projection=['a'])['a']
        self.assertEqual(refetched_list, ['b'])

    def test__getting_collection_via_getattr(self):
        col1 = self.db.some_collection_here
        col2 = self.db.some_collection_here
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db['some_collection_here'])
        self.assertIsInstance(col1, mongomock.Collection)

    def test__save_class_deriving_from_dict(self):
        # See https://github.com/vmalloc/mongomock_ng/issues/52
        class Document(dict):
            def __init__(self, collection):
                self.collection = collection
                super().__init__()
                self.save()

            def save(self):
                self.collection.insert_one(self)

        doc = Document(self.db.collection)
        self.assertIn('_id', doc)
        self.assertNotIn('collection', doc)

    def test__getting_collection_via_getitem(self):
        col1 = self.db['some_collection_here']
        col2 = self.db['some_collection_here']
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db.some_collection_here)
        self.assertIsInstance(col1, mongomock.Collection)

    def test__cannot_insert_non_string_keys(self):
        for key in [2, 2.0, True, object()]:
            with self.assertRaises(ValueError):
                self.db.col1.insert_one({key: 'value'})

    def assert_document_count(self, count=1):
        self.assertEqual(len(self.db.collection._store), count)

    def assert_document_stored(self, doc_id, expected=None):
        self.assertIn(doc_id, self.db.collection._store)
        if expected is not None:
            expected = expected.copy()
            expected['_id'] = doc_id
            doc = self.db.collection._store[doc_id]
            self.assertDictEqual(doc, expected)

    def assert_documents(self, documents, ignore_ids=True):
        projection = {'_id': False} if ignore_ids else None
        self.assertListEqual(list(self.db.collection.find(projection=projection)), documents)

    def test__insert_one(self):
        document = {'a': 1}
        result = self.db.collection.insert_one(document)
        self.assert_document_stored(result.inserted_id, document)

    def test__insert_one_type_error(self):
        with self.assertRaises(TypeError):
            self.db.collection.insert_one([{'a': 1}])
        self.assert_document_count(0)

        with self.assertRaises(TypeError):
            self.db.collection.insert_one('a')
        self.assert_document_count(0)

    def test__insert_many(self):
        documents = [{'a': 1}, {'b': 2}]
        result = self.db.collection.insert_many(documents)
        self.assertIsInstance(result.inserted_ids, list)

        for i, doc_id in enumerate(result.inserted_ids):
            self.assert_document_stored(doc_id, documents[i])

    def test__insert_many_with_generator(self):
        documents = [{'a': 1}, {'b': 2}]
        documents_generator = (doc for doc in [{'a': 1}, {'b': 2}])
        result = self.db.collection.insert_many(documents_generator)
        self.assertIsInstance(result.inserted_ids, list)
        self.assertEqual(2, len(result.inserted_ids), result)

        for i, doc_id in enumerate(result.inserted_ids):
            self.assert_document_stored(doc_id, documents[i])

    def test__insert_many_type_error(self):
        with self.assertRaises(TypeError):
            self.db.collection.insert_many({'a': 1})
        self.assert_document_count(0)

        with self.assertRaises(TypeError):
            self.db.collection.insert_many('a')
        self.assert_document_count(0)

        with self.assertRaises(TypeError):
            self.db.collection.insert_many(5)
        self.assert_document_count(0)

        with self.assertRaises(TypeError):
            self.db.collection.insert_many([])
        self.assert_document_count(0)

    def test__insert_many_type_error_do_not_insert(self):
        with self.assertRaises(TypeError):
            self.db.collection.insert_many([{'a': 1}, 'a'])
        self.assert_document_count(0)

    def test__insert_many_write_errors(self):
        self.db.collection.insert_one({'_id': 'a'})

        # Insert many, but the first one is a duplicate.
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            self.db.collection.insert_many([{'_id': 'a', 'culprit': True}, {'_id': 'b'}])
        error_details = err_context.exception.details
        self.assertEqual({'nInserted', 'writeErrors'}, set(error_details.keys()))
        self.assertEqual(0, error_details['nInserted'])
        self.assertEqual(
            [{'_id': 'a', 'culprit': True}], [e['op'] for e in error_details['writeErrors']]
        )

        # Insert many, and only the second one is a duplicate.
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            self.db.collection.insert_many([{'_id': 'c'}, {'_id': 'a', 'culprit': True}])
        error_details = err_context.exception.details
        self.assertEqual({'nInserted', 'writeErrors'}, set(error_details.keys()))
        self.assertEqual(1, error_details['nInserted'])
        self.assertEqual(
            [{'_id': 'a', 'culprit': True}], [e['op'] for e in error_details['writeErrors']]
        )

        # Insert many, with ordered=False.
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            self.db.collection.insert_many(
                [
                    {'_id': 'a', 'culprit': True},
                    {'_id': 'b'},
                    {'_id': 'c', 'culprit': True},
                ],
                ordered=False,
            )
        error_details = err_context.exception.details
        self.assertEqual({'nInserted', 'writeErrors'}, set(error_details.keys()))
        self.assertEqual([0, 2], sorted(e['index'] for e in error_details['writeErrors']))
        self.assertEqual(1, error_details['nInserted'])
        self.assertEqual({'a', 'b', 'c'}, {doc['_id'] for doc in self.db.collection.find()})

    def test__count_documents(self):
        self.db.collection.insert_many(
            [{'a': 1, 's': 0}, {'a': 2, 's': 0}, {'_id': 'unique', 'a': 3, 's': 1}]
        )
        self.assertEqual(3, self.db.collection.count_documents({}))
        self.assertEqual(2, self.db.collection.count_documents({'s': 0}))
        self.assertEqual(1, self.db.collection.count_documents({'s': 1}))

        self.assertEqual(2, self.db.collection.count_documents({}, skip=1))
        self.assertEqual(1, self.db.collection.count_documents({}, skip=1, limit=1))

        error_kwargs = [
            {'unknownKwarg': None},
            {'limit': 'one'},
            {'limit': -1},
            {'limit': 0},
        ]
        for error_kwarg in error_kwargs:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.collection.count_documents({}, **error_kwarg)

        with self.assertRaises(NotImplementedError):
            self.db.collection.count_documents({}, collation={'locale': 'fr'})

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.count_documents('unique')

    def test__find_returns_cursors(self):
        collection = self.db.collection
        self.assertEqual(type(collection.find()).__name__, 'Cursor')
        self.assertNotIsInstance(collection.find(), list)
        self.assertNotIsInstance(collection.find(), tuple)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__find_with_collation(self):
        collection = self.db.collection
        collation = Collation('fr')
        cursor = collection.find({}, collation=collation)
        self.assertEqual(cursor._collation, collation)

    def test__find_removed_and_changed_options(self):
        """Test that options that have been removed are rejected."""
        options = [
            {'slave_okay': True},
            {'as_class': dict},
            {'network_timeout': 10},
            {'secondary_acceptable_latency_ms': 10},
            {'max_scan': 10},
            {'snapshot': True},
            {'tailable': True},
            {'await_data': True},
            {'exhaust': True},
            {'fields': {'a': 1}},
            {'timeout': 10},
            {'partial': True},
        ]

        for option in options:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.collection.find({}, **option)

    def test__find_one_and_update_doc_with_zero_ids(self):
        ret = self.db.col_a.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}}, upsert=True, return_document=ReturnDocument.AFTER
        )
        self.assertEqual(ret, {'_id': 0, 'counter': 1})
        ret = self.db.col_a.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}}, upsert=True, return_document=ReturnDocument.AFTER
        )
        self.assertEqual(ret, {'_id': 0, 'counter': 2})

        ret = self.db.col_b.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}}, upsert=True, return_document=ReturnDocument.BEFORE
        )
        self.assertIsNone(ret)
        ret = self.db.col_b.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}}, upsert=True, return_document=ReturnDocument.BEFORE
        )
        self.assertEqual(ret, {'_id': 0, 'counter': 1})

    def test__find_one_and_replace_return_document_after_upsert(self):
        collection = self.db.col
        collection.insert_one({'_id': 123, 'val': 5})
        ret = collection.find_one_and_replace(
            {'val': 1}, {'val': 7}, upsert=True, return_document=ReturnDocument.AFTER
        )
        self.assertTrue(ret)
        self.assertEqual(7, ret['val'])

    def test__find_one_and_delete(self):
        documents = [{'x': 1, 's': 0}, {'x': 2, 's': 1}]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_delete({'x': 3})
        self.assert_documents(documents, ignore_ids=False)
        self.assertIsNone(doc)

        doc = self.db.collection.find_one_and_delete({'x': 2})
        self.assert_documents(documents[:-1], ignore_ids=False)
        self.assertDictEqual(doc, documents[1])

        doc = self.db.collection.find_one_and_delete({'s': 0}, {'_id': False, 'x': True})
        self.assertEqual(doc, {'x': 1})

    def test__find_one_and_replace(self):
        documents = [{'x': 1, 's': 0}, {'x': 1, 's': 1}]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_replace({'s': 3}, {'x': 2, 's': 1})
        self.assert_documents(documents, ignore_ids=False)
        self.assertIsNone(doc)

        doc = self.db.collection.find_one_and_replace({'s': 1}, {'x': 2, 's': 1})
        self.assertDictEqual(doc, documents[1])
        self.assert_document_count(2)

        doc = self.db.collection.find_one_and_replace({'s': 2}, {'x': 3, 's': 0}, upsert=True)
        self.assertIsNone(doc)
        self.assertIsNotNone(self.db.collection.find_one({'x': 3}))
        self.assert_document_count(3)

        replacement = {'x': 4, 's': 1}
        doc = self.db.collection.find_one_and_replace(
            {'s': 1}, replacement, return_document=ReturnDocument.AFTER
        )
        doc.pop('_id')
        self.assertDictEqual(doc, replacement)

    def test__find_one_sorting(self):
        documents = [
            {'x': 1, 's': 0},
            {'x': 1, 's': 1},
            {'x': 4, 's': 1},
            {'x': 1, 's': 2},
            {'x': 1, 's': 3},
            {'x': 9, 's': 1},
        ]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one({'x': 1}, sort={'s': -1})

        self.assertDictEqual(doc, documents[-2])

        doc = self.db.collection.find_one({'x': 1}, sort={'s': 1})

        self.assertDictEqual(doc, documents[0])

    def test__find_one_and_update(self):
        documents = [{'x': 1, 's': 0}, {'x': 1, 's': 1}]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_update({'s': 3}, {'$set': {'x': 2}})
        self.assertIsNone(doc)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_update({'s': 1}, {'$set': {'x': 2}})
        self.assertDictEqual(doc, documents[1])

        doc = self.db.collection.find_one_and_update(
            {'s': 3}, {'$set': {'x': 3, 's': 2}}, upsert=True
        )
        self.assertIsNone(doc)
        self.assertIsNotNone(self.db.collection.find_one({'x': 3}))

        update = {'x': 4, 's': 1}
        doc = self.db.collection.find_one_and_update(
            {'s': 1}, {'$set': update}, return_document=ReturnDocument.AFTER
        )
        doc.pop('_id')
        self.assertDictEqual(doc, update)

    def test__find_in_empty_collection(self):
        self.db.collection.drop()

        # Valid filter.
        self.db.collection.find_one({'a.b': 3})

        # Invalid filter.
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'$or': []})

        # Do not raise when creating the cursor.
        cursor = self.db.collection.find({'$or': []})
        # Only raise when using it.
        with self.assertRaises(mongomock.OperationFailure):
            next(cursor)

    def test__regex_options(self):
        self.db.collection.drop()
        self.db.collection.insert_one({'a': 'TADA'})
        self.db.collection.insert_one({'a': 'TA\nDA'})

        self.assertFalse(self.db.collection.find_one({'a': {'$regex': 'tada'}}))
        self.assertTrue(
            self.db.collection.find_one(
                {
                    'a': {
                        '$regex': re.compile('tada', re.IGNORECASE),
                    }
                }
            )
        )

        self.assertTrue(self.db.collection.find_one({'a': {'$regex': 'tada', '$options': 'i'}}))
        self.assertTrue(self.db.collection.find_one({'a': {'$regex': '^da', '$options': 'im'}}))
        self.assertFalse(self.db.collection.find_one({'a': {'$regex': 'tada', '$options': 'I'}}))
        self.assertTrue(self.db.collection.find_one({'a': {'$regex': 'TADA', '$options': 'z'}}))
        self.assertTrue(
            self.db.collection.find_one(
                {
                    'a': collections.OrderedDict(
                        [
                            ('$regex', re.compile('tada')),
                            ('$options', 'i'),
                        ]
                    )
                }
            )
        )
        self.assertTrue(
            self.db.collection.find_one(
                {
                    'a': collections.OrderedDict(
                        [
                            ('$regex', re.compile('tada', re.IGNORECASE)),
                            ('$options', 'm'),
                        ]
                    )
                }
            )
        )

        # Bad type for $options.
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$regex': 'tada', '$options': re.I}})

        # Bug https://jira.mongodb.org/browse/SERVER-38621
        self.assertTrue(
            self.db.collection.find_one(
                {
                    'a': collections.OrderedDict(
                        [
                            ('$options', 'i'),
                            ('$regex', re.compile('tada')),
                        ]
                    )
                }
            )
        )

    def test__iterate_on_find_and_update(self):
        documents = [{'x': 1, 's': 0}, {'x': 1, 's': 1}, {'x': 1, 's': 2}, {'x': 1, 's': 3}]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        self.assertEqual(self.db.collection.count_documents({'x': 1}), 4)

        # Update the field used by the cursor's filter should not upset the iteration
        for doc in self.db.collection.find({'x': 1}):
            self.db.collection.update_one({'_id': doc['_id']}, {'$set': {'x': 2}})

        self.assertEqual(self.db.collection.count_documents({'x': 1}), 0)
        self.assertEqual(self.db.collection.count_documents({'x': 2}), 4)

    def test__update_interns_lists_and_dicts(self):
        obj = {}
        obj_id = self.db.collection.insert_one(obj).inserted_id
        external_dict = {}
        external_list = []
        self.db.collection.replace_one({'_id': obj_id}, {'d': external_dict, 'l': external_list})
        external_dict['a'] = 'b'
        external_list.append(1)
        self.assertEqual(list(self.db.collection.find()), [{'_id': obj_id, 'd': {}, 'l': []}])

    def test__update_cannot_change__id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.replace_one({'_id': 1}, {'_id': 2, 'b': 2})

    def test__update_empty_id(self):
        self.db.collection.insert_one({'_id': '', 'a': 1})
        self.db.collection.replace_one({'_id': ''}, {'b': 1})
        doc = self.db.collection.find_one({'_id': ''})
        self.assertEqual(1, doc['b'])

    def test__update_one(self):
        insert_result = self.db.collection.insert_one({'a': 1})
        update_result = self.db.collection.update_one(filter={'a': 1}, update={'$set': {'a': 2}})
        self.assertEqual(update_result.matched_count, 1)
        self.assertEqual(update_result.modified_count, 1)
        self.assertIsNone(update_result.upserted_id)
        doc = self.db.collection.find_one({'a': 2})
        self.assertEqual(insert_result.inserted_id, doc['_id'])
        self.assertEqual(doc['a'], 2)

    def test__update_id(self):
        self.db.collection.insert_one({'a': 1})
        with self.assertRaises(mongomock.WriteError):
            self.db.collection.update_one({'a': 1}, {'$set': {'a': 2, '_id': 42}})
        self.assertEqual(1, self.db.collection.find_one({})['a'])

    def test__update_one_upsert(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_one(
            filter={'a': 1}, update={'$set': {'a': 1}}, upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'a': 1})

    def test__update_one_upsert_dots(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_one(
            filter={'a.b': 1}, update={'$set': {'c': 2}}, upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'a': {'b': 1}, 'c': 2})

    def test__update_one_upsert_match_subdocuments(self):
        update_result = self.db.collection.update_one(
            filter={'b.c.': 1, 'b.d': 3}, update={'$set': {'a': 1}}, upsert=True
        )

        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(
            update_result.upserted_id, {'a': 1, 'b': {'c': {'': 1}, 'd': 3}}
        )

    def test__update_one_upsert_operators(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_one(
            filter={'a.b': {'$eq': 1}, 'e.f': {'$gt': 3}, 'd': {}},
            update={'$set': {'c': 2}},
            upsert=True,
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'c': 2, 'd': {}, 'a': {'b': 1}})

    def test__update_one_unset_position(self):
        insert_result = self.db.collection.insert_one({'a': 1, 'b': [{'c': 2, 'd': 3}]})
        update_result = self.db.collection.update_one(
            filter={'a': 1, 'b': {'$elemMatch': {'c': 2, 'd': 3}}}, update={'$unset': {'b.$.c': ''}}
        )
        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(update_result.matched_count, 1)
        self.assert_document_stored(insert_result.inserted_id, {'a': 1, 'b': [{'d': 3}]})

    def test__update_one_no_change(self):
        self.db.collection.insert_one({'a': 1})
        update_result = self.db.collection.update_one(filter={'a': 1}, update={'$set': {'a': 1}})
        self.assertEqual(update_result.matched_count, 1)
        self.assertEqual(update_result.modified_count, 0)

    def test__rename_one_foo_to_bar(self):
        input_ = {'_id': 1, 'foo': 'bar'}
        expected = {'_id': 1, 'bar': 'bar'}
        insert_result = self.db.collection.insert_one(input_)
        query = {'_id': 1}
        update = {'$rename': {'foo': 'bar'}}
        update_result = self.db.collection.update_one(query, update=update)

        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(update_result.matched_count, 1)
        self.assert_document_stored(insert_result.inserted_id, expected)

    def test__rename_missing_field(self):
        input_ = {'_id': 1, 'foo': 'bar'}
        insert_result = self.db.collection.insert_one(input_)
        query = {'_id': 1}
        update = {'$rename': {'bar': 'foo'}}
        update_result = self.db.collection.update_one(query, update=update)

        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 1)
        self.assert_document_stored(insert_result.inserted_id, input_)

    def test__rename_with_dots(self):
        input_ = {'_id': 1, 'foo': 'bar'}
        insert_result = self.db.collection.insert_one(input_)
        self.assert_document_stored(insert_result.inserted_id, input_)

        query = {'_id': 1}
        update = {'$rename': {'foo': 'f.o.o'}}
        self.db.collection.update_one(query, update=update)
        doc = self.db.collection.find_one(query)
        self.assertEqual(doc['f']['o']['o'], 'bar')
        self.assertNotIn('foo', doc)

    def test__rename_nested(self):
        input_ = {'_id': 1, 'a': {'b': {'c': 'val'}}}
        self.db.collection.insert_one(input_)

        update = {'$rename': {'a.b.c': 'x.y'}}
        self.db.collection.update_one({'_id': 1}, update=update)
        doc = self.db.collection.find_one({'_id': 1})
        self.assertEqual(doc['x']['y'], 'val')
        self.assertNotIn('c', doc['a']['b'])

    def test__rename_source_missing(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'a': {'x': 1}})
        col.update_one({'_id': 1}, {'$rename': {'a.b.c': 'y'}})
        doc = col.find_one({'_id': 1})
        self.assertEqual(doc['a']['x'], 1)
        self.assertNotIn('y', doc)

    def test__add_to_set_with_each(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'items': [1, 2]})
        col.update_one({'_id': 1}, {'$addToSet': {'items': {'$each': [2, 3, 4]}}})
        doc = col.find_one({'_id': 1})
        self.assertEqual(doc['items'], [1, 2, 3, 4])

    def test__add_to_set_with_each_nested(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'nested': {'items': [1, 2]}})
        col.update_one({'_id': 1}, {'$addToSet': {'nested.items': {'$each': [2, 3, 4]}}})
        doc = col.find_one({'_id': 1})
        self.assertEqual(doc['nested']['items'], [1, 2, 3, 4])

    def test__add_to_set_boolean_distinct(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'flags': [1, 0]})
        col.update_one({'_id': 1}, {'$addToSet': {'flags': True}})
        col.update_one({'_id': 1}, {'$addToSet': {'flags': False}})
        doc = col.find_one({'_id': 1})
        self.assertIn(True, doc['flags'])
        self.assertIn(False, doc['flags'])
        self.assertIn(1, doc['flags'])
        self.assertIn(0, doc['flags'])
        self.assertEqual(len(doc['flags']), 4)

    def test__add_to_set_boolean_with_each(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'vals': [1, 0]})
        col.update_one({'_id': 1}, {'$addToSet': {'vals': {'$each': [True, False, 2]}}})
        doc = col.find_one({'_id': 1})
        self.assertIn(True, doc['vals'])
        self.assertIn(False, doc['vals'])
        self.assertIn(2, doc['vals'])

    def test__add_to_set_each_non_list(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'tags': ['a']})
        col.update_one({'_id': 1}, {'$addToSet': {'tags': {'$each': 'b'}}})
        doc = col.find_one({'_id': 1})
        self.assertIn('b', doc['tags'])
        self.assertEqual(len(doc['tags']), 2)

    def test__add_to_set_each_non_list_nested(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'nested': {'tags': ['a']}})
        col.update_one({'_id': 1}, {'$addToSet': {'nested.tags': {'$each': 'b'}}})
        doc = col.find_one({'_id': 1})
        self.assertIn('b', doc['nested']['tags'])
        self.assertEqual(len(doc['nested']['tags']), 2)

    def test__rename_dest_intermediate_not_mapping(self):
        col = self.db.collection
        col.insert_one({'_id': 1, 'foo': 'bar', 'x': 5})
        col.update_one({'_id': 1}, {'$rename': {'foo': 'x.y'}})
        doc = col.find_one({'_id': 1})
        self.assertEqual(doc['x'], 5)
        self.assertNotIn('y', doc)

    def test__mutable_mapping_filter(self):
        class AttrsDict(collections.abc.MutableMapping):
            def __init__(self, **kw):
                self.__d = dict(kw)

            def __getitem__(self, k):
                return self.__d[k]

            def __setitem__(self, k, v):
                self.__d[k] = v

            def __delitem__(self, k):
                del self.__d[k]

            def __iter__(self):
                return iter(self.__d)

            def __len__(self):
                return len(self.__d)

        col = self.db.collection
        doc = AttrsDict(_id=1, name='foo')
        col.insert_one(doc)
        cursor = col.find({'name': 'foo'})
        result = next(cursor)
        self.assertEqual(result['name'], 'foo')

    def test__mutable_mapping_update(self):
        class AttrsDict(collections.abc.MutableMapping):
            def __init__(self, **kw):
                self.__d = dict(kw)

            def __getitem__(self, k):
                return self.__d[k]

            def __setitem__(self, k, v):
                self.__d[k] = v

            def __delitem__(self, k):
                del self.__d[k]

            def __iter__(self):
                return iter(self.__d)

            def __len__(self):
                return len(self.__d)

        col = self.db.collection
        doc = AttrsDict(_id=1, name='foo')
        col.insert_one(doc)
        col.update_one({'_id': 1}, {'$set': {'name': 'bar'}})
        result = col.find_one({'_id': 1})
        self.assertEqual(result['name'], 'bar')

    def test__update_one_upsert_invalid_filter(self):
        with self.assertRaises(mongomock.WriteError):
            self.db.collection.update_one(
                filter={'a.b': 1, 'a': 3}, update={'$set': {'c': 2}}, upsert=True
            )

    def test__update_one_hint(self):
        self.db.collection.insert_one({'a': 1})
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_one(
                filter={'a': 1},
                update={'$set': {'a': 1}},
                hint='a',
            )

    def test__update_pipeline(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'_id': 1, 'a': 1, 'b': 2},
                {'_id': 2, 'a': 1, 'b': 2},
                {'_id': 3, 'a': 1, 'b': 2},
                {'_id': 4, 'a': 1, 'b': 2},
                {'_id': 5, 'a': 1, 'b': 2, 'c': {'d': 3}},
                {'_id': 6, 'a': 1, 'b': 2, 'c': {'d': 3}},
            ]
        )
        data = (
            (
                1,
                [{'$set': {'c': {'$add': ['$a', '$b']}}}],
                {'_id': 1, 'a': 1, 'b': 2, 'c': 3},
            ),
            (
                2,
                [{'$addFields': {'c': {'$add': ['$a', '$b']}}}],
                {'_id': 2, 'a': 1, 'b': 2, 'c': 3},
            ),
            (
                3,
                [{'$project': {'a': 0}}],
                {'_id': 3, 'b': 2},
            ),
            (
                4,
                [{'$replaceRoot': {'newRoot': {'_id': '$_id', 'x': {'$add': ['$a', '$b']}}}}],
                {'_id': 4, 'x': 3},
            ),
            (
                5,
                [{'$replaceWith': {'_id': '$_id', 'sum': {'$add': ['$a', '$b']}}}],
                {'_id': 5, 'sum': 3},
            ),
            (
                6,
                [{'$unset': 'b'}],
                {'_id': 6, 'a': 1, 'c': {'d': 3}},
            ),
            (
                1,
                [{'$unset': ['a', 'c.d']}],
                {'_id': 1, 'b': 2, 'c': 3},
            ),
        )
        for doc_id, update, expected in data:
            update_result = collection.update_one(
                filter={'_id': doc_id},
                update=update,
            )
            self.assertEqual(update_result.modified_count, 1)
            self.assertEqual(update_result.matched_count, 1)
            self.assert_document_stored(doc_id, expected)

    def test__update_pipeline_upsert(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': 1, 'b': 2})

        update_result = collection.update_one(
            filter={'a': 99, 'b': 100},
            update=[{'$set': {'a': {'$add': ['$a', 10]}}}],
            upsert=True,
        )
        expected = {
            '_id': update_result.upserted_id,
            'a': 109,
            'b': 100,
        }
        self.assertEqual(update_result.matched_count, 0)
        self.assert_document_stored(update_result.upserted_id, expected)

    def test__update_pipeline_upsert__invalid_stage_name(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': 1, 'b': 2})
        with self.assertRaises(mongomock.OperationFailure) as cm:
            collection.update_one(
                filter={'a': 99, 'b': 100},
                update=[{'$set': {'a': {'$invalidStageName': ['$a', 10]}}}],
                upsert=True,
            )
            self.assertIn("Unrecognized pipeline stage name: 'invalidStageName'", str(cm.exception))

    def test__update_pipeline_invalid_stages(self):
        collection = self.db.collection
        data = (
            (
                [{'$unwind': '$a'}],
                '$unwind is not allowed to be used within an update',
            ),
            (
                [{'$count': 'foo'}],
                '$count is not allowed to be used within an update',
            ),
        )
        for pipeline, msg in data:
            with self.assertRaises(mongomock.OperationFailure) as cm:
                collection.update_one(
                    filter={},
                    update=pipeline,
                )
            self.assertIn(msg, str(cm.exception))

    def test__update_pipeline_not_mapping_nor_list(self):
        collection = self.db.collection
        with self.assertRaises(TypeError) as cm:
            collection.update_one(
                filter={},
                update='not_mapping_nor_list',
            )
            self.assertIn(
                (
                    'update must either be a list or an instance of dict, '
                    'bson.son.SON, or any other type that inherits from collections.Mapping'
                ),
                str(cm.exception),
            )

    def test__update_many(self):
        self.db.collection.insert_many([{'a': 1, 'c': 2}, {'a': 1, 'c': 3}, {'a': 2, 'c': 4}])
        update_result = self.db.collection.update_many(filter={'a': 1}, update={'$set': {'c': 0}})
        self.assertEqual(update_result.modified_count, 2)
        self.assertEqual(update_result.matched_count, 2)
        self.assertIsNone(update_result.upserted_id)
        self.assert_documents([{'a': 1, 'c': 0}, {'a': 1, 'c': 0}, {'a': 2, 'c': 4}])

    def test__update_many_collation(self):
        self.db.collection.insert_many([{'a': 1, 'c': 2}, {'a': 1, 'c': 3}, {'a': 2, 'c': 4}])
        self.db.collection.update_many(
            filter={'a': 1},
            update={'$set': {'c': 0}},
            collation=None,
        )
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_many(
                filter={'a': 1},
                update={'$set': {'c': 0}},
                collation='fr',
            )

    def test__update_many_array_filters(self):
        self.db.collection.insert_many(
            [{'a': 1, 'c': [2, 5, 6]}, {'a': 1, 'c': [3, 4, 5]}, {'a': 2, 'c': [12, 15]}]
        )
        self.db.collection.update_many(
            filter={'a': 1},
            update={'$set': {'c.$[e]': 0}},
            array_filters=[{'e': {'$lt': 5}}],
        )
        doc = self.db.collection.find_one({'a': 1, 'c': [0, 5, 6]})
        self.assertIsNotNone(doc)
        doc2 = self.db.collection.find_one({'a': 2})
        self.assertEqual(doc2['c'], [12, 15])

    def test__update_many_let(self):
        self.db.collection.insert_many([{'a': 1, 'c': 2}, {'a': 1, 'c': 3}, {'a': 2, 'c': 4}])
        self.db.collection.update_many(
            filter={'a': 1},
            update={'$set': {'c': '$$newValue'}},
            let=None,
        )
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_many(
                filter={'a': 1},
                update={'$set': {'c': '$$newValue'}},
                let={'newValue': 0},
            )

    def test__update_many_upsert(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_many(
            filter={'a': 1}, update={'$set': {'a': 1, 'c': 0}}, upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'a': 1, 'c': 0})

    def test__update_non_json_values(self):
        self.db.collection.insert_one({'a': collections.Counter({'b': 1})})
        self.assertEqual({'b': 1}, self.db.collection.find_one()['a'])
        self.db.collection.update_one({}, {'$set': {'a': collections.Counter({'b': 2})}})
        self.assertEqual({'b': 2}, self.db.collection.find_one()['a'])

    def test__update_push_slice_from_the_end(self):
        self.db.collection.insert_one({'scores': [40, 50, 60]})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'scores': {
                        '$each': [80, 78, 86],
                        '$slice': -5,
                    }
                }
            },
        )
        self.assertEqual([50, 60, 80, 78, 86], self.db.collection.find_one()['scores'])

    def test__update_push_slice_from_the_front(self):
        self.db.collection.insert_one({'scores': [89, 90]})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'scores': {
                        '$each': [100, 20],
                        '$slice': 3,
                    }
                }
            },
        )
        self.assertEqual([89, 90, 100], self.db.collection.find_one()['scores'])

    def test__update_push_slice_to_zero(self):
        self.db.collection.insert_one({'scores': [40, 50, 60]})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'scores': {
                        '$each': [80, 78, 86],
                        '$slice': 0,
                    }
                }
            },
        )
        self.assertEqual([], self.db.collection.find_one()['scores'])

    def test__update_push_slice_only(self):
        self.db.collection.insert_one({'scores': [89, 70, 100, 20]})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'scores': {
                        '$each': [],
                        '$slice': -3,
                    }
                }
            },
        )
        self.assertEqual([70, 100, 20], self.db.collection.find_one()['scores'])

    def test__update_push_slice_nested_field(self):
        self.db.collection.insert_one({'games': [{'scores': [89, 70, 100, 20]}]})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'games.0.scores': {
                        '$each': [15],
                        '$slice': -3,
                    }
                }
            },
        )
        self.assertEqual([100, 20, 15], self.db.collection.find_one()['games'][0]['scores'])

    def test__update_push_slice_positional_nested_field(self):
        self.db.collection.insert_one({'games': [{'scores': [0, 1]}, {'scores': [2, 3]}]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'scores.0': 2}}},
            {
                '$push': {
                    'games.$.scores': {
                        '$each': [15],
                        '$slice': -2,
                    }
                }
            },
        )
        self.assertEqual([0, 1], self.db.collection.find_one()['games'][0]['scores'])
        self.assertEqual([3, 15], self.db.collection.find_one()['games'][1]['scores'])

    def test__update_push_sort(self):
        self.db.collection.insert_one({'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'a.b': {
                        '$each': [{'value': 4}],
                        '$sort': {'value': 1},
                    }
                }
            },
        )
        self.assertEqual(
            {'b': [{'value': 1}, {'value': 2}, {'value': 3}, {'value': 4}]},
            self.db.collection.find_one()['a'],
        )

    def test__update_push_sort_document(self):
        self.db.collection.insert_one({'a': {'b': [3, 1, 2]}})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'a.b': {
                        '$each': [4, 5],
                        '$sort': -1,
                    }
                }
            },
        )
        self.assertEqual({'b': [5, 4, 3, 2, 1]}, self.db.collection.find_one()['a'])

    def test__update_push_position(self):
        self.db.collection.insert_one({'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'a.b': {
                        '$each': [{'value': 4}],
                        '$position': 1,
                    }
                }
            },
        )
        self.assertEqual(
            {'b': [{'value': 3}, {'value': 4}, {'value': 1}, {'value': 2}]},
            self.db.collection.find_one()['a'],
        )

    def test__update_push_negative_position(self):
        self.db.collection.insert_one({'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one(
            {},
            {
                '$push': {
                    'a.b': {
                        '$each': [{'value': 4}],
                        '$position': -2,
                    }
                }
            },
        )
        self.assertEqual(
            {'b': [{'value': 3}, {'value': 4}, {'value': 1}, {'value': 2}]},
            self.db.collection.find_one()['a'],
        )

    def test__update_push_other_clauses(self):
        self.db.collection.insert_one({'games': [{'scores': [0, 1]}, {'scores': [2, 3]}]})
        with self.assertRaises(mongomock.WriteError):
            self.db.collection.update_one(
                {'games': {'$elemMatch': {'scores.0': 2}}},
                {
                    '$push': {
                        'games.$.scores': {
                            '$each': [15, 13],
                            '$a_clause_that_does_not_exit': 1,
                        }
                    }
                },
            )

    def test__update_push_positional_nested_field(self):
        self.db.collection.insert_one({'games': [{}]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'player.scores': {'$exists': False}}}},
            {'$push': {'games.$.player.scores': 15}},
        )
        self.assertEqual([{'player': {'scores': [15]}}], self.db.collection.find_one()['games'])

    def test__update_push_array_of_arrays(self):
        self.db.collection.insert_one({'games': [[0], [1]]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'0': 1}}}, {'$push': {'games.$': 15}}
        )
        self.assertEqual([[0], [1, 15]], self.db.collection.find_one()['games'])

    def test__update_pull_filter_operator(self):
        self.db.collection.insert_one({'b': 0, 'arr': [0, 1, 2, 3, 4]})
        self.db.collection.update_one({}, {'$pull': {'arr': {'$gt': 2}}})
        self.assertEqual({'b': 0, 'arr': [0, 1, 2]}, self.db.collection.find_one({}, {'_id': 0}))

    def test__update_pull_filter_operator_on_subdocs(self):
        self.db.collection.insert_one({'arr': [{'size': 0}, {'size': 1}]})
        self.db.collection.update_one({}, {'$pull': {'arr': {'size': {'$gte': 1}}}})
        self.assertEqual({'arr': [{'size': 0}]}, self.db.collection.find_one({}, {'_id': 0}))

    def test__update_pull_in(self):
        self.db.collection.insert_one({'b': 0, 'arr': ['a1', 'a2']})
        self.db.collection.update_one({}, {'$pull': {'arr': {'$in': ['a1']}}})
        self.assertEqual({'b': 0, 'arr': ['a2']}, self.db.collection.find_one({}, {'_id': 0}))

    def test__update_pull_in_nested(self):
        self.db.collection.insert_one(
            {
                'food': {
                    'fruits': ['apples', 'pears', 'oranges', 'grapes', 'bananas'],
                    'vegetables': ['carrots', 'celery', 'squash', 'carrots'],
                }
            }
        )
        self.db.collection.update_one(
            {},
            {
                '$pull': {
                    'food.fruits': {'$in': ['apples', 'oranges']},
                    'food.vegetables': 'carrots',
                }
            },
        )
        self.assertEqual(
            {
                'food': {
                    'fruits': ['pears', 'grapes', 'bananas'],
                    'vegetables': ['celery', 'squash'],
                }
            },
            self.db.collection.find_one({}, {'_id': 0}),
        )

    def test__update_pop(self):
        self.db.collection.insert_one({'name': 'bob', 'hat': ['green', 'tall']})
        self.db.collection.update_one({'name': 'bob'}, {'$pop': {'hat': 1}})
        res = self.db.collection.find_one({'name': 'bob'})
        self.assertEqual(['green'], res['hat'])

    def test__update_pop_negative_index(self):
        self.db.collection.insert_one({'name': 'bob', 'hat': ['green', 'tall']})
        self.db.collection.update_one({'name': 'bob'}, {'$pop': {'hat': -1}})
        res = self.db.collection.find_one({'name': 'bob'})
        self.assertEqual(['tall'], res['hat'])

    def test__update_pop_large_index(self):
        self.db.collection.insert_one({'name': 'bob', 'hat': [['green', 'tall']]})
        self.db.collection.update_one({'name': 'bob'}, {'$pop': {'hat.1': 1}})
        res = self.db.collection.find_one({'name': 'bob'})
        self.assertEqual([['green', 'tall']], res['hat'])

    def test__update_pop_empty(self):
        self.db.collection.insert_one({'name': 'bob', 'hat': []})
        self.db.collection.update_one({'name': 'bob'}, {'$pop': {'hat': 1}})
        res = self.db.collection.find_one({'name': 'bob'})
        self.assertEqual([], res['hat'])

    def test__replace_one(self):
        self.db.collection.insert_one({'a': 1, 'b': 2})
        self.assert_documents([{'a': 1, 'b': 2}])

        result = self.db.collection.replace_one(filter={'a': 2}, replacement={'x': 1, 'y': 2})
        self.assert_documents([{'a': 1, 'b': 2}])
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

        result = self.db.collection.replace_one(filter={'a': 1}, replacement={'x': 1, 'y': 2})
        self.assert_documents([{'x': 1, 'y': 2}])
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)

    def test__replace_one_upsert(self):
        self.assert_document_count(0)
        result = self.db.collection.replace_one(
            filter={'a': 2}, replacement={'x': 1, 'y': 2}, upsert=True
        )
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsNotNone(result.upserted_id)
        self.assert_document_stored(result.upserted_id, {'x': 1, 'y': 2})

    def test__replace_one_invalid(self):
        with self.assertRaises(ValueError):
            self.db.collection.replace_one(filter={'a': 2}, replacement={'$set': {'x': 1, 'y': 2}})

    def test__update_one_invalid(self):
        with self.assertRaises(ValueError):
            self.db.collection.update_one({'a': 2}, {})

    def test__delete_one(self):
        self.assert_document_count(0)
        self.db.collection.insert_one({'a': 1})
        self.assert_document_count(1)

        self.db.collection.delete_one({'a': 2})
        self.assert_document_count(1)

        self.db.collection.delete_one({'a': 1})
        self.assert_document_count(0)

    def test__delete_one_invalid_filter(self):
        with self.assertRaises(TypeError):
            self.db.collection.delete_one('a')

        with self.assertRaises(TypeError):
            self.db.collection.delete_one(['a'])

    def test__delete_many(self):
        self.db.collection.insert_many([{'a': 1, 'c': 2}, {'a': 1, 'c': 3}, {'a': 2, 'c': 4}])
        self.assert_document_count(3)

        self.db.collection.delete_many({'a': 2})
        self.assert_document_count(2)

        self.db.collection.delete_many({'a': 1})
        self.assert_document_count(0)

    def test__delete_many_collation_option(self):
        """Ensure collation delete_many's option is not rejected."""
        self.assertTrue(self.db.collection.delete_many({}, collation=None))
        with self.assertRaises(NotImplementedError):
            self.db.collection.delete_many({}, collation='fr')

    def test__delete_many_hint_option(self):
        """Ensure hint delete_many's option is not rejected."""
        self.assertTrue(self.db.collection.delete_many({}, hint=None))
        with self.assertRaises(NotImplementedError):
            self.db.collection.delete_many({}, hint='_index')

    def test__string_matching(self):
        """Make sure strings are not treated as collections on find"""
        self.db['abc'].insert_one({'name': 'test1'})
        self.db['abc'].insert_one({'name': 'test2'})
        # now searching for 'name':'e' returns test1
        self.assertIsNone(self.db['abc'].find_one({'name': 'e'}))

    def test__collection_is_indexable(self):
        self.db['def'].insert_one({'name': 'test1'})
        self.assertEqual(self.db['def'].find({'name': 'test1'})[0]['name'], 'test1')

    def test__cursor_distinct(self):
        larry_bob = {'name': 'larry'}
        larry = {'name': 'larry'}
        gary = {'name': 'gary'}
        self.db['coll_name'].insert_many([larry_bob, larry, gary])
        ret_val = self.db['coll_name'].find().distinct('name')
        self.assertIsInstance(ret_val, list)
        self.assertTrue(set(ret_val) == {'larry', 'gary'})

    def test__cursor_limit(self):
        self.db.collection.insert_many([{'a': i} for i in range(100)])
        cursor = self.db.collection.find().limit(30)
        first_ones = list(cursor)
        self.assertEqual(30, len(first_ones))

    def test__cursor_negative_limit(self):
        self.db.collection.insert_many([{'a': i} for i in range(100)])
        cursor = self.db.collection.find().limit(-30)
        first_ones = list(cursor)
        self.assertEqual(30, len(first_ones))

    def test__cursor_getitem_when_db_changes(self):
        self.db['coll_name'].insert_one({})
        cursor = self.db['coll_name'].find()
        self.db['coll_name'].insert_one({})
        cursor_items = list(cursor)
        self.assertEqual(len(cursor_items), 2)

    def test__cursor_getitem(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert_many([first, second, third])
        cursor = self.db['coll_name'].find()
        item = cursor[0]
        self.assertEqual(item['name'], 'first')

    def test__cursor_getitem_slice(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert_many([first, second, third])
        cursor = self.db['coll_name'].find()
        ret = cursor[1:4]
        self.assertIs(ret, cursor)
        count = sum(1 for d in cursor)
        self.assertEqual(count, 2)

    def test__cursor_getitem_negative_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert_many([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(IndexError):
            cursor[-1]  # pylint: disable=pointless-statement

    def test__cursor_getitem_bad_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert_many([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(TypeError):
            cursor['not_a_number']  # pylint: disable=pointless-statement

    def test__find_with_skip_param(self):
        """Make sure that find() will take in account skip parameter"""

        u1 = {'name': 'first'}
        u2 = {'name': 'second'}
        self.db['users'].insert_many([u1, u2])
        count = sum(1 for d in self.db['users'].find(sort=[('name', 1)], skip=1))
        self.assertEqual(1, count)
        self.assertEqual(self.db['users'].find(sort=[('name', 1)], skip=1)[0]['name'], 'second')

    def test__ordered_insert_find(self):
        """Tests ordered inserts

        If we insert values 1, 2, 3 and find them, we must see them in order as
        we inserted them.
        """

        values = list(range(20))
        random.shuffle(values)
        for val in values:
            self.db.collection.insert_one({'_id': val})

        find_cursor = self.db.collection.find()

        for val in values:
            in_db_val = find_cursor.next()
            expected = {'_id': val}
            self.assertEqual(in_db_val, expected)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.create_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_uniq_idxs_with_descending_ordering(self):
        self.db.collection.create_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__create_uniq_idxs_without_ordering(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__create_index_duplicate(self):
        self.db.collection.create_index([('value', 1)])
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.create_index([('value', 1)], unique=True)

    def test__create_index_wrong_type(self):
        with self.assertRaises(TypeError):
            self.db.collection.create_index({'value': 1})
        with self.assertRaises(TypeError):
            self.db.collection.create_index([('value', 1, 'foo', 'bar')])

    def test__ttl_index_ignores_record_in_the_future(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one(
            {'value': datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=100)}
        )
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_index_ignores_records_with_non_datetime_values(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one({'value': 'not a dt'})
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_index_record_expiry(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=5)
        self.db.collection.insert_one(
            {'value': datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5)}
        )
        self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__ttl_expiration_of_0(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__ttl_with_non_integer_value_is_ignored(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds='a')
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_applied_to_compound_key_is_ignored(self):
        self.db.collection.create_index([('field1', 1), ('field2', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one(
            {'field1': datetime.now(timezone.utc).replace(tzinfo=None), 'field2': 'val2'}
        )
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_ignored_when_document_does_not_contain_indexed_field(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one(
            {'other_value': datetime.now(timezone.utc).replace(tzinfo=None)}
        )
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_of_array_field_expiration(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=5)
        self.db.collection.insert_one(
            {
                'value': [
                    'a',
                    'b',
                    datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=100),
                ]
            }
        )
        self.assertEqual(self.db.collection.count_documents({}), 1)

        self.db.collection.drop()
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=5)
        self.db.collection.insert_one(
            {
                'value': [
                    'a',
                    'b',
                    datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5),
                    datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=100),
                ]
            }
        )
        self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__ttl_of_array_field_without_datetime_does_not_expire(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=5)
        self.db.collection.insert_one({'value': ['a', 'b', 'c', 1, 2, 3]})
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_expiry_with_mock(self):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=100)
        self.db.collection.insert_one({'value': now + timedelta(seconds=100)})
        self.assertEqual(self.db.collection.count_documents({}), 1)

        with mock.patch('mongomock.utcnow') as mongomock_ng_utcnow:
            mongomock_ng_utcnow.return_value = now + timedelta(100)
            self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__ttl_index_is_removed_if_collection_dropped(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 0)

        self.db.collection.drop()
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_index_is_removed_when_index_is_dropped(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 0)

        self.db.collection.drop_index('value_1')
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})
        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ttl_index_removes_expired_documents_prior_to_removal(self):
        self.db.collection.create_index([('value', 1)], expireAfterSeconds=0)
        self.db.collection.insert_one({'value': datetime.now(timezone.utc).replace(tzinfo=None)})

        self.db.collection.drop_index('value_1')
        self.assertEqual(self.db.collection.count_documents({}), 0)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_indexes_with_expireAfterSeconds(self):
        indexes = [
            pymongo.operations.IndexModel([('value', pymongo.ASCENDING)], expireAfterSeconds=5),
        ]
        index_names = self.db.collection.create_indexes(indexes)
        self.assertEqual(1, len(index_names))

        self.db.collection.insert_one(
            {'value': datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5)}
        )
        self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__ttl_expires_nested_field(self):
        self.db.collection.create_index([('data.timestamp', 1)], expireAfterSeconds=5)
        self.db.collection.insert_one(
            {
                'data': {
                    'timestamp': datetime.now(timezone.utc).replace(tzinfo=None)
                    - timedelta(seconds=5)
                }
            }
        )
        self.assertEqual(self.db.collection.count_documents({}), 0)

    def test__create_indexes_wrong_type(self):
        indexes = [('value', 1), ('name', 1)]
        with self.assertRaises(TypeError):
            self.db.collection.create_indexes(indexes)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_indexes_uniq_idxs(self):
        indexes = [
            pymongo.operations.IndexModel([('value', pymongo.ASCENDING)], unique=True),
            pymongo.operations.IndexModel([('name', pymongo.ASCENDING)], unique=True),
        ]
        index_names = self.db.collection.create_indexes(indexes)
        self.assertEqual(2, len(index_names))

        self.db.collection.insert_one({'value': 1, 'name': 'bob'})
        # Ensure both uniq indexes have been created
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1, 'name': 'different'})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 0, 'name': 'bob'})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_indexes_with_partial_filter_expression(self):
        self.db.collection.insert_one({'status': 'draft', 'value': 1})
        self.db.collection.insert_one({'status': 'draft', 'value': 1})
        indexes = [
            pymongo.operations.IndexModel(
                [('value', pymongo.ASCENDING)],
                unique=True,
                partialFilterExpression={'status': 'published'},
            ),
        ]
        index_names = self.db.collection.create_indexes(indexes)
        self.assertEqual(1, len(index_names))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_indexes_names(self):
        indexes = [
            pymongo.operations.IndexModel([('value', pymongo.ASCENDING)], name='index_name'),
            pymongo.operations.IndexModel([('name', pymongo.ASCENDING)], unique=True),
        ]
        index_names = self.db.collection.create_indexes(indexes)
        self.assertEqual(['index_name', 'name_1'], index_names)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__ensure_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.create_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__ensure_uniq_idxs_with_descending_ordering(self):
        self.db.collection.create_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__ensure_uniq_idxs_on_nested_field(self):
        self.db.collection.create_index([('a.b', 1)], unique=True)

        self.db.collection.insert_one({'a': 1})
        self.db.collection.insert_one({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'a': {'b': 1}})

        self.assertEqual(self.db.collection.count_documents({}), 2)

    def test__ensure_sparse_uniq_idxs_on_nested_field(self):
        self.db.collection.create_index([('a.b', 1)], unique=True, sparse=True)
        self.db.collection.create_index([('c', 1)], unique=True, sparse=True)

        self.db.collection.insert_one({})
        self.db.collection.insert_one({})
        self.db.collection.insert_one({'c': 1})
        self.db.collection.insert_one({'a': 1})
        self.db.collection.insert_one({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'c': 1})

        self.assertEqual(self.db.collection.count_documents({}), 5)

    def test__ensure_partial_filter_expression_unique_index(self):
        self.db.collection.delete_many({})
        self.db.collection.create_index(
            (('partialFilterExpression_value', 1), ('value', 1)),
            unique=True,
            partialFilterExpression={'partialFilterExpression_value': {'$eq': 1}},
        )

        # We should be able to add documents with duplicated `value` and
        # `partialFilterExpression_value` if `partialFilterExpression_value` isn't set to 1
        self.db.collection.insert_one({'partialFilterExpression_value': 3, 'value': 4})
        self.db.collection.insert_one({'partialFilterExpression_value': 3, 'value': 4})

        # We should be able to add documents with distinct `value` values and duplicated
        # `partialFilterExpression_value` value set to 1.
        self.db.collection.insert_one({'partialFilterExpression_value': 1, 'value': 2})
        self.db.collection.insert_one({'partialFilterExpression_value': 1, 'value': 3})

        # We should not be able to add documents with duplicated `partialFilterExpression_value` and
        # `value` values if `partialFilterExpression_value` is 1.
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'partialFilterExpression_value': 1, 'value': 3})

        self.assertEqual(self.db.collection.count_documents({}), 4)

    def test__partial_filter_expression_create_index_skips_non_matching(self):
        self.db.collection.insert_one({'status': 'draft', 'value': 1})
        self.db.collection.insert_one({'status': 'draft', 'value': 1})
        self.db.collection.create_index(
            [('value', 1)],
            unique=True,
            partialFilterExpression={'status': 'published'},
        )

    def test__partial_filter_expression_create_index_rejects_matching_dupes(self):
        self.db.collection.insert_one({'status': 'published', 'value': 1})
        self.db.collection.insert_one({'status': 'published', 'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.create_index(
                [('value', 1)],
                unique=True,
                partialFilterExpression={'status': 'published'},
            )

    def test__ensure_uniq_idxs_without_ordering(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__insert_empty_doc_uniq_idx(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'value': 1})
        self.db.collection.insert_one({})

        self.assertEqual(self.db.collection.count_documents({}), 2)

    def test__insert_empty_doc_twice_uniq_idx(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test_sparse_unique_index(self):
        self.db.collection.create_index([('value', 1)], unique=True, sparse=True)

        self.db.collection.insert_one({})
        self.db.collection.insert_one({})
        self.db.collection.insert_one({'value': None})
        self.db.collection.insert_one({'value': None})

        self.assertEqual(self.db.collection.count_documents({}), 4)

    def test_unique_index_with_upsert_insertion(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'_id': 1, 'value': 1})
        # Updating document should not trigger error
        self.db.collection.replace_one({'_id': 1}, {'value': 1})
        self.db.collection.replace_one({'value': 1}, {'value': 1}, upsert=True)
        # Creating new documents with same value should
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.replace_one({'bad': 'condition'}, {'value': 1}, upsert=True)
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'_id': 2, 'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update_one({'_id': 2}, {'$set': {'value': 1}}, upsert=True)

    def test_unique_index_with_update(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'_id': 1, 'value': 1})
        self.db.collection.insert_one({'_id': 2, 'value': 2})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.replace_one({'value': 1}, {'value': 2})

    def test_unique_index_with_update_on_nested_field(self):
        self.db.collection.create_index([('a.b', 1)], unique=True)

        self.db.collection.insert_one({'_id': 1, 'a': {'b': 1}})
        self.db.collection.insert_one({'_id': 2, 'a': {'b': 2}})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update_one({'_id': 1}, {'$set': {'a.b': 2}})

    def test_unique_index_on_dict(self):
        self.db.collection.insert_one({'_id': 1, 'a': {'b': 1}})
        self.db.collection.insert_one({'_id': 2, 'a': {'b': 2}})

        self.db.collection.create_index([('a', 1)], unique=True)

        self.db.collection.insert_one({'_id': 3, 'a': {'b': 3}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'_id': 4, 'a': {'b': 2}})

    def test_sparse_unique_index_dup(self):
        self.db.collection.create_index([('value', 1)], unique=True, sparse=True)

        self.db.collection.insert_one({'value': 'a'})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 'a'})

        self.assertEqual(self.db.collection.count_documents({}), 1)

    def test__unique_index_with_shared_array_element(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'value': [1, 2]})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': [2, 3]})

    def test__unique_index_array_element_conflicts_with_scalar(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'value': [1, 2]})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'value': 2})

    def test__unique_index_scalar_does_not_conflict_with_array(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'value': 1})
        self.db.collection.insert_one({'value': [2, 3]})

    def test__create_uniq_idxs_with_dupes_already_there(self):
        self.db.collection.insert_one({'value': 1})
        self.db.collection.insert_one({'value': 1})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert_one({'value': 1})
        self.assertEqual(self.db.collection.count_documents({}), 3)

    def test__create_uniq_idx_with_array_dupes_already_there(self):
        self.db.collection.insert_one({'value': [1, 2]})
        self.db.collection.insert_one({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.create_index([('value', 1)], unique=True)

    def test__create_uniq_idx_with_array_no_dupes(self):
        self.db.collection.insert_one({'value': 1})
        self.db.collection.insert_one({'value': [2, 3]})
        self.db.collection.create_index([('value', 1)], unique=True)

    def test__create_uniq_idx_with_shared_array_element_and_partial_filter(self):
        self.db.collection.insert_one({'status': 'published', 'value': [1, 2]})
        self.db.collection.insert_one({'status': 'published', 'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.create_index(
                [('value', 1)],
                unique=True,
                partialFilterExpression={'status': 'published'},
            )

    def test__create_uniq_idx_with_array_element_update_conflict(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'_id': 1, 'value': [1, 2]})
        self.db.collection.insert_one({'_id': 2, 'value': 3})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update_one({'_id': 2}, {'$set': {'value': 2}})

    def test__sparse_unique_index_skip_missing_field(self):
        self.db.collection.create_index([('value', 1)], unique=True, sparse=True)
        self.db.collection.insert_one({'name': 'no_value'})
        self.db.collection.insert_one({'name': 'another_no_value'})

    def test__sparse_unique_index_skip_all_null(self):
        self.db.collection.create_index([('value', 1)], unique=True, sparse=True)
        self.db.collection.insert_one({'value': None})
        self.db.collection.insert_one({'value': None})

    def test__unique_index_update_same_doc_preserves_self(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'_id': 1, 'value': 1})
        self.db.collection.update_one({'_id': 1}, {'$set': {'name': 'updated'}})
        self.assertEqual(self.db.collection.find_one({'_id': 1})['name'], 'updated')

    def test__unique_index_on_dict_value(self):
        self.db.collection.create_index([('value', 1)], unique=True)
        self.db.collection.insert_one({'_id': 1, 'value': {'nested': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert_one({'_id': 2, 'value': {'nested': 1}})

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_index_with_name(self):
        name = self.db.collection.create_index([('value', 1)], name='index_name')
        self.assertEqual('index_name', name)
        self.db.collection.create_index([('value', 1)], name='index_name')
        self.assertEqual({'_id_', 'index_name'}, set(self.db.collection.index_information().keys()))

    def test__insert_empty_doc_idx_information(self):
        self.db.collection.insert_one({})

        index_information = self.db.collection.index_information()
        self.assertEqual(
            {'_id_': {'v': 2, 'key': [('_id', 1)]}},
            index_information,
        )
        self.assertEqual(
            [{'name': '_id_', 'key': {'_id': 1}, 'v': 2}],
            list(self.db.collection.list_indexes()),
        )

        del index_information['_id_']

        self.assertEqual(
            {'_id_': {'v': 2, 'key': [('_id', 1)]}},
            self.db.collection.index_information(),
            msg='index_information is immutable',
        )

    def test__empty_table_idx_information(self):
        self.db.collection.drop()
        index_information = self.db.collection.index_information()
        self.assertEqual({}, index_information)

    def test__create_idx_information(self):
        index = self.db.collection.create_index([('value', 1)])

        self.db.collection.insert_one({})

        self.assertDictEqual(
            {
                'key': [('value', 1)],
                'v': 2,
            },
            self.db.collection.index_information()[index],
        )
        self.assertEqual({'_id_', index}, set(self.db.collection.index_information().keys()))

        self.db.collection.drop_index(index)
        self.assertEqual({'_id_'}, set(self.db.collection.index_information().keys()))

    def test__drop_index_not_found(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.drop_index('unknownIndex')

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_unique_idx_information_with_ascending_ordering(self):
        index = self.db.collection.create_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})

        self.assertDictEqual(
            {
                'key': [('value', pymongo.ASCENDING)],
                'unique': True,
                'v': 2,
            },
            self.db.collection.index_information()[index],
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__create_unique_idx_information_with_descending_ordering(self):
        index = self.db.collection.create_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert_one({'value': 1})

        self.assertDictEqual(
            self.db.collection.index_information()[index],
            {
                'key': [('value', pymongo.DESCENDING)],
                'unique': True,
                'v': 2,
            },
        )

    def test__set_with_positional_operator(self):
        """Real mongodb support positional operator $ for $set operation"""
        base_document = {
            'int_field': 1,
            'list_field': [{'str_field': 'a'}, {'str_field': 'b'}, {'str_field': 'c'}],
        }

        self.db.collection.insert_one(base_document)
        self.db.collection.update_one(
            {'int_field': 1, 'list_field.str_field': 'b'}, {'$set': {'list_field.$.marker': True}}
        )

        expected_document = copy.deepcopy(base_document)
        expected_document['list_field'][1]['marker'] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

        self.db.collection.update_one(
            {'int_field': 1, 'list_field.str_field': 'a'}, {'$set': {'list_field.$.marker': True}}
        )

        self.db.collection.update_one(
            {'int_field': 1, 'list_field.str_field': 'c'}, {'$set': {'list_field.$.marker': True}}
        )

        expected_document['list_field'][0]['marker'] = True
        expected_document['list_field'][2]['marker'] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__set_replace_subdocument(self):
        base_document = {
            'int_field': 1,
            'list_field': [
                {'str_field': 'a'},
                {'str_field': 'b', 'int_field': 1},
                {'str_field': 'c'},
            ],
        }
        new_subdoc = {'str_field': 'x'}
        self.db.collection.insert_one(base_document)
        self.db.collection.update_one({'int_field': 1}, {'$set': {'list_field.1': new_subdoc}})

        self.db.collection.update_one(
            {'int_field': 1, 'list_field.2.str_field': 'c'}, {'$set': {'list_field.2': new_subdoc}}
        )

        expected_document = copy.deepcopy(base_document)
        expected_document['list_field'][1] = new_subdoc
        expected_document['list_field'][2] = new_subdoc

        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__set_replace_subdocument_positional_operator(self):
        base_document = {
            'int_field': 1,
            'list_field': [
                {'str_field': 'a'},
                {'str_field': 'b', 'int_field': 1},
                {'str_field': 'c'},
            ],
        }
        new_subdoc = {'str_field': 'x'}
        self.db.collection.insert_one(base_document)
        self.db.collection.update_one(
            {'int_field': 1, 'list_field.str_field': 'b'}, {'$set': {'list_field.$': new_subdoc}}
        )

        expected_document = copy.deepcopy(base_document)
        expected_document['list_field'][1] = new_subdoc

        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__cursor_sort_kept_after_clone(self):
        self.db.collection.insert_one({'time_check': float(time.time())})

        cursor = self.db.collection.find({}, sort=[('time_check', -1)])
        cursor2 = cursor.clone()
        cursor3 = self.db.collection.find({})
        cursor3.sort([('time_check', -1)])
        cursor4 = cursor3.clone()
        cursor_result = list(cursor)
        cursor2_result = list(cursor2)
        cursor3_result = list(cursor3)
        cursor4_result = list(cursor4)
        self.assertEqual(cursor2_result, cursor_result)
        self.assertEqual(cursor3_result, cursor_result)
        self.assertEqual(cursor4_result, cursor_result)

    def test__avoid_change_data_after_set(self):
        test_data = {'test': ['test_data']}
        self.db.collection.insert_one({'_id': 1})
        self.db.collection.update_one({'_id': 1}, {'$set': test_data})

        self.db.collection.update_one({'_id': 1}, {'$addToSet': {'test': 'another_one'}})
        data_in_db = self.db.collection.find_one({'_id': 1})
        self.assertNotEqual(data_in_db['test'], test_data['test'])
        self.assertEqual(len(test_data['test']), 1)
        self.assertEqual(len(data_in_db['test']), 2)

    def test__filter_with_ne(self):
        self.db.collection.insert_one({'_id': 1, 'test_list': [{'data': 'val'}]})
        data_in_db = self.db.collection.find({'test_list.marker_field': {'$ne': True}})
        self.assertEqual(list(data_in_db), [{'_id': 1, 'test_list': [{'data': 'val'}]}])

    def test__filter_with_ne_none(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'field1': 'baz', 'field2': 'bar'},
                {'_id': 2, 'field1': 'baz'},
                {'_id': 3, 'field1': 'baz', 'field2': None},
                {'_id': 4, 'field1': 'baz', 'field2': False},
                {'_id': 5, 'field1': 'baz', 'field2': 0},
            ]
        )
        data_in_db = self.db.collection.find({'field1': 'baz', 'field2': {'$ne': None}})
        self.assertEqual([1, 4, 5], [d['_id'] for d in data_in_db])

    def test__filter_unknown_top_level(self):
        with self.assertRaises(mongomock.OperationFailure) as error:
            self.db.collection.find_one({'$and': [{'$ne': False}]})
        self.assertEqual('unknown top level operator: $ne', str(error.exception))

    def test__filter_unknown_op(self):
        with self.assertRaises(mongomock.OperationFailure) as error:
            self.db.collection.find_one({'a': {'$foo': 3}})
        self.assertEqual('unknown operator: $foo', str(error.exception))

    def test__filter_on_dict(self):
        self.db.collection.insert_one({'doc': {}})
        self.assertTrue(self.db.collection.find_one({'doc': {}}))

    def test__find_or(self):
        self.db.collection.insert_many(
            [
                {'x': 4},
                {'x': [2, 4, 6, 8]},
                {'x': [2, 3, 5, 7]},
                {'x': {}},
            ]
        )
        self.assertEqual(
            [4, [2, 4, 6, 8], [2, 3, 5, 7]],
            [d['x'] for d in self.db.collection.find({'$or': [{'x': 4}, {'x': 2}]})],
        )

    def test__find_with_max_time_ms(self):
        self.db.collection.insert_many([{'x': 1}, {'x': 2}])
        self.assertEqual([1, 2], [d['x'] for d in self.db.collection.find({}, max_time_ms=1000)])

        with self.assertRaises(TypeError):
            self.db.collection.find({}, max_time_ms='1000')

    def test__find_and_project_3_level_deep_nested_field(self):
        self.db.collection.insert_one({'_id': 1, 'a': {'b': {'c': 2}}})
        data_in_db = self.db.collection.find(projection=['a.b.c'])
        self.assertEqual(list(data_in_db), [{'_id': 1, 'a': {'b': {'c': 2}}}])

    def test__find_and_project_wrong_types(self):
        self.db.collection.insert_one({'_id': 1, 'a': {'b': {'c': 2}}})
        with self.assertRaises(TypeError):
            self.db.collection.find_one({}, projection=[{'a': {'b': {'c': 1}}}])

    def test__find_projection_with_subdoc_lists(self):
        doc = {'a': 1, 'b': [{'c': 2, 'd': 3, 'e': 4}, {'c': 5, 'd': 6, 'e': 7}]}
        self.db.collection.insert_one(doc)

        result = self.db.collection.find_one({'a': 1}, {'a': 1, 'b': 1})
        self.assertEqual(result, doc)

        result = self.db.collection.find_one({'a': 1}, {'_id': 0, 'a': 1, 'b.c': 1, 'b.d': 1})
        self.assertEqual(result, {'a': 1, 'b': [{'c': 2, 'd': 3}, {'c': 5, 'd': 6}]})

        result = self.db.collection.find_one({'a': 1}, {'_id': 0, 'a': 0, 'b.c': 0, 'b.e': 0})
        self.assertEqual(result, {'b': [{'d': 3}, {'d': 6}]})

        # Test that a projection that does not fit the document does not result in an error
        result = self.db.collection.find_one({'a': 1}, {'_id': 0, 'a': 1, 'b.c.f': 1})
        self.assertEqual(result, {'a': 1, 'b': [{}, {}]})

    def test__find_projection_with_subdoc_lists_refinements(self):
        doc = {'a': 1, 'b': [{'c': 2, 'd': 3, 'e': 4}, {'c': 5, 'd': 6, 'e': 7}]}
        self.db.collection.insert_one(doc)

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one(
                {'a': 1}, collections.OrderedDict([('a', 1), ('b.c', 1), ('b', 1)])
            )

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one(
                {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 1), ('b', 1), ('b.c', 1)])
            )

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one(
                {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 0), ('b', 0), ('b.c', 0)])
            )

        # This one is tricky: the refinement 'b' overrides the previous 'b.c'
        # but it is not the equivalent of having only 'b'.
        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one(
                {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 0), ('b.c', 0), ('b', 0)])
            )

    def test__find_and_project(self):
        self.db.collection.insert_one({'_id': 1, 'a': 42, 'b': 'other', 'c': {'d': 'nested'}})

        self.assertEqual(
            [{'_id': 1, 'a': 42}], list(self.db.collection.find({}, projection={'a': 1}))
        )
        self.assertEqual(
            [{'_id': 1, 'a': 42}], list(self.db.collection.find({}, projection={'a': '1'}))
        )
        self.assertEqual(
            [{'_id': 1, 'a': 42}], list(self.db.collection.find({}, projection={'a': '0'}))
        )
        self.assertEqual(
            [{'_id': 1, 'a': 42}], list(self.db.collection.find({}, projection={'a': 'other'}))
        )

        self.assertEqual(
            [{'_id': 1, 'b': 'other', 'c': {'d': 'nested'}}],
            list(self.db.collection.find({}, projection={'a': 0})),
        )
        self.assertEqual(
            [{'_id': 1, 'b': 'other', 'c': {'d': 'nested'}}],
            list(self.db.collection.find({}, projection={'a': False})),
        )

    def test__find_and_project_positional(self):
        self.db.collection.insert_one({'_id': 1, 'a': [{'b': 1}, {'b': 2}]})

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a.b': {'$exists': True}}, projection={'a.$.b': 0})

        # Positional $ returns the first matching array element
        result = self.db.collection.find_one({'a.b': {'$exists': True}}, projection={'a.$.b': 1})
        self.assertEqual({'_id': 1, 'a': {'b': 1}}, result)

    def test__find_and_project_positional_match_value(self):
        self.db.collection.insert_one({'_id': 1, 'scores': [10, 50, 90]})

        result = self.db.collection.find_one({'scores': 90}, projection={'scores.$': 1})
        self.assertEqual({'_id': 1, 'scores': 90}, result)

    def test__find_and_project_positional_no_match(self):
        # Filter matches doc via 'c' but no element in 'a' has matching field
        self.db.collection.insert_one({'_id': 1, 'a': [{'b': 1}, {'b': 2}], 'c': 1})

        result = self.db.collection.find_one({'c': 1}, projection={'a.$.b': 1})
        # 'c' not in projection, 'a' omitted because no element matched positional $
        self.assertEqual({'_id': 1}, result)

    def test__find_and_project_positional_multiple_docs(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': [{'b': 1}, {'b': 2}]},
                {'_id': 2, 'a': [{'b': 3}, {'b': 4}]},
                {'_id': 3, 'a': [{'b': 5}, {'b': 6}]},
            ]
        )

        results = list(self.db.collection.find({'a.b': {'$gte': 3}}, projection={'a.$.b': 1}))
        self.assertEqual(
            [
                {'_id': 2, 'a': {'b': 3}},
                {'_id': 3, 'a': {'b': 5}},
            ],
            results,
        )

    def test__find_dict_in_nested_list(self):
        self.db.collection.insert_one({'a': {'b': [{'c': 1}]}})
        self.assertTrue(self.db.collection.find_one({'a.b': {'c': 1}}))

    def test__find_in_not_a_list(self):
        self.db.collection.insert_one({'a': 'a'})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$in': 'not a list'}})

    def test__find_in_by_empty_list(self):
        sample_dicts = [{'key': 1}, {'key': []}, {'key': ''}, {'key': []}]
        self.db.collection.insert_many(sample_dicts)
        filter_by = [[]]
        returned_document = self.db.collection.find_one({'key': {'$in': filter_by}})
        self.assertTrue(returned_document)

    def test__find_one_immutable_data_projection(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'years': {'last_job': 2010}})
        item = collection.find_one({'_id': 1}, projection={'years': True})
        item['years']['last_job'] = 2021
        item_2 = collection.find_one({'_id': 1})
        self.assertEqual(item_2['years']['last_job'], 2010)

    def test__find_many_immutable_data_projection(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': 'it', 'years': {'it': {'last_job': 2011}}})
        collection.insert_one({'_id': 2, 'a': 'it', 'years': {'it': {'last_job': 2012}}})
        items = collection.find({'a': 'it'}, projection={'years': True})
        for item in items:
            item['years']['it']['last_job'] = 2021
        items_2 = list(collection.find({'a': 'it'}, projection={'years': True}))
        self.assertEqual(
            items_2,
            [
                {'_id': 1, 'years': {'it': {'last_job': 2011}}},
                {'_id': 2, 'years': {'it': {'last_job': 2012}}},
            ],
        )

    def test__with_options(self):
        self.db.collection.with_options(read_preference=None)
        self.db.collection.with_options(write_concern=self.db.collection.write_concern)
        self.db.collection.with_options(write_concern=WriteConcern(w=1))
        self.db.collection.with_options(read_concern=self.db.collection.read_concern)
        self.db.collection.with_options(read_concern=ReadConcern(level='local'))

    def test__with_options_different_write_concern(self):
        self.db.collection.insert_one({'name': 'col1'})
        col2 = self.db.collection.with_options(write_concern=WriteConcern(w=2))
        col2.insert_one({'name': 'col2'})

        # Check that the two objects have the same data.
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in self.db.collection.find()})
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in col2.find()})

        # Check that each object has its own write concern.
        self.assertEqual({}, self.db.collection.write_concern.document)
        self.assertNotEqual(self.db.collection.write_concern, col2.write_concern)
        self.assertEqual({'w': 2}, col2.write_concern.document)

    def test__with_options_different_read_concern(self):
        self.db.collection.insert_one({'name': 'col1'})
        col2 = self.db.collection.with_options(read_concern=ReadConcern(level='majority'))
        col2.insert_one({'name': 'col2'})

        # Check that the two objects have the same data.
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in self.db.collection.find()})
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in col2.find()})

        # Check that each object has its own read concern.
        self.assertEqual({}, self.db.collection.read_concern.document)
        self.assertNotEqual(self.db.collection.read_concern, col2.read_concern)
        self.assertEqual({'level': 'majority'}, col2.read_concern.document)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__with_options_different_read_preference(self):
        self.db.collection.insert_one({'name': 'col1'})
        col2 = self.db.collection.with_options(read_preference=ReadPreference.NEAREST)
        col2.insert_one({'name': 'col2'})

        # Check that the two objects have the same data.
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in self.db.collection.find()})
        self.assertEqual({'col1', 'col2'}, {d['name'] for d in col2.find()})

        # Check that each object has its own read preference
        self.assertEqual('primary', self.db.collection.read_preference.mongos_mode)
        self.assertNotEqual(self.db.collection.read_preference, col2.read_preference)
        self.assertEqual('nearest', col2.read_preference.mongos_mode)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__codec_options(self):
        self.assertEqual(codec_options.CodecOptions(), self.db.collection.codec_options)
        self.db.collection.with_options(codec_options.CodecOptions())

    def test__codec_options_without_pymongo(self):
        self.assertEqual(self.db.collection.codec_options, self.db.codec_options)

    def test__with_options_wrong_kwarg(self):
        self.assertRaises(TypeError, self.db.collection.with_options, red_preference=None)

    def test__with_options_not_implemented(self):
        _CodecOptions = collections.namedtuple(
            'CodecOptions', ['document_class', 'tz_aware', 'uuid_representation']
        )
        with self.assertRaises(NotImplementedError):
            self.db.collection.with_options(codec_options=_CodecOptions(None, True, 3))

    def test__with_options_wrong_type(self):
        with self.assertRaises(TypeError):
            self.db.collection.with_options(write_concern=1)

    def test__update_current_date(self):
        for type_specification in [True, {'$type': 'date'}]:
            self.db.collection.update_one(
                {}, {'$currentDate': {'updated_at': type_specification}}, upsert=True
            )
            self.assertIsInstance(self.db.collection.find_one({})['updated_at'], datetime)

    def test_datetime_precision(self):
        too_precise_dt = datetime(2000, 1, 1, 12, 30, 30, 123456)
        mongo_dt = datetime(2000, 1, 1, 12, 30, 30, 123000)
        objid = self.db.collection.insert_one(
            {'date_too_precise': too_precise_dt, 'date': mongo_dt}
        ).inserted_id
        self.assert_document_count(1)
        # Given both date are equivalent, we can mix them
        self.db.collection.update_one(
            {'date_too_precise': mongo_dt, 'date': too_precise_dt},
            {'$set': {'new_date_too_precise': too_precise_dt, 'new_date': mongo_dt}},
            upsert=True,
        )
        self.assert_document_count(1)
        doc = self.db.collection.find_one(
            {'new_date_too_precise': mongo_dt, 'new_date': too_precise_dt}
        )
        assert doc == {
            '_id': objid,
            'date_too_precise': mongo_dt,
            'date': mongo_dt,
            'new_date_too_precise': mongo_dt,
            'new_date': mongo_dt,
        }
        self.db.collection.delete_one(
            {'new_date_too_precise': mongo_dt, 'new_date': too_precise_dt}
        )
        self.assert_document_count(0)

    def test__mix_tz_naive_aware(self):
        utc2tz = UTCPlus2()
        naive = datetime(1999, 12, 31, 22)
        aware = datetime(2000, 1, 1, tzinfo=utc2tz)
        self.db.collection.insert_one({'date_aware': aware, 'date_naive': naive})
        self.assert_document_count(1)
        # Given both date are equivalent, we can mix them
        self.db.collection.update_one(
            {'date_aware': naive, 'date_naive': aware},
            {'$set': {'new_aware': aware, 'new_naive': naive}},
            upsert=True,
        )
        self.assert_document_count(1)
        self.db.collection.find_one({'new_aware': naive, 'new_naive': aware})
        self.db.collection.delete_one({'new_aware': naive, 'new_naive': aware})
        self.assert_document_count(0)

    def test__configure_client_tz_aware(self):
        for tz_awarness in (True, False):
            client = mongomock.MongoClient(tz_aware=tz_awarness)
            db = client['somedb']

            utc2tz = UTCPlus2()
            naive = datetime(2000, 1, 1, 2, 0, 0)
            aware = datetime(2000, 1, 1, 4, 0, 0, tzinfo=utc2tz)
            if tz_awarness:
                returned = datetime(2000, 1, 1, 2, 0, 0, tzinfo=helpers.utc)
            else:
                returned = datetime(2000, 1, 1, 2, 0, 0)
            objid = db.collection.insert_one({'date_aware': aware, 'date_naive': naive}).inserted_id

            objs = list(db.collection.find())
            self.assertEqual(objs, [{'_id': objid, 'date_aware': returned, 'date_naive': returned}])

            if tz_awarness:
                self.assertEqual('UTC', returned.tzinfo.tzname(returned))
                self.assertEqual(timedelta(0), returned.tzinfo.utcoffset(returned))
                self.assertEqual(timedelta(0), returned.tzinfo.dst(returned))
                self.assertEqual((timedelta(0), 'UTC'), returned.tzinfo.__getinitargs__())

            # Given both date are equivalent, we can mix them
            db.collection.update_one(
                {'date_aware': naive, 'date_naive': aware},
                {'$set': {'new_aware': aware, 'new_naive': naive}},
                upsert=True,
            )

            objs = list(db.collection.find())
            self.assertEqual(
                objs,
                [
                    {
                        '_id': objid,
                        'date_aware': returned,
                        'date_naive': returned,
                        'new_aware': returned,
                        'new_naive': returned,
                    }
                ],
                msg=tz_awarness,
            )

            ret = db.collection.find_one({'new_aware': naive, 'new_naive': aware})
            self.assertEqual(ret, objs[0], msg=tz_awarness)

            num = db.collection.count_documents({'date_naive': {'$gte': aware}})
            self.assertEqual(1, num, msg=tz_awarness)

            objs = list(db.collection.aggregate([{'$match': {'date_naive': {'$gte': aware}}}]))
            self.assertEqual(1, len(objs), msg=tz_awarness)

            db.collection.delete_one({'new_aware': naive, 'new_naive': naive})
            objs = list(db.collection.find())
            self.assertFalse(objs, msg=tz_awarness)

    def test__list_of_dates(self):
        client = mongomock.MongoClient(tz_aware=True)
        client.db.collection.insert_one({'dates': [datetime.now(), datetime.now()]})
        dates = client.db.collection.find_one()['dates']
        self.assertTrue(dates[0].tzinfo)
        self.assertEqual(dates[0].tzinfo, dates[1].tzinfo)

    @skipIf(helpers.HAVE_PYMONGO, 'pymongo installed')
    def test__current_date_timestamp_requires_pymongo(self):
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_one(
                {},
                {
                    '$currentDate': {
                        'updated_at': {'$type': 'timestamp'},
                        'updated_again': {'$type': 'timestamp'},
                    }
                },
                upsert=True,
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__current_date_timestamp(self):
        before = datetime.now(tz_util.utc) - timedelta(seconds=1)
        self.db.collection.update_one(
            {},
            {
                '$currentDate': {
                    'updated_at': {'$type': 'timestamp'},
                    'updated_again': {'$type': 'timestamp'},
                }
            },
            upsert=True,
        )
        after = datetime.now(tz_util.utc)

        doc = self.db.collection.find_one()
        self.assertTrue(doc.get('updated_at'))
        self.assertTrue(doc.get('updated_again'))
        self.assertNotEqual(doc['updated_at'], doc['updated_again'])

        self.assertLessEqual(before, doc['updated_at'].as_datetime())
        self.assertLessEqual(doc['updated_at'].as_datetime(), after)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__insert_zero_timestamp(self):
        self.db.collection.drop()
        before = datetime.now(tz_util.utc) - timedelta(seconds=1)
        self.db.collection.insert_one({'zero': Timestamp(0, 0)})
        after = datetime.now(tz_util.utc)

        doc = self.db.collection.find_one()
        self.assertLessEqual(before, doc['zero'].as_datetime())
        self.assertLessEqual(doc['zero'].as_datetime(), after)

    def test__rename_collection(self):
        self.db.collection.insert_one({'_id': 1, 'test_list': [{'data': 'val'}]})
        coll = self.db.collection

        coll.rename('other_name')

        self.assertEqual('collection', coll.name)
        self.assertEqual({'other_name'}, set(self.db.list_collection_names()))
        self.assertNotEqual(coll, self.db.other_name)
        self.assertEqual([], list(coll.find()))
        data_in_db = self.db.other_name.find()
        self.assertEqual([({'_id': 1, 'test_list': [{'data': 'val'}]})], list(data_in_db))

    def test__rename_collection_to_bad_names(self):
        coll = self.db.create_collection('a')
        self.assertRaises(TypeError, coll.rename, ['a'])
        self.assertRaises(mongomock.InvalidName, coll.rename, '.a')
        self.assertRaises(mongomock.InvalidName, coll.rename, '$a')
        self.assertRaises(mongomock.InvalidName, coll.rename, '.a')
        self.assertRaises(mongomock.InvalidName, coll.rename, '$a')

    def test__rename_collection_already_exists(self):
        coll = self.db.create_collection('a')
        self.db.create_collection('c')
        self.assertRaises(mongomock.OperationFailure, coll.rename, 'c')

    def test__rename_collection_drop_target(self):
        coll = self.db.create_collection('a')
        self.db.create_collection('c')
        coll.rename('c', dropTarget=True)
        self.assertEqual({'c'}, set(self.db.list_collection_names()))

    def test__cursor_rewind(self):
        coll = self.db.create_collection('a')
        coll.insert_one({'a': 1})
        coll.insert_one({'a': 2})
        coll.insert_one({'a': 3})

        curs = coll.find().sort('a')
        self.assertEqual(next(curs)['a'], 1)
        self.assertEqual(next(curs)['a'], 2)
        curs.rewind()
        self.assertEqual(next(curs)['a'], 1)
        self.assertEqual(next(curs)['a'], 2)

    def test__cursor_sort(self):
        coll = self.db.create_collection('a')
        coll.insert_many([{'a': 1}, {'a': 3}, {'a': 2}])

        self.assertEqual([1, 2, 3], [doc['a'] for doc in coll.find().sort('a')])
        self.assertEqual([3, 2, 1], [doc['a'] for doc in coll.find().sort('a', -1)])

        self.assertEqual([1, 3, 2], [doc['a'] for doc in coll.find().sort('$natural', 1)])
        self.assertEqual([2, 3, 1], [doc['a'] for doc in coll.find().sort('$natural', -1)])

        with self.assertRaises(NotImplementedError) as err:
            list(coll.find().sort('$text_score'))
        self.assertIn('$text_score', str(err.exception))

        cursor = coll.find()
        with self.assertRaises(ValueError) as err:
            cursor.sort([])
        self.assertIn('empty list', str(err.exception))

    def test__cursor_sort_composed(self):
        coll = self.db.create_collection('a')
        coll.insert_many(
            [
                {'_id': 1, 'a': 1, 'b': 2},
                {'_id': 2, 'a': 1, 'b': 0},
                {'_id': 3, 'a': 2, 'b': 1},
            ]
        )

        self.assertEqual([2, 1, 3], [doc['_id'] for doc in coll.find().sort((('a', 1), ('b', 1)))])
        self.assertEqual([1, 2, 3], [doc['_id'] for doc in coll.find().sort((('a', 1), ('b', -1)))])
        self.assertEqual([2, 3, 1], [doc['_id'] for doc in coll.find().sort((('b', 1), ('a', 1)))])

    def test__cursor_sort_list_of_field_names(self):
        col = self.db.create_collection('sort_list_of_fields')
        col.insert_many(
            [
                {'a': 2, 'b': 2},
                {'a': 1, 'b': 3},
                {'a': 1, 'b': 1},
            ]
        )
        self.assertEqual([1, 3, 2], [doc['b'] for doc in col.find(sort=['a', 'b'])])

    def test__cursor_sort_projection(self):
        col = self.db.col
        col.insert_many([{'a': 1, 'b': 1}, {'a': 3, 'b': 3}, {'a': 2, 'b': 2}])

        self.assertEqual([1, 2, 3], [doc['b'] for doc in col.find().sort('a')])
        self.assertEqual([1, 2, 3], [doc['b'] for doc in col.find(projection=['b']).sort('a')])

    def test__cursor_sort_dicts(self):
        col = self.db.col
        col.insert_many(
            [
                {'_id': 1, 'b': {'value': 1}},
                {'_id': 2, 'b': {'value': 3}},
                {'_id': 3, 'b': {'value': 2}},
            ]
        )

        self.assertEqual([1, 3, 2], [doc['_id'] for doc in col.find().sort('b')])

    def test__cursor_max_time_ms(self):
        col = self.db.col
        col.find().max_time_ms(15)
        col.find().max_time_ms(None)

        with self.assertRaises(TypeError):
            col.find().max_time_ms(3.4)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_insert_one(self):
        operations = [pymongo.InsertOne({'a': 1, 'b': 2})]
        result = self.db.collection.bulk_write(operations)

        self.assert_document_count(1)
        doc = next(self.db.collection.find({}))
        self.assert_document_stored(doc['_id'], {'a': 1, 'b': 2})
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 0,
                'nInserted': 1,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_update_one(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.UpdateOne({'a': 1}, {'$set': {'a': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 2}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 1,
                'nUpserted': 0,
                'nMatched': 1,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

        # Upsert == True
        operations = [pymongo.UpdateOne({'a': 1}, {'$set': {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 1,
                'nMatched': 0,
                'writeErrors': [],
                'writeConcernErrors': [],
                'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_update_many(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1, 'b': 1})
        self.db.collection.insert_one({'a': 1, 'b': 0})
        operations = [pymongo.UpdateMany({'a': 1}, {'$set': {'b': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'b': 2}))
        self.assertEqual(len(docs), 2)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 2,
                'nUpserted': 0,
                'nMatched': 2,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

        # Upsert == True
        operations = [pymongo.UpdateMany({'a': 2}, {'$set': {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 1,
                'nMatched': 0,
                'writeErrors': [],
                'writeConcernErrors': [],
                'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_replace_one(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1, 'b': 0})
        operations = [pymongo.ReplaceOne({'a': 1}, {'a': 2})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 2}))
        self.assertEqual(len(docs), 1)
        doc = docs[0]
        doc_id = doc['_id']
        self.assertEqual(doc, {'_id': doc_id, 'a': 2})
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 1,
                'nUpserted': 0,
                'nMatched': 1,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

        # Upsert == True
        operations = [pymongo.ReplaceOne({'a': 1}, {'a': 3}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 1,
                'nMatched': 0,
                'writeErrors': [],
                'writeConcernErrors': [],
                'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
                'nRemoved': 0,
                'nInserted': 0,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_delete_one(self):
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.DeleteOne({'a': 1})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({}))
        self.assertEqual(len(docs), 0)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 1,
                'nInserted': 0,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_delete_many(self):
        self.db.collection.insert_one({'a': 1})
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.DeleteMany({'a': 1})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({}))
        self.assertEqual(len(docs), 0)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(
            result.bulk_api_result,
            {
                'nModified': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'writeErrors': [],
                'upserted': [],
                'writeConcernErrors': [],
                'nRemoved': 2,
                'nInserted': 0,
            },
        )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_matched_count_no_changes(self):
        self.db.collection.insert_one({'name': 'luke'})
        result = self.db.collection.bulk_write(
            [
                pymongo.ReplaceOne({'name': 'luke'}, {'name': 'luke'}),
            ]
        )
        self.assertEqual(1, result.matched_count)
        self.assertEqual(0, result.modified_count)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_matched_count_replace_multiple_objects(self):
        self.db.collection.insert_one({'name': 'luke'})
        self.db.collection.insert_one({'name': 'anna'})
        result = self.db.collection.bulk_write(
            [
                pymongo.ReplaceOne({'name': 'luke'}, {'name': 'Luke'}),
                pymongo.ReplaceOne({'name': 'anna'}, {'name': 'anna'}),
            ]
        )
        self.assertEqual(2, result.matched_count)
        self.assertEqual(1, result.modified_count)

    def test_find_with_comment(self):
        self.db.collection.insert_one({'_id': 1})
        actual = list(self.db.collection.find({'_id': 1, '$comment': 'test'}))
        self.assertEqual([{'_id': 1}], actual)

    def test__find_comment_param(self):
        col = self.db.col
        col.find({}, comment='this_query')

    def test__find_hint_param(self):
        col = self.db.col
        col.find({}, hint='this_index')

    def test__find_one_comment_param(self):
        col = self.db.col
        col.find_one({}, comment='this_query')

    def test__find_one_hint_param(self):
        col = self.db.col
        col.find_one({}, hint='this_index')

    def test__find_one_and_update_comment_param(self):
        col = self.db.col
        col.find_one_and_update({}, {'$set': {'final': True}}, comment='this_query')

    def test__find_one_and_update_hint_param(self):
        col = self.db.col
        col.find_one_and_update({}, {'$set': {'final': True}}, hint='this_index')

    def test__count_documents_comment_param(self):
        col = self.db.col
        col.count_documents({}, comment='this_query')

    def test__count_documents_hint_param(self):
        col = self.db.col
        col.count_documents({}, hint='this_index')

    def test__estimated_document_count_comment_param(self):
        col = self.db.col
        col.estimated_document_count(comment='this_query')

    def test__distinct_comment_param(self):
        col = self.db.col
        col.distinct('a', comment='this_query')

    def test__distinct_hint_param(self):
        col = self.db.col
        col.distinct('a', hint='this_index')

    def test__find_one_and_replace_comment_param(self):
        col = self.db.col
        col.find_one_and_replace({}, {'a': 'b'}, comment='this_query')

    def test__find_one_and_replace_hint_param(self):
        col = self.db.col
        col.find_one_and_replace({}, {'a': 'b'}, hint='this_index')

    def test__insert_one_comment_param(self):
        col = self.db.col
        col.insert_one({'a': 1}, comment='this_query')

    def test__delete_one_comment_param(self):
        col = self.db.col
        col.delete_one({'a': 1}, comment='this_query')

    def test__update_one_comment_param(self):
        col = self.db.col
        col.update_one({'a': 1}, {'$set': {'b': 1}}, comment='this_query')

    def test__bulk_write_comment_param(self):
        col = self.db.col
        requests = [pymongo.InsertOne({'a': 1})]
        col.bulk_write(requests, comment='this_query')

    def test__find_with_expr(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': [5]},
                {'_id': 2, 'a': [1, 2, 3]},
                {'_id': 3, 'a': []},
            ]
        )
        actual = list(self.db.collection.find({'$expr': {'$eq': [{'$size': ['$a']}, 1]}}))
        self.assertEqual([{'_id': 1, 'a': [5]}], actual)

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.insert_one({'_id': 4})
            list(self.db.collection.find({'$expr': {'$eq': [{'$size': ['$a']}, 1]}}))

    def test__find_or_and(self):
        self.db.collection.insert_many(
            [
                {'x': 1, 'y': 1},
                {'x': 2, 'y': 2},
            ]
        )
        search_filter = collections.OrderedDict(
            [
                ('$or', [{'x': 1}, {'x': 2}]),
                ('y', 2),
            ]
        )
        self.assertEqual([2], [d['x'] for d in self.db.collection.find(search_filter)])

    def test__aggregate_fill(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}, 'in_farm': False},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}, 'in_farm': True},
                {'_id': 3, 'pets': {'cats': 4}},
            ]
        )
        actual = self.db.a.aggregate([{'$fill': {'output': {'in_farm': {'value': False}}}}])
        self.assertListEqual(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}, 'in_farm': False},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}, 'in_farm': True},
                {'_id': 3, 'pets': {'cats': 4}, 'in_farm': False},
            ],
            list(actual),
        )

    def test__aggregate_fill_locf(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'x': 1, 'y': None},
                {'_id': 2, 'x': 2, 'y': 5},
                {'_id': 3, 'x': 3, 'y': None},
                {'_id': 4, 'x': 4, 'y': 10},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$fill': {
                        'sortBy': {'x': 1},
                        'output': {'y': {'method': 'locf'}},
                    }
                },
            ]
        )
        self.assertListEqual(
            [None, 5, 5, 10],
            [d['y'] for d in actual],
        )

    def test__aggregate_fill_linear(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'x': 1, 'y': None},
                {'_id': 2, 'x': 2, 'y': 10},
                {'_id': 3, 'x': 3, 'y': None},
                {'_id': 4, 'x': 4, 'y': 20},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$fill': {
                        'sortBy': {'x': 1},
                        'output': {'y': {'method': 'linear'}},
                    }
                },
            ]
        )
        self.assertListEqual(
            [None, 10, 15, 20],
            [d['y'] for d in actual],
        )

    def test__aggregate_fill_partition_by_fields(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'g': 1, 'o': None},
                {'_id': 2, 'g': 1, 'o': 5},
                {'_id': 3, 'g': 1, 'o': None},
                {'_id': 4, 'g': 2, 'o': None},
                {'_id': 5, 'g': 2, 'o': None},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$fill': {
                        'sortBy': {'_id': 1},
                        'output': {'o': {'method': 'locf'}},
                        'partitionByFields': ['g'],
                    }
                },
            ]
        )
        self.assertListEqual(
            [None, 5, 5, None, None],
            [d['o'] for d in actual],
        )

    def test__aggregate_fill_multiple_fields(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'a': None, 'b': None},
                {'_id': 2, 'a': 10, 'b': 100},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$fill': {
                        'output': {
                            'a': {'method': 'locf'},
                            'b': {'value': 0},
                        },
                    }
                },
            ]
        )
        self.assertListEqual(
            [{'_id': 1, 'a': None, 'b': 0}, {'_id': 2, 'a': 10, 'b': 100}],
            list(actual),
        )

    def test__aggregate_fill_value_and_missing(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}, 'in_farm': False, 'owner': 'alice'},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}, 'in_farm': True},
                {'_id': 3, 'pets': {'cats': 4}},
                {'_id': 4, 'pets': {'dogs': 1}},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$fill': {
                        'output': {
                            'in_farm': {'value': False},
                            'owner': {'value': 'unknown'},
                        },
                    }
                },
            ]
        )
        self.assertListEqual(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}, 'in_farm': False, 'owner': 'alice'},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}, 'in_farm': True, 'owner': 'unknown'},
                {'_id': 3, 'pets': {'cats': 4}, 'in_farm': False, 'owner': 'unknown'},
                {'_id': 4, 'pets': {'dogs': 1}, 'in_farm': False, 'owner': 'unknown'},
            ],
            list(actual),
        )

    def test__aggregate_replace_root(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
            ]
        )
        actual = self.db.a.aggregate([{'$replaceRoot': {'newRoot': '$pets'}}])
        self.assertListEqual([{'dogs': 2, 'cats': 3}, {'hamsters': 3, 'cats': 4}], list(actual))

    def test__aggregate_replace_root_use_dots(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': {'male': 1}}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': {'female': 5}}},
            ]
        )
        actual = self.db.a.aggregate([{'$replaceRoot': {'newRoot': '$pets.cats'}}])
        self.assertListEqual([{'male': 1}, {'female': 5}], list(actual))

    def test__aggregate_replace_root_non_existing(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
            ]
        )
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([{'$replaceRoot': {'newRoot': '$not_here'}}])
        self.assertIn('expression', str(err.exception))

    def test__aggregate_replace_root_missing_in_expr(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
                {'_id': 3, 'pets': {'cats': 5}},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$replaceRoot': {
                        'newRoot': {'dogs': '$pets.dogs', 'hamsters': '$pets.hamsters'},
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {'dogs': 2},
                {'hamsters': 3},
                {},
            ],
            list(actual),
        )

    def test__aggregate_replace_root_static(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
            ]
        )
        actual = self.db.a.aggregate([{'$replaceRoot': {'newRoot': {'document': 'new'}}}])
        self.assertListEqual([{'document': 'new'}, {'document': 'new'}], list(actual))

    def test__aggregate_replace_root_expression(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'first_name': 'Gary', 'last_name': 'Sheffield', 'city': 'New York'},
                {'_id': 2, 'first_name': 'Nancy', 'last_name': 'Walker', 'city': 'Anaheim'},
                {'_id': 3, 'first_name': 'Peter', 'last_name': 'Sumner', 'city': 'Toledo'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$replaceRoot': {
                        'newRoot': {'full_name': {'$concat': ['$first_name', ' ', '$last_name']}},
                    }
                }
            ]
        )
        self.assertListEqual(
            [
                {'full_name': 'Gary Sheffield'},
                {'full_name': 'Nancy Walker'},
                {'full_name': 'Peter Sumner'},
            ],
            list(actual),
        )

    def test__aggregate_replace_root_with_array(self):
        self.db.a.insert_many(
            [
                {
                    '_id': 1,
                    'name': 'Susan',
                    'phones': [{'cell': '555-653-6527'}, {'home': '555-965-2454'}],
                },
                {
                    '_id': 2,
                    'name': 'Mark',
                    'phones': [{'cell': '555-445-8767'}, {'home': '555-322-2774'}],
                },
            ]
        )
        actual = self.db.a.aggregate(
            [
                {'$unwind': '$phones'},
                {'$match': {'phones.cell': {'$exists': True}}},
                {'$replaceRoot': {'newRoot': '$phones'}},
            ]
        )
        self.assertListEqual([{'cell': '555-653-6527'}, {'cell': '555-445-8767'}], list(actual))

    def test__aggregate_replace_root_wrong_options(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
            ]
        )
        with self.assertRaises(mongomock.OperationFailure):
            self.db.a.aggregate([{'$replaceRoot': {'new_root': '$pets'}}])

    def test__aggregate_replace_with(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'pets': {'dogs': 2, 'cats': 3}},
                {'_id': 2, 'pets': {'hamsters': 3, 'cats': 4}},
            ]
        )
        actual = self.db.a.aggregate([{'$replaceWith': '$pets'}])
        self.assertListEqual([{'dogs': 2, 'cats': 3}, {'hamsters': 3, 'cats': 4}], list(actual))

    def test__aggregate_merge(self):
        self.db.source.insert_many(
            [
                {'_id': 1, 'sku': 'abc', 'qty': 5, 'color': 'red'},
                {'_id': 2, 'sku': 'def', 'qty': 3, 'color': 'blue'},
            ]
        )
        self.db.target.insert_one({'_id': 100, 'sku': 'abc', 'qty': 1, 'keep': True})

        actual = self.db.source.aggregate(
            [
                {
                    '$merge': {
                        'into': {'coll': 'target'},
                        'on': 'sku',
                    }
                }
            ]
        )

        self.assertListEqual(
            [
                {'_id': 1, 'sku': 'abc', 'qty': 5, 'color': 'red'},
                {'_id': 2, 'sku': 'def', 'qty': 3, 'color': 'blue'},
            ],
            list(actual),
        )
        self.assertListEqual(
            [
                {'_id': 100, 'sku': 'abc', 'qty': 5, 'keep': True, 'color': 'red'},
                {'_id': 2, 'sku': 'def', 'qty': 3, 'color': 'blue'},
            ],
            list(self.db.target.find().sort('sku')),
        )

    def test__aggregate_merge_replace(self):
        self.db.source.insert_one({'_id': 1, 'sku': 'abc', 'qty': 5})
        self.db.target.insert_one({'_id': 100, 'sku': 'abc', 'qty': 1, 'keep': True})

        list(
            self.db.source.aggregate(
                [
                    {
                        '$merge': {
                            'into': 'target',
                            'on': 'sku',
                            'whenMatched': 'replace',
                        }
                    }
                ]
            )
        )

        self.assertEqual(
            {'_id': 100, 'sku': 'abc', 'qty': 5}, self.db.target.find_one({'sku': 'abc'})
        )

    def test__aggregate_merge_pipeline_not_implemented(self):
        self.db.source.insert_one({'_id': 1, 'sku': 'abc', 'qty': 5})
        with self.assertRaises(NotImplementedError):
            list(
                self.db.source.aggregate(
                    [
                        {
                            '$merge': {
                                'into': 'target',
                                'on': 'sku',
                                'whenMatched': 'pipeline',
                            }
                        }
                    ]
                )
            )

    def test__aggregate_lookup(self):
        self.db.a.insert_one({'_id': 1, 'arr': [2, 4]})
        self.db.b.insert_many(
            [
                {'_id': 2, 'should': 'include'},
                {'_id': 3, 'should': 'skip'},
                {'_id': 4, 'should': 'include'},
            ]
        )
        actual = self.db.a.aggregate(
            [{'$lookup': {'from': 'b', 'localField': 'arr', 'foreignField': '_id', 'as': 'b'}}]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'arr': [2, 4],
                    'b': [{'_id': 2, 'should': 'include'}, {'_id': 4, 'should': 'include'}],
                }
            ],
            list(actual),
        )

    def test__aggregate_lookup_reverse(self):
        self.db.a.insert_many([{'_id': 1}, {'_id': 2}, {'_id': 3}])
        self.db.b.insert_one({'_id': 4, 'arr': [1, 3]})
        actual = self.db.a.aggregate(
            [{'$lookup': {'from': 'b', 'localField': '_id', 'foreignField': 'arr', 'as': 'b'}}]
        )
        self.assertEqual(
            [
                {'_id': 1, 'b': [{'_id': 4, 'arr': [1, 3]}]},
                {'_id': 2, 'b': []},
                {'_id': 3, 'b': [{'_id': 4, 'arr': [1, 3]}]},
            ],
            list(actual),
        )

    def test__aggregate_lookup_missing_operator(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [{'$lookup': {'localField': '_id', 'foreignField': 'arr', 'as': 'b'}}]
            )
        self.assertEqual("Must specify 'from' field for a $lookup", str(err.exception))

    def test__aggregate_lookup_operator_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [{'$lookup': {'from': 'b', 'localField': 1, 'foreignField': 'arr', 'as': 'b'}}]
            )
        self.assertEqual('Arguments to $lookup must be strings', str(err.exception))

    def test__aggregate_lookup_dot_in_local_field(self):
        self.db.a.insert_many(
            [
                {'_id': 2, 'should': {'do': 'join'}},
                {'_id': 3, 'should': {'do': 'not_join'}},
                {'_id': 4, 'should': 'skip'},
            ]
        )
        self.db.b.insert_many(
            [
                {'_id': 2, 'should': 'join'},
                {'_id': 3, 'should': 'join'},
                {'_id': 4, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$lookup': {
                        'from': 'b',
                        'localField': 'should.do',
                        'foreignField': 'should',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 2,
                    'b': [{'_id': 2, 'should': 'join'}, {'_id': 3, 'should': 'join'}],
                    'should': {'do': 'join'},
                },
                {'_id': 3, 'b': [], 'should': {'do': 'not_join'}},
                {'_id': 4, 'b': [], 'should': 'skip'},
            ],
            list(actual),
        )

    def test__aggregate_lookup_dot_in_as(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate(
                [
                    {
                        '$lookup': {
                            'from': 'b',
                            'localField': '_id',
                            'foreignField': 'arr',
                            'as': 'should.fail',
                        }
                    }
                ]
            )
        self.assertIn("Although '.' is valid in the 'as' parameters ", str(err.exception))

    def test__aggregate_lookup_pipeline(self):
        self.db.orders.insert_many(
            [
                {'_id': 1, 'item': 'almonds', 'price': 12, 'ordered': 2},
                {'_id': 2, 'item': 'pecans', 'price': 20, 'ordered': 1},
                {'_id': 3, 'item': 'cookies', 'price': 10, 'ordered': 60},
            ]
        )
        self.db.warehouses.insert_many(
            [
                {'_id': 1, 'stock_item': 'almonds', 'warehouse': 'A', 'instock': 120},
                {'_id': 2, 'stock_item': 'pecans', 'warehouse': 'A', 'instock': 80},
                {'_id': 3, 'stock_item': 'almonds', 'warehouse': 'B', 'instock': 60},
                {'_id': 4, 'stock_item': 'cookies', 'warehouse': 'B', 'instock': 40},
                {'_id': 5, 'stock_item': 'cookies', 'warehouse': 'A', 'instock': 80},
            ]
        )
        pipeline = [
            {
                '$lookup': {
                    'from': 'warehouses',
                    'let': {'order_item': '$item', 'order_qty': '$ordered'},
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$stock_item', '$$order_item']},
                                        {'$gte': ['$instock', '$$order_qty']},
                                    ]
                                }
                            }
                        },
                        {'$project': {'stock_item': 0, '_id': 0}},
                    ],
                    'as': 'stockdata',
                }
            }
        ]
        actual = list(self.db.orders.aggregate(pipeline))
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 'almonds',
                    'price': 12,
                    'ordered': 2,
                    'stockdata': [
                        {'warehouse': 'A', 'instock': 120},
                        {'warehouse': 'B', 'instock': 60},
                    ],
                },
                {
                    '_id': 2,
                    'item': 'pecans',
                    'price': 20,
                    'ordered': 1,
                    'stockdata': [{'warehouse': 'A', 'instock': 80}],
                },
                {
                    '_id': 3,
                    'item': 'cookies',
                    'price': 10,
                    'ordered': 60,
                    'stockdata': [{'warehouse': 'A', 'instock': 80}],
                },
            ],
            actual,
        )

    def test__aggregate_lookup_pipeline_with_concise_correlated_subquery(self):
        self.db.labels.insert_many(
            [
                {'_id': 1, 'nr': 'N080', 'name': 'milk'},
                {'_id': 2, 'nr': 'N102', 'name': 'cookies'},
            ]
        )
        self.db.stock.insert_many(
            [
                {'_id': 1, 'nr': 'N102'},
                {'_id': 2, 'nr': 'N080'},
                {'_id': 3, 'nr': 'N100'},
            ]
        )
        pipeline = [
            {
                '$lookup': {
                    'from': 'labels',
                    'localField': 'nr',
                    'foreignField': 'nr',
                    'pipeline': [
                        {'$project': {'name': 1}},
                    ],
                    'as': 'label',
                }
            },
            {'$addFields': {'label': {'$first': '$label.name'}}},
        ]
        actual = list(self.db.stock.aggregate(pipeline))
        self.assertEqual(
            [
                {'_id': 1, 'nr': 'N102', 'label': 'cookies'},
                {'_id': 2, 'nr': 'N080', 'label': 'milk'},
                {'_id': 3, 'nr': 'N100', 'label': None},
            ],
            actual,
        )

    def test__aggregate_lookup_dbref(self):
        self.db.a.insert_many(
            [
                {'_id': 2},
                {'_id': 3},
                {'_id': 4},
            ]
        )
        self.db.b.insert_one(
            {
                '_id': 1,
                'refs': [
                    DBRef('a', 2),
                    DBRef('a', 4, self.db.name),
                ],
            }
        )
        actual = self.db.b.aggregate(
            [
                {
                    '$lookup': {
                        'from': 'a',
                        'localField': 'refs.$id',
                        'foreignField': '_id',
                        'as': 'related',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'refs': [
                        DBRef('a', 2),
                        DBRef('a', 4, self.db.name),
                    ],
                    'related': [
                        {'_id': 2},
                        {'_id': 4},
                    ],
                }
            ],
            list(actual),
        )

    def test__filter_dbref_id_array_mixed(self):
        self.db.a.insert_one(
            {
                '_id': 1,
                'items': [DBRef('b', 10), 'string', 42],
            }
        )
        docs = list(self.db.a.find({'items.$id': 10}))
        self.assertEqual(1, len(docs))
        self.assertEqual(1, docs[0]['_id'])

    def test__filter_dbref_id_array(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'refs': [DBRef('b', 10), DBRef('b', 20)]},
                {'_id': 2, 'refs': [DBRef('b', 20)]},
                {'_id': 3, 'refs': []},
            ]
        )
        docs = list(self.db.a.find({'refs.$id': 10}))
        self.assertEqual(1, len(docs))
        self.assertEqual(1, docs[0]['_id'])

    def test__filter_dbref_id_single(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'ref': DBRef('b', 10)},
                {'_id': 2, 'ref': DBRef('b', 20)},
            ]
        )
        docs = list(self.db.a.find({'ref.$id': 10}))
        self.assertEqual(1, len(docs))
        self.assertEqual(1, docs[0]['_id'])

    def test__aggregate_graph_lookup_behaves_as_lookup(self):
        self.db.a.insert_one({'_id': 1, 'arr': [2, 4]})
        self.db.b.insert_many(
            [
                {'_id': 2, 'should': 'include'},
                {'_id': 3, 'should': 'skip'},
                {'_id': 4, 'should': 'include'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$arr',
                        'connectFromField': 'should',
                        'connectToField': '_id',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'arr': [2, 4],
                    'b': [{'_id': 2, 'should': 'include'}, {'_id': 4, 'should': 'include'}],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_basic(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'include'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$item',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 2,
                    'b': [
                        {'_id': 2, 'parent': 3, 'should': 'include'},
                        {'_id': 3, 'parent': 4, 'should': 'include'},
                        {'_id': 4, 'should': 'include'},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_expression_start_with(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'include'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': {'$add': [1, 1]},
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 2,
                    'b': [
                        {'_id': 2, 'parent': 3, 'should': 'include'},
                        {'_id': 3, 'parent': 4, 'should': 'include'},
                        {'_id': 4, 'should': 'include'},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_depth_field(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'include'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$item',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'depthField': 'dpth',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 2,
                    'b': [
                        {'_id': 2, 'parent': 3, 'should': 'include', 'dpth': 0},
                        {'_id': 3, 'parent': 4, 'should': 'include', 'dpth': 1},
                        {'_id': 4, 'should': 'include', 'dpth': 2},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_multiple_connections(self):
        self.db.a.insert_one({'_id': 1, 'parent_name': 'b'})
        self.db.b.insert_many(
            [
                {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include'},
                {'_id': 3, 'name': 'b', 'should': 'skip'},
                {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include'},
                {'_id': 5, 'name': 'd', 'parent': 'c', 'should': 'include'},
                {'_id': 6, 'name': 'e', 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$parent_name',
                        'connectFromField': 'name',
                        'connectToField': 'parent',
                        'depthField': 'dpth',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'parent_name': 'b',
                    'b': [
                        {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include', 'dpth': 0},
                        {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include', 'dpth': 0},
                        {'_id': 5, 'name': 'd', 'parent': 'c', 'should': 'include', 'dpth': 1},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_cyclic_pointers(self):
        self.db.a.insert_one({'_id': 1, 'parent_name': 'b'})
        self.db.b.insert_many(
            [
                {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include'},
                {'_id': 3, 'name': 'b', 'parent': 'a', 'should': 'include'},
                {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include'},
                {'_id': 5, 'name': 'd', 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$parent_name',
                        'connectFromField': 'name',
                        'connectToField': 'parent',
                        'depthField': 'dpth',
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'parent_name': 'b',
                    'b': [
                        {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include', 'dpth': 0},
                        {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include', 'dpth': 0},
                        {'_id': 3, 'name': 'b', 'parent': 'a', 'should': 'include', 'dpth': 1},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_restrict_search(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'skip'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$item',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'restrictSearchWithMatch': {'should': 'include'},
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 2,
                    'b': [
                        {'_id': 2, 'parent': 3, 'should': 'include'},
                        {'_id': 3, 'parent': 4, 'should': 'include'},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_max_depth(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'skip'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$item',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'maxDepth': 1,
                        'as': 'b',
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'item': 2,
                    'b': [
                        {'_id': 2, 'parent': 3, 'should': 'include'},
                        {'_id': 3, 'parent': 4, 'should': 'include'},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_graph_lookup_max_depth_0(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'skip'},
                {'_id': 5, 'should': 'skip'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$item',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'maxDepth': 0,
                        'as': 'b',
                    }
                }
            ]
        )
        lookup_res = self.db.a.aggregate(
            [{'$lookup': {'from': 'b', 'localField': 'item', 'foreignField': '_id', 'as': 'b'}}]
        )
        self.assertEqual(list(lookup_res), list(actual))

    def test__aggregate_graph_lookup_from_array(self):
        self.db.a.insert_one({'_id': 1, 'items': [2, 8]})
        self.db.b.insert_many(
            [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'include'},
                {'_id': 5, 'should': 'skip'},
                {'_id': 6, 'should': 'include'},
                {'_id': 7, 'should': 'skip'},
                {'_id': 8, 'parent': 6, 'should': 'include'},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$graphLookup': {
                        'from': 'b',
                        'startWith': '$items',
                        'connectFromField': 'parent',
                        'connectToField': '_id',
                        'as': 'b',
                    }
                }
            ]
        )
        expected_list = [
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'include'},
            {'_id': 6, 'should': 'include'},
            {'_id': 8, 'parent': 6, 'should': 'include'},
        ]
        result_list = next(iter(actual))['b']

        def sorter(doc):
            return doc['_id']

        self.assertEqual(len(expected_list), len(result_list))
        self.assertEqual(sorted(expected_list, key=sorter), sorted(result_list, key=sorter))

    def test_aggregate_graph_lookup_basic_connect_from(self):
        """TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

        * This testcase has a simple connect from field without the dot operator.

        * The test case is taken from
          https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

        * The inputs and the query are copy/pasted directly from the link
          above.

        * The expected output is formatted to match the pprint'ed output
          produced by mongomock.

        * The elements are:

             - data_a: documents for database a
             - data_b: documents for database b
             - query: query for database b
             - expected: result expected from query execution
        """

        data_a = [
            {'_id': 0, 'airport': 'JFK', 'connects': ['BOS', 'ORD']},
            {'_id': 1, 'airport': 'BOS', 'connects': ['JFK', 'PWM']},
            {'_id': 2, 'airport': 'ORD', 'connects': ['JFK']},
            {'_id': 3, 'airport': 'PWM', 'connects': ['BOS', 'LHR']},
            {'_id': 4, 'airport': 'LHR', 'connects': ['PWM']},
        ]

        data_b = [
            {'_id': 1, 'name': 'Dev', 'nearestAirport': 'JFK'},
            {'_id': 2, 'name': 'Eliot', 'nearestAirport': 'JFK'},
            {'_id': 3, 'name': 'Jeff', 'nearestAirport': 'BOS'},
        ]

        query = [
            {
                '$graphLookup': {
                    'from': 'a',
                    'startWith': '$nearestAirport',
                    'connectFromField': 'connects',
                    'connectToField': 'airport',
                    'maxDepth': 2,
                    'depthField': 'numConnections',
                    'as': 'destinations',
                }
            }
        ]

        ordered_dict = collections.OrderedDict
        expected = [
            {
                '_id': 1,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            ('connects', ['BOS', 'ORD']),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            ('connects', ['JFK', 'PWM']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', ['JFK']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            ('connects', ['BOS', 'LHR']),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Dev',
                'nearestAirport': 'JFK',
            },
            {
                '_id': 2,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            ('connects', ['BOS', 'ORD']),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            ('connects', ['JFK', 'PWM']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', ['JFK']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            ('connects', ['BOS', 'LHR']),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Eliot',
                'nearestAirport': 'JFK',
            },
            {
                '_id': 3,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            ('connects', ['JFK', 'PWM']),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            ('connects', ['BOS', 'ORD']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            ('connects', ['BOS', 'LHR']),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', ['JFK']),
                            ('numConnections', 2),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 4),
                            ('airport', 'LHR'),
                            ('connects', ['PWM']),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Jeff',
                'nearestAirport': 'BOS',
            },
        ]

        self.db.a.insert_many(data_a)
        self.db.b.insert_many(data_b)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        # the diff between expected and actual should be empty
        res = diff(expected, actual)
        self.assertEqual(res, [])

    def test_aggregate_graph_lookup_nested_array(self):
        """TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

        * This test cases connectfrom x.y where x is an array.

        * The test case is adaptaed from
          https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

        * The input is modified wrap a dictionary around the list of cities in
        * And query is modified accordingly.
        * The expected output is formatted to match the pprint'ed output
          produced by mongomock.

        * The elements are:

             - data_a: documents for database a
             - data_b: documents for database b
             - query: query for database b
             - expected: result expected from query execution
        """

        data_a = [
            {
                '_id': 0,
                'airport': 'JFK',
                'connects': [{'to': 'BOS', 'distance': 200}, {'to': 'ORD', 'distance': 800}],
            },
            {
                '_id': 1,
                'airport': 'BOS',
                'connects': [{'to': 'JFK', 'distance': 200}, {'to': 'PWM', 'distance': 2000}],
            },
            {'_id': 2, 'airport': 'ORD', 'connects': [{'to': 'JFK', 'distance': 800}]},
            {
                '_id': 3,
                'airport': 'PWM',
                'connects': [{'to': 'BOS', 'distance': 2000}, {'to': 'LHR', 'distance': 6000}],
            },
            {'_id': 4, 'airport': 'LHR', 'connects': [{'to': 'PWM', 'distance': 6000}]},
        ]

        data_b = [
            {'_id': 1, 'name': 'Dev', 'nearestAirport': 'JFK'},
            {'_id': 2, 'name': 'Eliot', 'nearestAirport': 'JFK'},
            {'_id': 3, 'name': 'Jeff', 'nearestAirport': 'BOS'},
        ]

        query = [
            {
                '$graphLookup': {
                    'from': 'a',
                    'startWith': '$nearestAirport',
                    'connectFromField': 'connects.to',
                    'connectToField': 'airport',
                    'maxDepth': 2,
                    'depthField': 'numConnections',
                    'as': 'destinations',
                }
            }
        ]

        ordered_dict = collections.OrderedDict
        expected = [
            {
                '_id': 1,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'BOS'}, {'distance': 800, 'to': 'ORD'}],
                            ),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'JFK'}, {'distance': 2000, 'to': 'PWM'}],
                            ),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', [{'distance': 800, 'to': 'JFK'}]),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            (
                                'connects',
                                [{'distance': 2000, 'to': 'BOS'}, {'distance': 6000, 'to': 'LHR'}],
                            ),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Dev',
                'nearestAirport': 'JFK',
            },
            {
                '_id': 2,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'BOS'}, {'distance': 800, 'to': 'ORD'}],
                            ),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'JFK'}, {'distance': 2000, 'to': 'PWM'}],
                            ),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', [{'distance': 800, 'to': 'JFK'}]),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            (
                                'connects',
                                [{'distance': 2000, 'to': 'BOS'}, {'distance': 6000, 'to': 'LHR'}],
                            ),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Eliot',
                'nearestAirport': 'JFK',
            },
            {
                '_id': 3,
                'destinations': [
                    ordered_dict(
                        [
                            ('_id', 1),
                            ('airport', 'BOS'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'JFK'}, {'distance': 2000, 'to': 'PWM'}],
                            ),
                            ('numConnections', 0),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 0),
                            ('airport', 'JFK'),
                            (
                                'connects',
                                [{'distance': 200, 'to': 'BOS'}, {'distance': 800, 'to': 'ORD'}],
                            ),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 3),
                            ('airport', 'PWM'),
                            (
                                'connects',
                                [{'distance': 2000, 'to': 'BOS'}, {'distance': 6000, 'to': 'LHR'}],
                            ),
                            ('numConnections', 1),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 2),
                            ('airport', 'ORD'),
                            ('connects', [{'distance': 800, 'to': 'JFK'}]),
                            ('numConnections', 2),
                        ]
                    ),
                    ordered_dict(
                        [
                            ('_id', 4),
                            ('airport', 'LHR'),
                            ('connects', [{'distance': 6000, 'to': 'PWM'}]),
                            ('numConnections', 2),
                        ]
                    ),
                ],
                'name': 'Jeff',
                'nearestAirport': 'BOS',
            },
        ]

        self.db.a.insert_many(data_a)
        self.db.b.insert_many(data_b)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        # the diff between expected and actual should be empty
        res = diff(expected, actual)
        self.assertEqual(res, [])

    def test_aggregate_graph_lookup_connect_from_nested_dict(self):
        """TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

        * This test cases connectfrom x.y where x is a dictionary.

        * The testcase is taken from
              https://stackoverflow.com/questions/40989763/mongodb-graphlookup

        * The inputs and the query are copy/pasted directly from the link
          above (with some cleanup)

        * The expected output is formatted to match the pprint'ed output
          produced by mongomock.

        * The elements are:

             - data_a: documents for database a
             - data_b: documents for database b
             - query: query for database b
             - expected: result expected from query execution
        """

        data_b = [
            {'_id': 1, 'name': 'Dev'},
            {
                '_id': 2,
                'name': 'Eliot',
                'reportsTo': {'name': 'Dev', 'from': '2016-01-01T00:00:00.000Z'},
            },
            {
                '_id': 3,
                'name': 'Ron',
                'reportsTo': {'name': 'Eliot', 'from': '2016-01-01T00:00:00.000Z'},
            },
            {
                '_id': 4,
                'name': 'Andrew',
                'reportsTo': {'name': 'Eliot', 'from': '2016-01-01T00:00:00.000Z'},
            },
            {
                '_id': 5,
                'name': 'Asya',
                'reportsTo': {'name': 'Ron', 'from': '2016-01-01T00:00:00.000Z'},
            },
            {
                '_id': 6,
                'name': 'Dan',
                'reportsTo': {'name': 'Andrew', 'from': '2016-01-01T00:00:00.000Z'},
            },
        ]

        data_a = [{'_id': 1, 'name': 'x'}]

        query = [
            {
                '$graphLookup': {
                    'from': 'b',
                    'startWith': '$name',
                    'connectFromField': 'reportsTo.name',
                    'connectToField': 'name',
                    'as': 'reportingHierarchy',
                }
            }
        ]

        expected = [
            {'_id': 1, 'name': 'Dev', 'reportingHierarchy': [{'_id': 1, 'name': 'Dev'}]},
            {
                '_id': 2,
                'name': 'Eliot',
                'reportingHierarchy': [
                    {
                        '_id': 2,
                        'name': 'Eliot',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
                    },
                    {'_id': 1, 'name': 'Dev'},
                ],
                'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
            },
            {
                '_id': 3,
                'name': 'Ron',
                'reportingHierarchy': [
                    {
                        '_id': 3,
                        'name': 'Ron',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
                    },
                    {
                        '_id': 2,
                        'name': 'Eliot',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
                    },
                    {'_id': 1, 'name': 'Dev'},
                ],
                'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
            },
            {
                '_id': 4,
                'name': 'Andrew',
                'reportingHierarchy': [
                    {
                        '_id': 4,
                        'name': 'Andrew',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
                    },
                    {
                        '_id': 2,
                        'name': 'Eliot',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
                    },
                    {'_id': 1, 'name': 'Dev'},
                ],
                'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
            },
            {
                '_id': 5,
                'name': 'Asya',
                'reportingHierarchy': [
                    {
                        '_id': 5,
                        'name': 'Asya',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Ron'},
                    },
                    {
                        '_id': 3,
                        'name': 'Ron',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
                    },
                    {
                        '_id': 2,
                        'name': 'Eliot',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
                    },
                    {'_id': 1, 'name': 'Dev'},
                ],
                'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Ron'},
            },
            {
                '_id': 6,
                'name': 'Dan',
                'reportingHierarchy': [
                    {
                        '_id': 6,
                        'name': 'Dan',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Andrew'},
                    },
                    {
                        '_id': 4,
                        'name': 'Andrew',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Eliot'},
                    },
                    {
                        '_id': 2,
                        'name': 'Eliot',
                        'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Dev'},
                    },
                    {'_id': 1, 'name': 'Dev'},
                ],
                'reportsTo': {'from': '2016-01-01T00:00:00.000Z', 'name': 'Andrew'},
            },
        ]

        self.db.a.insert_many(data_a)
        self.db.b.insert_many(data_b)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        # the diff between expected and actual should be empty
        res = diff(expected, actual)
        self.assertEqual(res, [])

    def test__aggregate_graph_lookup_missing_operator(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 'arr',
                            'as': 'b',
                        }
                    }
                ]
            )
        self.assertEqual(
            "Must specify 'connectToField' field for a $graphLookup", str(err.exception)
        )

    def test__aggregate_graphlookup_operator_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 1,
                            'connectToField': '_id',
                            'as': 'b',
                        }
                    }
                ]
            )
        self.assertEqual(
            "Argument 'connectFromField' to $graphLookup must be string", str(err.exception)
        )

    def test__aggregate_graph_lookup_restrict_not_dict(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 'parent',
                            'connectToField': '_id',
                            'restrictSearchWithMatch': 3,
                            'as': 'b',
                        }
                    }
                ]
            )
        self.assertEqual(
            "Argument 'restrictSearchWithMatch' to $graphLookup must be a Dictionary",
            str(err.exception),
        )

    def test__aggregate_graph_lookup_max_depth_not_number(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 'parent',
                            'connectToField': '_id',
                            'maxDepth': 's',
                            'as': 'b',
                        }
                    }
                ]
            )
        self.assertEqual("Argument 'maxDepth' to $graphLookup must be a number", str(err.exception))

    def test__aggregate_graph_lookup_depth_filed_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 'parent',
                            'connectToField': '_id',
                            'depthField': 4,
                            'as': 'b',
                        }
                    }
                ]
            )
        self.assertEqual(
            "Argument 'depthField' to $graphlookup must be a string", str(err.exception)
        )

    def test__aggregate_graph_lookup_dot_in_as_field(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate(
                [
                    {
                        '$graphLookup': {
                            'from': 'arr',
                            'startWith': '$_id',
                            'connectFromField': 'parent',
                            'connectToField': '_id',
                            'as': 'b.id',
                        }
                    }
                ]
            )
        self.assertIn("Although '.' is valid in the 'as' parameter", str(err.exception))

    def test__aggregate_sample(self):
        self.db.a.insert_many([{'_id': i} for i in range(5)])

        actual = list(self.db.a.aggregate([{'$sample': {'size': 2}}]))
        self.assertEqual(2, len(actual))
        results = {doc.get('_id') for doc in actual}
        self.assertLessEqual(results, {0, 1, 2, 3, 4})
        self.assertLessEqual(2, len(results))

        actual = list(self.db.a.aggregate([{'$sample': {'size': 10}}]))
        self.assertEqual(5, len(actual))
        self.assertEqual({doc.get('_id') for doc in actual}, {0, 1, 2, 3, 4})

    def test__aggregate_empty(self):
        self.db.a.drop()

        actual = list(self.db.a.aggregate([{'$sample': {'size': 1}}]))
        self.assertEqual([], list(actual))

    def test__aggregate_sample_errors(self):
        self.db.a.insert_many([{'_id': i} for i in range(5)])
        # Many cases for '$sample' options that should raise an operation failure.
        cases = (None, 3, {}, {'size': 2, 'otherUnknownOption': 3})
        for case in cases:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.a.aggregate([{'$sample': case}])

    def test__aggregate_count(self):
        self.db.a.insert_many([{'_id': 1, 'a': 1}, {'_id': 2, 'a': 2}, {'_id': 3, 'a': 1}])

        actual = list(self.db.a.aggregate([{'$match': {'a': 1}}, {'$count': 'one_count'}]))
        self.assertEqual([{'one_count': 2}], actual)

    def test__aggregate_count_errors(self):
        self.db.a.insert_many([{'_id': i} for i in range(5)])
        # Many cases for '$count' options that should raise an operation failure.
        cases = (None, 3, {}, [], '', '$one_count', 'one.count')
        for case in cases:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.a.aggregate([{'$count': case}])

    def test__aggregate_facet(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many(
            [
                {
                    '_id': 1,
                    'title': 'The Pillars of Society',
                    'artist': 'Grosz',
                    'year': 1926,
                    'price': 199.99,
                },
                {
                    '_id': 2,
                    'title': 'Melancholy III',
                    'artist': 'Munch',
                    'year': 1902,
                    'price': 200.00,
                },
                {
                    '_id': 3,
                    'title': 'Melancholy III',
                    'artist': 'Munch',
                    'year': 1902,
                    'price': 200.00,
                },
            ]
        )

        actual = collection.aggregate(
            [
                {'$group': {'_id': '$year'}},
                {
                    '$facet': {
                        'grouped_and_limited': [{'$limit': 1}],
                        'groups_count': [{'$count': 'total_count'}],
                        'grouped_and_unlimited': [],
                    }
                },
            ]
        )
        expect = [
            {
                'grouped_and_limited': [{'_id': 1902}],
                'grouped_and_unlimited': [{'_id': 1902}, {'_id': 1926}],
                'groups_count': [{'total_count': 2}],
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_project_array_size(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('_id', False), ('a', {'$size': '$arr'})])},
            ]
        )
        self.assertEqual([{'a': 2}], list(actual))

    def test__aggregate_project_array_size_missing(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            list(
                self.db.collection.aggregate(
                    [
                        {'$match': {'_id': 1}},
                        {
                            '$project': collections.OrderedDict(
                                [('_id', False), ('a', {'$size': '$arr'})]
                            )
                        },
                    ]
                )
            )

    def test__aggregate_project_cond_mongodb_to_bool(self):
        self.db.collection.insert_one({'_id': 1})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        '_id': False,
                        # undefined aka KeyError
                        'undefined_value': {'$cond': ['$not_existing_field', 't', 'f']},
                        'false_value': {'$cond': [False, 't', 'f']},
                        'null_value': {'$cond': [None, 't', 'f']},
                        'zero_value': {'$cond': [0, 't', 'f']},
                        'true_value': {'$cond': [True, 't', 'f']},
                        'one_value': {'$cond': [1, 't', 'f']},
                        'empty_string': {'$cond': ['', 't', 'f']},
                        'empty_list': {'$cond': [[], 't', 'f']},
                        'empty_dict': {'$cond': [{}, 't', 'f']},
                    }
                },
            ]
        )
        expected = {
            'undefined_value': 'f',
            'false_value': 'f',
            'null_value': 'f',
            'zero_value': 'f',
            'true_value': 't',
            'one_value': 't',
            'empty_string': 't',
            'empty_list': 't',
            'empty_dict': 't',
        }
        self.assertEqual([expected], list(actual))

    def test__aggregate_project_array_size_if_null(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        self.db.collection.insert_one({'_id': 2})
        self.db.collection.insert_one({'_id': 3, 'arr': None})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': collections.OrderedDict(
                        [('_id', False), ('a', {'$size': {'$ifNull': ['$arr', []]}})]
                    )
                }
            ]
        )
        self.assertEqual([{'a': 2}, {'a': 0}, {'a': 0}], list(actual))

    def test__aggregate_project_if_null(self):
        self.db.collection.insert_one({'_id': 1, 'elem_a': '<present_a>'})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {
                    '$project': collections.OrderedDict(
                        [
                            ('_id', False),
                            ('a', {'$ifNull': ['$elem_a', '<missing_a>']}),
                            ('b', {'$ifNull': ['$elem_b', '<missing_b>']}),
                        ]
                    )
                },
            ]
        )
        self.assertEqual([{'a': '<present_a>', 'b': '<missing_b>'}], list(actual))

    @skipIf(
        version.parse('4.4') < SERVER_VERSION,
        'multiple input expressions in $ifNull are not supported in MongoDB v4.4 and earlier',
    )
    def test__aggregate_project_if_null_multi_field_not_supported(self):
        self.db.collection.insert_one({'_id': 1, 'elem_a': '<present_a>'})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(
                [
                    {'$match': {'_id': 1}},
                    {
                        '$project': collections.OrderedDict(
                            [
                                ('_id', False),
                                ('a_and_b', {'$ifNull': ['$elem_a', '$elem_b', '<missing_both>']}),
                                ('b_and_a', {'$ifNull': ['$elem_b', '$elem_a', '<missing_both>']}),
                                ('b_and_c', {'$ifNull': ['$elem_b', '$elem_c', '<missing_both>']}),
                            ]
                        )
                    },
                ]
            )

    @skipIf(
        version.parse('4.4') >= SERVER_VERSION,
        'multiple input expressions in $ifNull are not supported in MongoDB v4.4 and earlier',
    )
    def test__aggregate_project_if_null_multi_field(self):
        self.db.collection.insert_one({'_id': 1, 'elem_a': '<present_a>'})
        actual = list(
            self.db.collection.aggregate(
                [
                    {'$match': {'_id': 1}},
                    {
                        '$project': collections.OrderedDict(
                            [
                                ('_id', False),
                                ('a_and_b', {'$ifNull': ['$elem_a', '$elem_b', '<missing_both>']}),
                                ('b_and_a', {'$ifNull': ['$elem_b', '$elem_a', '<missing_both>']}),
                                ('b_and_c', {'$ifNull': ['$elem_b', '$elem_c', '<missing_both>']}),
                            ]
                        )
                    },
                ]
            )
        )
        expected = [
            {'a_and_b': '<present_a>', 'b_and_a': '<present_a>', 'b_and_c': '<missing_both>'}
        ]

        self.assertEqual(expected, list(actual))

    def test__aggregate_project_if_null_expression(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'description': 'Description 1', 'title': 'Title 1'},
                {'_id': 2, 'title': 'Title 2'},
                {'_id': 3, 'description': None, 'title': 'Title 3'},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'full_description': {'$ifNull': ['$description', '$title']},
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {'_id': 1, 'full_description': 'Description 1'},
                {'_id': 2, 'full_description': 'Title 2'},
                {'_id': 3, 'full_description': 'Title 3'},
            ],
            list(actual),
        )

    def test__aggregate_switch(self):
        self.db.collection.insert_one({'_id': 1, 'a': 0})
        # Expressions taken directly from official documentation:
        # https://docs.mongodb.com/manual/reference/operator/aggregation/switch/
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {
                    '$project': {
                        'doc_example_1': {
                            '$switch': {
                                'branches': [
                                    {'case': {'$eq': ['$a', 5]}, 'then': 'equals'},
                                    {'case': {'$gt': ['$a', 5]}, 'then': 'greater than'},
                                    {'case': {'$lt': ['$a', 5]}, 'then': 'less than'},
                                ],
                            }
                        },
                        'doc_example_2': {
                            '$switch': {
                                'branches': [
                                    {'case': {'$eq': ['$a', 5]}, 'then': 'equals'},
                                    {'case': {'$gt': ['$a', 5]}, 'then': 'greater than'},
                                ],
                                'default': 'did not match',
                            }
                        },
                        'doc_example_3': {
                            '$switch': {
                                'branches': [
                                    {'case': 'this is true', 'then': 'first case'},
                                    {'case': False, 'then': 'second case'},
                                ],
                                'default': 'did not match',
                            }
                        },
                        'branches_is_tuple': {
                            '$switch': {
                                'branches': (
                                    {'case': False, 'then': 'value_f'},
                                    {'case': True, 'then': 'value_t'},
                                ),
                            }
                        },
                        'missing_field': {
                            '$switch': {
                                'branches': [
                                    {'case': '$missing_field', 'then': 'first case'},
                                    {'case': True, 'then': '$missing_field'},
                                ],
                                'default': 'did not match',
                            }
                        },
                    }
                },
            ]
        )
        expected = {
            '_id': 1,
            'doc_example_1': 'less than',
            'doc_example_2': 'did not match',
            'doc_example_3': 'first case',
            'branches_is_tuple': 'value_t',
        }
        self.assertEqual([expected], list(actual))

    def test__aggregate_switch_operation_failures(self):
        self.db.collection.insert_one({'_id': 1, 'a': 0})

        tests_cases = [
            (
                {'$switch': []},
                f'$switch requires an object as an argument, found: {type([])}',
            ),
            (
                {'$switch': {}},
                '$switch requires at least one branch.',
            ),
            (
                {'$switch': {'branches': {}}},
                f"$switch expected an array for 'branches', found: {type({})}",
            ),
            (
                {'$switch': {'branches': []}},
                '$switch requires at least one branch.',
            ),
            (
                {'$switch': {'branches': [{}, 7]}},
                "$switch requires each branch have a 'case' expression",
            ),
            (
                {'$switch': {'branches': [{'case': True}, 7]}},
                "$switch requires each branch have a 'then' expression.",
            ),
            (
                {'$switch': {'branches': [{'case': True, 'then': 3}, 7]}},
                f'$switch expected each branch to be an object, found: {int}',
            ),
            (
                {'$switch': {'branches': [7, {}]}},
                f'$switch expected each branch to be an object, found: {int}',
            ),
            (
                {'$switch': {'branches': [{'case': False, 'then': 3}]}},
                '$switch could not find a matching branch for an input, '
                'and no default was specified.',
            ),
        ]

        for switch_operator, expected_exception in tests_cases:
            pipeline = [
                {'$match': {'_id': 1}},
                {'$project': {'result_field': switch_operator}},
            ]
            with self.assertRaises(mongomock.OperationFailure) as err:
                self.db.collection.aggregate(pipeline)
            self.assertEqual(expected_exception, str(err.exception))

    def test__aggregate_switch_mongodb_to_bool(self):
        def build_switch(case):
            return {
                '$switch': {
                    'branches': [
                        {'case': case, 'then': 't'},
                    ],
                    'default': 'f',
                }
            }

        self.db.collection.insert_one({'_id': 1})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        '_id': False,
                        'undefined_value': build_switch('$not_existing_field'),
                        'false_value': build_switch(False),
                        'null_value': build_switch(None),
                        'zero_value': build_switch(0),
                        'true_value': build_switch(True),
                        'one_value': build_switch(1),
                        'empty_string': build_switch(''),
                        'empty_list': build_switch([]),
                        'empty_dict': build_switch({}),
                    }
                },
            ]
        )
        expected = {
            'undefined_value': 'f',
            'false_value': 'f',
            'null_value': 'f',
            'zero_value': 'f',
            'true_value': 't',
            'one_value': 't',
            'empty_string': 't',
            'empty_list': 't',
            'empty_dict': 't',
        }
        self.assertEqual([expected], list(actual))

    def test__aggregate_project_array_element_at(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {
                    '$project': collections.OrderedDict(
                        [('_id', False), ('a', {'$arrayElemAt': ['$arr', 1]})]
                    )
                },
            ]
        )
        self.assertEqual([{'a': 3}], list(actual))

    def test__aggregate_project_first(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('_id', False), ('a', {'$first': '$arr'})])},
            ]
        )
        self.assertEqual([{'a': 2}], list(actual))

    def test__aggregate_project_last(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('_id', False), ('a', {'$last': '$arr'})])},
            ]
        )
        self.assertEqual([{'a': 3}], list(actual))

    def test__aggregate_project_rename__id(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('_id', False), ('rename_id', '$_id')])},
            ]
        )
        self.assertEqual([{'rename_id': 1}], list(actual))

    def test__aggregate_project_rename_dot_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('_id', False), ('rename_dot', '$arr.a')])},
            ]
        )
        self.assertEqual([{'rename_dot': 2}], list(actual))

    def test__aggregate_project_nested_field_extraction(self):
        """Dot notation in $project extracts nested fields."""
        self.db.collection.insert_one({'_id': 1, 'nested': {'field': 'value'}})
        result = list(self.db.collection.aggregate([{'$project': {'out': '$nested.field'}}]))
        self.assertEqual(result[0]['out'], 'value')

    def test__aggregate_project_dot_notation_inclusion(self):
        """Dot notation inclusion in $project."""
        self.db.collection.insert_one({'_id': 1, 'a': {'b': 1, 'c': 2}})
        result = list(self.db.collection.aggregate([{'$project': {'a.b': 1}}]))
        self.assertEqual(result[0], {'_id': 1, 'a': {'b': 1}})

    def test__aggregate_addfields_dot_notation_output(self):
        """Dot notation in $addFields output field name creates nested doc."""
        self.db.collection.insert_one({'_id': 1, 'x': 10})
        result = list(self.db.collection.aggregate([{'$addFields': {'nested.value': '$x'}}]))
        self.assertEqual(result[0]['nested']['value'], 10)

    def test__aggregate_project_deep_nested_path(self):
        """Deep nested dot path in $project expression."""
        self.db.collection.insert_one({'_id': 1, 'a': {'b': {'c': 'deep'}}})
        result = list(self.db.collection.aggregate([{'$project': {'result': '$a.b.c'}}]))
        self.assertEqual(result[0]['result'], 'deep')

    def test__aggregate_addfields_nested_input_path(self):
        """$addFields reading from nested path."""
        self.db.collection.insert_one({'_id': 1, 'info': {'name': 'test', 'value': 42}})
        result = list(
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'fullName': '$info.name',
                            'doubled': {'$multiply': ['$info.value', 2]},
                        }
                    }
                ]
            )
        )
        self.assertEqual(result[0]['fullName'], 'test')
        self.assertEqual(result[0]['doubled'], 84)

    def test__aggregate_project_id(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': 11},
                {'_id': 2, 'a': 12},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {'$project': {'_id': '$a'}},
            ]
        )
        self.assertEqual([{'_id': 11}, {'_id': 12}], list(actual))

    def test__aggregate_project_missing_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {
                    '$project': collections.OrderedDict(
                        [('_id', False), ('rename_dot', '$arr.c'), ('a', '$arr.a')]
                    )
                },
            ]
        )
        self.assertEqual([{'a': 2}], list(actual))

    def test__aggregate_project_missing_nested_fields(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': {'c': 1}})
        actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {
                    '$project': collections.OrderedDict(
                        [('_id', False), ('nested_dictionary', {'c': '$b.c', 'd': '$b.d'})]
                    )
                },
            ]
        )
        self.assertEqual([{'nested_dictionary': {'c': 1}}], list(actual))

    def test__aggregate_project_out(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        old_actual = self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': collections.OrderedDict([('rename_dot', '$arr.a')])},
                {'$out': 'new_collection'},
            ]
        )
        new_collection = self.db.get_collection('new_collection')
        new_actual = list(new_collection.find())
        expect = [{'_id': 1, 'rename_dot': 2}]

        self.assertEqual(expect, new_actual)
        self.assertEqual(expect, list(old_actual))

    def test__aggregate_project_out_no_entries(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        old_actual = self.db.collection.aggregate(
            [{'$match': {'_id': 3}}, {'$out': 'new_collection'}]
        )
        new_collection = self.db.get_collection('new_collection')
        new_actual = list(new_collection.find())
        expect = []

        self.assertEqual(expect, new_actual)
        self.assertEqual(expect, list(old_actual))

    def test__aggregate_project_include_in_exclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate(
                [{'$project': collections.OrderedDict([('a', False), ('b', True)])}]
            )
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_exclude_in_inclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate(
                [{'$project': collections.OrderedDict([('a', True), ('b', False)])}]
            )
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_computed_field_in_exclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate(
                [
                    {'$project': {'a': 0, 'b': '$a'}},
                ]
            )
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_id_can_always_be_excluded(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate(
            [{'$project': collections.OrderedDict([('a', True), ('b', True), ('_id', False)])}]
        )
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

    def test__aggregate_project_inclusion_with_only_id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate([{'$project': {'_id': True}}])
        self.assertEqual([{'_id': 1}], list(actual))

    def test__aggregate_project_exclusion_with_only_id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate([{'$project': {'_id': False}}])
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

        actual = self.db.collection.aggregate([{'$project': {'_id': 0}}])
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

    def test__aggregate_project_subfield(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': {'b': 3}, 'other': 1},
                {'_id': 2, 'a': {'c': 3}},
                {'_id': 3, 'b': {'c': 3}},
                {'_id': 4, 'a': 5},
            ]
        )
        self.assertEqual(
            [
                {'_id': 1, 'a': {'b': 3}},
                {'_id': 2, 'a': {}},
                {'_id': 3},
                {'_id': 4},
            ],
            list(
                self.db.collection.aggregate(
                    [
                        {'$project': {'a.b': 1}},
                    ]
                )
            ),
        )

    def test__aggregate_project_subfield_exclude(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': {'b': 3}, 'other': 1},
                {'_id': 2, 'a': {'c': 3}},
                {'_id': 3, 'b': {'c': 3}},
                {'_id': 4, 'a': 5},
            ]
        )
        self.assertEqual(
            [
                {'_id': 1, 'a': {}, 'other': 1},
                {'_id': 2, 'a': {'c': 3}},
                {'_id': 3, 'b': {'c': 3}},
                {'_id': 4, 'a': 5},
            ],
            list(
                self.db.collection.aggregate(
                    [
                        {'$project': {'a.b': 0}},
                    ]
                )
            ),
        )

    def test__aggregate_project_subfield_conflict(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': {'b': 3}, 'other': 1},
                {'_id': 2, 'a': {'c': 3}},
                {'_id': 3, 'b': {'c': 3}},
            ]
        )
        with self.assertRaises(mongomock.OperationFailure):
            list(
                self.db.collection.aggregate(
                    [
                        {'$project': collections.OrderedDict([('a.b', 1), ('a', 1)])},
                    ]
                )
            )
        with self.assertRaises(mongomock.OperationFailure):
            list(
                self.db.collection.aggregate(
                    [
                        {'$project': collections.OrderedDict([('a', 1), ('a.b', 1)])},
                    ]
                )
            )
        with self.assertRaises(mongomock.OperationFailure):
            list(
                self.db.collection.aggregate(
                    [
                        {'$project': collections.OrderedDict([('d.e.f', 1), ('d.e.f.g', 1)])},
                    ]
                )
            )

    def test__aggregate_project_group_operations(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3, 'c': '$d'})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 1,
                        'max': {'$max': [5, 9, '$a', None]},
                        'min': {'$min': [8, '$a', None, '$b']},
                        'avg': {'$avg': [4, '$a', '$b', 'a', 'b']},
                        'sum': {'$sum': [4, '$a', None, '$b', 'a', 'b', {'$sum': [0, 1, '$b']}]},
                        'maxString': {'$max': [{'$literal': '$b'}, '$c']},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'_id': 1, 'max': 9, 'min': 2, 'avg': 3, 'sum': 13, 'maxString': '$d'}], list(actual)
        )

    def test__aggregate_project_array_subfield(self):
        self.db.collection.insert_many(
            [
                {'a': [{'b': 1, 'c': 2, 'd': 3}], 'e': 4},
                {'a': [{'c': 12, 'd': 13}], 'e': 14},
                {'a': [{'b': 21, 'd': 23}], 'e': 24},
                {'a': [{'b': 31, 'c': 32}], 'e': 34},
                {'a': [{'b': 41}], 'e': 44},
                {'a': [{'c': 51}], 'e': 54},
                {'a': [{'d': 51}], 'e': 54},
                {
                    'a': [{'b': 61, 'c': 62, 'd': 63}, 65, 'foobar', {'b': 66, 'c': 67, 'd': 68}],
                    'e': 64,
                },
                {'a': []},
                {'a': [1, 2, 3, 4]},
                {'a': 'foobar'},
                {'a': 5},
            ]
        )
        actual = self.db.collection.aggregate([{'$project': {'a.b': 1, 'a.c': 1, '_id': 0}}])
        self.assertEqual(
            list(actual),
            [
                {'a': [{'b': 1, 'c': 2}]},
                {'a': [{'c': 12}]},
                {'a': [{'b': 21}]},
                {'a': [{'b': 31, 'c': 32}]},
                {'a': [{'b': 41}]},
                {'a': [{'c': 51}]},
                {'a': [{}]},
                {'a': [{'b': 61, 'c': 62}, {'b': 66, 'c': 67}]},
                {'a': []},
                {'a': []},
                {},
                {},
            ],
        )

    def test__aggregate_arithmetic(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
                'b': 2,
                'c': 2,
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'sum': {'$add': [15, '$a', '$b', '$c']},
                        'prod': {'$multiply': [5, '$a', '$b', '$c']},
                        'trunc': {'$trunc': '$a'},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'sum': 20.5, 'prod': 30, 'trunc': 1}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_string_operation_split_exceptions(self):
        self.db.collection.insert_one({'a': 'Hello', 'b': 'World', 'c': 3})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'split': {'$split': []}}}])
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'split': {'$split': ['$a']}}}])
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'split': {'$split': ['$a', '$b', '$c']}}}])
        with self.assertRaises(TypeError):
            self.db.collection.aggregate([{'$project': {'split': {'$split': ['$a', 1]}}}])
        with self.assertRaises(TypeError):
            self.db.collection.aggregate([{'$project': {'split': {'$split': [1, '$a']}}}])

    def test__aggregate_string_operations(self):
        self.db.collection.insert_one({'a': 'Hello', 'b': 'World', 'c': 3})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'concat': {'$concat': ['$a', ' Dear ', '$b']},
                        'concat_none': {'$concat': ['$a', None, '$b']},
                        'sub1': {'$substr': ['$a', 0, 4]},
                        'sub2': {'$substr': ['$a', -1, 3]},
                        'sub3': {'$substr': ['$a', 2, -1]},
                        'lower': {'$toLower': '$a'},
                        'lower_err': {'$toLower': None},
                        'split_string_none': {'$split': [None, 'l']},
                        'split_string_missing': {'$split': ['$missingField', 'l']},
                        'split_delimiter_none': {'$split': ['$a', None]},
                        'split_delimiter_missing': {'$split': ['$a', '$missingField']},
                        'split': {'$split': ['$a', 'l']},
                        'strcasecmp': {'$strcasecmp': ['$a', '$b']},
                        'upper': {'$toUpper': '$a'},
                        'upper_err': {'$toUpper': None},
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    'concat': 'Hello Dear World',
                    'concat_none': None,
                    'sub1': 'Hell',
                    'sub2': '',
                    'sub3': 'llo',
                    'lower': 'hello',
                    'lower_err': '',
                    'split_string_none': None,
                    'split_string_missing': None,
                    'split_delimiter_none': None,
                    'split_delimiter_missing': None,
                    'split': ['He', '', 'o'],
                    'strcasecmp': -1,
                    'upper': 'HELLO',
                    'upper_err': '',
                }
            ],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_match_expr(self):
        self.db.collection.insert_many(
            [
                {'_id': 0, 'a': 2, 'b': 3},
                {'_id': 1, 'a': 2, 'b': 2},
                {'_id': 2, 'a': 5, 'b': 2},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$match': {
                        '$or': [{'$expr': {'$gt': ['$a', 3]}}, {'b': 3}],
                    }
                }
            ]
        )
        self.assertEqual({0, 2}, {d['_id'] for d in actual})

    def test__aggregate_regexpmatch(self):
        self.db.collection.insert_one({'a': 'Hello', 'b': 'World', 'c': 3})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'Hello': {'$regexMatch': {'input': '$a', 'regex': 'Hel*o'}},
                        'Word': {'$regexMatch': {'input': '$b', 'regex': 'Word'}},
                        'missing-field': {'$regexMatch': {'input': '$d', 'regex': 'orl'}},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'Hello': True, 'Word': False, 'missing-field': False}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_add_fields(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
                'b': 2,
                'c': 2,
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$addFields': {
                        'sum': {'$add': [15, '$a', '$b', '$c']},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'sum': 20.5, 'a': 1.5, 'b': 2, 'c': 2}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_set(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
                'b': 2,
                'c': 2,
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$set': {
                        'sum': {'$add': [15, '$a', '$b', '$c']},
                        'prod': {'$multiply': [5, '$a', '$b', '$c']},
                        'trunc': {'$trunc': '$a'},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'sum': 20.5, 'prod': 30, 'trunc': 1, 'a': 1.5, 'b': 2, 'c': 2}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_set_empty(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
                'b': 2,
                'c': 2,
            }
        )
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$set': {}}])

    def test__aggregate_set_override(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
                'b': 2,
                'c': 2,
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$set': {
                        'a': {'$add': [15, '$a', '$b', '$c']},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'a': 20.5, 'b': 2, 'c': 2}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_set_error(self):
        self.db.collection.insert_one(
            {
                'a': 1.5,
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$set': {
                        'sumA': {'$sum': [15, '$a']},
                        'sum': {'$sum': [15, '$a', '$b', '$c']},
                        'bCopy': '$b',
                    }
                }
            ]
        )
        self.assertEqual(
            [{'a': 1.5, 'sumA': 16.5, 'sum': 16.5}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_set_subfield(self):
        self.db.collection.insert_many(
            [
                {'a': {'b': 1}},
                {'b': 2},
                {'a': {'b': 3, 'c': 4}},
                {'a': 1},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$set': {
                        'a.c': 3,
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {'a': {'b': 1, 'c': 3}},
                {'a': {'c': 3}, 'b': 2},
                {'a': {'b': 3, 'c': 3}},
                {'a': {'c': 3}},
            ],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__strcmp_not_enough_params(self):
        self.db.collection.insert_one(
            {
                'a': 'Hello',
            }
        )
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([{'$project': {'cmp': {'$strcasecmp': ['s']}}}])
        self.assertEqual('strcasecmp must have 2 items', str(err.exception))

    def test__substr_not_enough_params(self):
        self.db.collection.insert_one(
            {
                'a': 'Hello',
            }
        )
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([{'$project': {'sub': {'$substr': ['$a', 1]}}}])
        self.assertEqual('substr must have 3 items', str(err.exception))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_tostr_operation_objectid(self):
        self.db.collection.insert_one({'a': ObjectId('5abcfad1fbc93d00080cfe66')})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'toString': {'$toString': '$a'},
                    }
                }
            ]
        )
        self.assertEqual(
            [{'toString': '5abcfad1fbc93d00080cfe66'}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_unrecognized(self):
        self.db.collection.insert_one({})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'a': {'$notAValidOperation': True}}}])

    def test__aggregate_bitwise_operators(self):
        self.db.collection.insert_one({'a': 5, 'b': 3})
        result = list(
            self.db.collection.aggregate(
                [
                    {'$project': {'bitAnd': {'$bitAnd': ['$a', '$b']}}},
                ]
            )
        )
        self.assertEqual(result[0]['bitAnd'], 1)
        result = list(
            self.db.collection.aggregate(
                [
                    {'$project': {'bitOr': {'$bitOr': ['$a', '$b']}}},
                ]
            )
        )
        self.assertEqual(result[0]['bitOr'], 7)
        result = list(
            self.db.collection.aggregate(
                [
                    {'$project': {'bitXor': {'$bitXor': ['$a', '$b']}}},
                ]
            )
        )
        self.assertEqual(result[0]['bitXor'], 6)
        result = list(
            self.db.collection.aggregate(
                [
                    {'$project': {'bitNot': {'$bitNot': '$a'}}},
                ]
            )
        )
        self.assertEqual(result[0]['bitNot'], -6)

    def test__aggregate_bitwise_errors(self):
        self.db.collection.insert_one({'a': 5, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([{'$project': {'bitAnd': {'$bitAnd': ['$a']}}}]))
        with self.assertRaises(mongomock.OperationFailure):
            list(
                self.db.collection.aggregate(
                    [{'$project': {'bitAnd': {'$bitAnd': ['$a', '$b', '$a']}}}]
                )
            )
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([{'$project': {'bitAnd': {'$bitAnd': ['$a', 1.5]}}}]))
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([{'$project': {'bitNot': {'$bitNot': 1.5}}}]))

    def test__aggregate_std_dev(self):
        self.db.collection.insert_many(
            [
                {'g': 'a', 'v': 2},
                {'g': 'a', 'v': 4},
                {'g': 'a', 'v': 6},
                {'g': 'b', 'v': 10},
            ]
        )
        result = list(
            self.db.collection.aggregate(
                [
                    {
                        '$group': {
                            '_id': '$g',
                            'pop': {'$stdDevPop': '$v'},
                            'samp': {'$stdDevSamp': '$v'},
                        }
                    },
                    {'$sort': {'_id': 1}},
                ]
            )
        )
        self.assertAlmostEqual(result[0]['pop'], 1.632993161855452)
        self.assertAlmostEqual(result[0]['samp'], 2.0)
        self.assertEqual(result[1]['pop'], 0.0)
        self.assertIsNone(result[1]['samp'])

    def test__aggregate_std_dev_edge_cases(self):
        self.db.collection.insert_many(
            [
                {'g': 'a', 'v': 'x'},
                {'g': 'a', 'v': 'y'},
            ]
        )
        result = list(
            self.db.collection.aggregate(
                [
                    {
                        '$group': {
                            '_id': '$g',
                            'pop': {'$stdDevPop': '$v'},
                            'samp': {'$stdDevSamp': '$v'},
                        }
                    },
                ]
            )
        )
        self.assertIsNone(result[0]['pop'])
        self.assertIsNone(result[0]['samp'])

    def test__update_bit_operator(self):
        self.db.collection.insert_one({'a': 5})
        self.db.collection.update_one({}, {'$bit': {'a': {'and': 3}}})
        self.assertEqual(self.db.collection.find_one()['a'], 1)
        self.db.collection.update_one({}, {'$bit': {'a': {'or': 4}}})
        self.assertEqual(self.db.collection.find_one()['a'], 5)
        self.db.collection.update_one({}, {'$bit': {'a': {'xor': 6}}})
        self.assertEqual(self.db.collection.find_one()['a'], 3)

    def test__aggregate_not_implemented(self):
        self.db.collection.insert_one({})

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {'$project': {'a': {'$cmp': [1, 2]}}},
                ]
            )

    def test__aggregate_project_let(self):
        self.db.collection.insert_one({'_id': 1, 'a': 5, 'b': 2, 'c': 3})
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'a': {
                            '$let': {
                                'vars': {'a': 1},
                                'in': {'$multiply': ['$$a', 3]},
                            }
                        },
                    }
                }
            ]
        )
        self.assertEqual([{'_id': 1, 'a': 3}], list(actual))

    def test__aggregate_project_rotate(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1, 'b': 2, 'c': 3})
        actual = self.db.collection.aggregate(
            [
                {'$project': {'a': '$b', 'b': '$a', 'c': 1}},
            ]
        )
        self.assertEqual([{'_id': 1, 'a': 2, 'b': 1, 'c': 3}], list(actual))

    def test__aggregate_mixed_expression(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(
                [
                    {'$project': {'a': {'$literal': False, 'hint': False}}},
                ]
            )

    def test__find_type_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        actual = self.db.collection.find({'arr': {'$type': 'array'}})
        expect = [{'_id': 1, 'arr': [1, 2]}]

        self.assertEqual(expect, list(actual))

    def test__find_type_object(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        actual = self.db.collection.find({'arr': {'$type': 'object'}})
        expect = [{'_id': 2, 'arr': {'a': 4, 'b': 5}}]

        self.assertEqual(expect, list(actual))

    def test__find_type_number(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': 'str'},
                {'_id': 2, 'a': 1},
                {'_id': 3, 'a': {'b': 1}},
                {'_id': 4, 'a': 1.2},
                {'_id': 5, 'a': None},
            ]
        )
        actual = self.db.collection.find({'a': {'$type': 'number'}})
        expect = [
            {'_id': 2, 'a': 1},
            {'_id': 4, 'a': 1.2},
        ]
        self.assertEqual(expect, list(actual))

    def test__find_unknown_type(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'arr': {'$type': 'unknown-type'}})

    def test__find_unimplemented_type(self):
        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one({'arr': {'$type': 'javascript'}})

    def test__find_eq_none(self):
        self.db.collection.insert_one({'_id': 1, 'arr': None})
        self.db.collection.insert_one({'_id': 2})
        actual = self.db.collection.find({'arr': {'$eq': None}}, projection=['_id'])
        expect = [{'_id': 1}, {'_id': 2}]

        self.assertEqual(expect, list(actual))

    def test__find_too_much_nested(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': {'b': 1}}})
        self.db.collection.insert_one({'_id': 2, 'arr': None})
        actual = self.db.collection.find({'arr.a.b': 1}, projection=['_id'])
        self.assertEqual([{'_id': 1}], list(actual))

    def test__find_too_far(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [0, 1]})
        self.db.collection.insert_one({'_id': 2, 'arr': [0]})

        actual = self.db.collection.find({'arr.1': 1}, projection=['_id'])
        self.assertEqual([{'_id': 1}], list(actual))

        actual = self.db.collection.find({'arr.1': {'$exists': False}}, projection=['_id'])
        self.assertEqual([{'_id': 2}], list(actual))

    def test__find_elemmatch_none(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [0, 1]})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'arr': {'$elemMatch': None}})

    def test__find_where(self):
        self.db.collection.insert_many(
            [
                {'name': 'Anya', 'age': 25},
                {'name': 'Bob', 'age': 30},
            ]
        )
        result = self.db.collection.find_one({'$where': 'this.name == "Anya"'})
        self.assertEqual(result['name'], 'Anya')

    def test__find_where_false(self):
        self.db.collection.insert_many(
            [
                {'name': 'Anya', 'age': 25},
                {'name': 'Bob', 'age': 30},
            ]
        )
        result = self.db.collection.find_one({'$where': 'this.age > 30'})
        self.assertIsNone(result)

    def test__find_where_warning(self):
        self.db.collection.insert_one({'name': 'Anya'})
        with self.assertWarns(RuntimeWarning) as ctx:
            self.db.collection.find_one({'$where': 'this.name == "Anya"'})
        self.assertIn('eval', str(ctx.warning))

    def test__find_where_invalid(self):
        self.db.collection.insert_one({'name': 'Anya'})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'$where': 'this.name == '})

    def test__find_where_nonexistent_field(self):
        self.db.collection.insert_one({'name': 'Anya'})
        result = self.db.collection.find_one({'$where': 'this.nonexistent > 0'})
        self.assertIsNone(result)

    def test__find_where_bracket_notation(self):
        self.db.collection.insert_one({'name': 'Anya'})
        result = self.db.collection.find_one({'$where': 'this["name"] == "Anya"'})
        self.assertEqual(result['name'], 'Anya')

    def test__unwind_no_prefix(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        with self.assertRaises(ValueError) as err:
            self.db.collection.aggregate([{'$unwind': 'arr'}])
        self.assertEqual(
            "$unwind failed: exception: field path references must be prefixed with a '$' 'arr'",
            str(err.exception),
        )

    def test__unwind_dict_options(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        actual = self.db.collection.aggregate([{'$unwind': {'path': '$arr'}}])
        self.assertEqual(
            [
                {'_id': 1, 'arr': 1},
                {'_id': 1, 'arr': 2},
            ],
            list(actual),
        )

    def test__unwind_not_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': 1})
        actual = self.db.collection.aggregate([{'$unwind': '$arr'}])
        self.assertEqual([{'_id': 1, 'arr': 1}], list(actual))

    def test__unwind_include_array_index(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'item': 'ABC', 'sizes': ['S', 'M', 'L']},
                {'_id': 2, 'item': 'EFG', 'sizes': []},
                {'_id': 3, 'item': 'IJK', 'sizes': 'M'},
                {'_id': 4, 'item': 'LMN'},
                {'_id': 5, 'item': 'XYZ', 'sizes': None},
            ]
        )
        actual = self.db.collection.aggregate(
            [{'$unwind': {'path': '$sizes', 'includeArrayIndex': 'arrayIndex'}}]
        )
        self.assertEqual(
            [
                {'_id': 1, 'item': 'ABC', 'sizes': 'S', 'arrayIndex': 0},
                {'_id': 1, 'item': 'ABC', 'sizes': 'M', 'arrayIndex': 1},
                {'_id': 1, 'item': 'ABC', 'sizes': 'L', 'arrayIndex': 2},
                {'_id': 3, 'item': 'IJK', 'sizes': 'M', 'arrayIndex': None},
            ],
            list(actual),
        )

    def test__unwind_preserve_null_and_empty_arrays(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'item': 'ABC', 'sizes': ['S', 'M', 'L']},
                {'_id': 2, 'item': 'EFG', 'sizes': []},
                {'_id': 3, 'item': 'IJK', 'sizes': 'M'},
                {'_id': 4, 'item': 'LMN'},
                {'_id': 5, 'item': 'XYZ', 'sizes': None},
                {'_id': 6, 'item': 'abc', 'sizes': False},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {'$unwind': {'path': '$sizes', 'preserveNullAndEmptyArrays': True}},
            ]
        )
        self.assertEqual(
            [
                {'_id': 1, 'item': 'ABC', 'sizes': 'S'},
                {'_id': 1, 'item': 'ABC', 'sizes': 'M'},
                {'_id': 1, 'item': 'ABC', 'sizes': 'L'},
                {'_id': 2, 'item': 'EFG'},
                {'_id': 3, 'item': 'IJK', 'sizes': 'M'},
                {'_id': 4, 'item': 'LMN'},
                {'_id': 5, 'item': 'XYZ', 'sizes': None},
                {'_id': 6, 'item': 'abc', 'sizes': False},
            ],
            list(actual),
        )

    def test__unwind_preserve_null_and_empty_arrays_on_nested(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'item': 'ABC', 'nest': {'sizes': ['S', 'M', 'L']}},
                {'_id': 2, 'item': 'EFG', 'nest': {'sizes': []}},
                {'_id': 3, 'item': 'IJK', 'nest': {'sizes': 'M'}},
                {'_id': 4, 'item': 'LMN', 'nest': {}},
                {'_id': 5, 'item': 'XYZ', 'nest': {'sizes': None}},
                {'_id': 6, 'item': 'abc', 'nest': {'sizes': False}},
                {'_id': 7, 'item': 'abc', 'nest': ['A', 'B', 'C']},
                {'_id': 8, 'item': 'abc', 'nest': [{'sizes': 'A'}, {'sizes': ['B', 'C']}]},
                {'_id': 9, 'item': 'def'},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {'$unwind': {'path': '$nest.sizes', 'preserveNullAndEmptyArrays': True}},
            ]
        )
        self.assertEqual(
            [
                {'_id': 1, 'item': 'ABC', 'nest': {'sizes': 'S'}},
                {'_id': 1, 'item': 'ABC', 'nest': {'sizes': 'M'}},
                {'_id': 1, 'item': 'ABC', 'nest': {'sizes': 'L'}},
                {'_id': 2, 'item': 'EFG', 'nest': {}},
                {'_id': 3, 'item': 'IJK', 'nest': {'sizes': 'M'}},
                {'_id': 4, 'item': 'LMN', 'nest': {}},
                {'_id': 5, 'item': 'XYZ', 'nest': {'sizes': None}},
                {'_id': 6, 'item': 'abc', 'nest': {'sizes': False}},
                {'_id': 7, 'item': 'abc', 'nest': ['A', 'B', 'C']},
                {'_id': 8, 'item': 'abc', 'nest': [{'sizes': 'A'}, {'sizes': ['B', 'C']}]},
                {'_id': 9, 'item': 'def'},
            ],
            list(actual),
        )

    def test__array_size_non_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr0': [], 'arr3': [1, 2, 3]})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([{'$project': {'size': {'$size': 'arr'}}}])
        self.assertEqual(
            f'The argument to $size must be an array, but was of type: {str}', str(err.exception)
        )

    def test__array_size_argument_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([{'$project': {'size': {'$size': [1, 2, 3]}}}])
        self.assertEqual(
            'Expression $size takes exactly 1 arguments. 3 were passed in.', str(err.exception)
        )

    def test__array_size_valid_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr0': [], 'arr3': [1, 2, 3]})
        result1 = self.db.collection.aggregate([{'$project': {'size': {'$size': '$arr0'}}}]).next()
        self.assertEqual(result1['size'], 0)

        result2 = self.db.collection.aggregate([{'$project': {'size': {'$size': '$arr3'}}}]).next()
        self.assertEqual(result2['size'], 3)

    def test__array_size_valid_argument_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        result1 = self.db.collection.aggregate([{'$project': {'size': {'$size': [[1, 2]]}}}]).next()
        self.assertEqual(result1['size'], 2)

        result2 = self.db.collection.aggregate([{'$project': {'size': {'$size': ['$arr']}}}]).next()
        self.assertEqual(result2['size'], 3)

        result3 = self.db.collection.aggregate(
            [{'$project': {'size': {'$size': [{'$literal': [1, 2, 3, 4, 5]}]}}}]
        ).next()
        self.assertEqual(result3['size'], 5)

    def test__array_size_valid_expression(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        result = self.db.collection.aggregate(
            [{'$project': {'size': {'$size': {'$literal': [1, 2, 3, 4]}}}}]
        ).next()
        self.assertEqual(result['size'], 4)

    def test__aggregate_project_out_replace(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        new_collection = self.db.get_collection('new_collection')
        new_collection.insert_one({'_id': 3})
        self.db.collection.aggregate(
            [
                {'$match': {'_id': 1}},
                {'$project': {'rename_dot': '$arr.a'}},
                {'$out': 'new_collection'},
            ]
        )
        actual = list(new_collection.find())
        expect = [{'_id': 1, 'rename_dot': 2}]

        self.assertEqual(expect, actual)

    def test__all_elemmatch(self):
        self.db.collection.insert_many(
            [
                {
                    '_id': 5,
                    'code': 'xyz',
                    'tags': ['school', 'book', 'bag', 'headphone', 'appliance'],
                    'qty': [
                        {'size': 'S', 'num': 10, 'color': 'blue'},
                        {'size': 'M', 'num': 45, 'color': 'blue'},
                        {'size': 'L', 'num': 100, 'color': 'green'},
                    ],
                },
                {
                    '_id': 6,
                    'code': 'abc',
                    'tags': ['appliance', 'school', 'book'],
                    'qty': [
                        {'size': '6', 'num': 100, 'color': 'green'},
                        {'size': '6', 'num': 50, 'color': 'blue'},
                        {'size': '8', 'num': 100, 'color': 'brown'},
                    ],
                },
                {
                    '_id': 7,
                    'code': 'efg',
                    'tags': ['school', 'book'],
                    'qty': [
                        {'size': 'S', 'num': 10, 'color': 'blue'},
                        {'size': 'M', 'num': 100, 'color': 'blue'},
                        {'size': 'L', 'num': 100, 'color': 'green'},
                    ],
                },
                {
                    '_id': 8,
                    'code': 'ijk',
                    'tags': ['electronics', 'school'],
                    'qty': [
                        {'size': 'M', 'num': 100, 'color': 'green'},
                    ],
                },
            ]
        )
        filters = {
            'qty': {
                '$all': [
                    {'$elemMatch': {'size': 'M', 'num': {'$gt': 50}}},
                    {'$elemMatch': {'num': 100, 'color': 'green'}},
                ],
            },
        }
        results = self.db.collection.find(filters)
        self.assertEqual([doc['_id'] for doc in results], [7, 8])

    def test__all_size(self):
        self.db.collection.insert_many(
            [
                {
                    'code': 'ijk',
                    'tags': ['electronics', 'school'],
                    'qty': [{'size': 'M', 'num': 100, 'color': 'green'}],
                },
                {
                    'code': 'efg',
                    'tags': ['school', 'book'],
                    'qty': [
                        {'size': 'S', 'num': 10, 'color': 'blue'},
                        {'size': 'M', 'num': 100, 'color': 'blue'},
                        {'size': 'L', 'num': 100, 'color': 'green'},
                    ],
                },
            ]
        )
        self.assertEqual(1, self.db.collection.count_documents({'qty.size': {'$all': ['M', 'L']}}))

    def test__filter_eq_on_array(self):
        """$eq on array matches if one element of the array matches."""
        collection = self.db.collection
        collection.insert_many(
            [
                {'_id': 1, 'shape': [{'color': 'red'}]},
                {'_id': 2, 'shape': [{'color': 'yellow'}]},
                {'_id': 3, 'shape': [{'color': 'red'}, {'color': 'yellow'}]},
                {'_id': 4, 'shape': [{'size': 3}]},
                {'_id': 5},
                {'_id': 6, 'shape': {'color': ['red', 'yellow']}},
            ]
        )

        results = self.db.collection.find({'shape.color': {'$eq': 'red'}})
        self.assertEqual([1, 3, 6], [doc['_id'] for doc in results])

        # testing eq operation with null as value
        results = self.db.collection.find({'shape.color': {'$eq': None}})
        self.assertEqual([4, 5], [doc['_id'] for doc in results])

        results = self.db.collection.find({'shape.color': None})
        self.assertEqual([4, 5], [doc['_id'] for doc in results])

    def test__filter_ne_on_array(self):
        """$ne and $nin on array only matches if no element of the array matches."""
        collection = self.db.collection
        collection.insert_many(
            [
                {'_id': 1, 'shape': [{'color': 'red'}]},
                {'_id': 2, 'shape': [{'color': 'yellow'}]},
                {'_id': 3, 'shape': [{'color': 'red'}, {'color': 'yellow'}]},
                {'_id': 4, 'shape': [{'size': 3}]},
                {'_id': 5},
                {'_id': 6, 'shape': {'color': ['red', 'yellow']}},
            ]
        )

        # $ne
        results = self.db.collection.find({'shape.color': {'$ne': 'red'}})
        self.assertEqual([2, 4, 5], [doc['_id'] for doc in results])

        # $ne
        results = self.db.collection.find({'shape.color': {'$ne': ['red', 'yellow']}})
        self.assertEqual([1, 2, 3, 4, 5], [doc['_id'] for doc in results])

        # $nin
        results = self.db.collection.find({'shape.color': {'$nin': ['blue', 'red']}})
        self.assertEqual([2, 4, 5], [doc['_id'] for doc in results])

    def test__filter_ne_multiple_keys(self):
        """Using $ne and another operator."""
        collection = self.db.collection
        collection.insert_many(
            [
                {'_id': 1, 'cases': [{'total': 1}]},
                {'_id': 2, 'cases': [{'total': 2}]},
                {'_id': 3, 'cases': [{'total': 3}]},
                {'_id': 4, 'cases': []},
                {'_id': 5},
            ]
        )

        # $ne
        results = self.db.collection.find({'cases.total': {'$gt': 1, '$ne': 3}})
        self.assertEqual([2], [doc['_id'] for doc in results])

        # $nin
        results = self.db.collection.find({'cases.total': {'$gt': 1, '$nin': [1, 3]}})
        self.assertEqual([2], [doc['_id'] for doc in results])

    def test__filter_objects_comparison(self):
        collection = self.db.collection
        query = {'counts': {'$gt': {'circles': 1}}}
        collection.insert_many(
            [
                # Document kept: circles' value 3 is greater than 1.
                {'_id': 1, 'counts': {'circles': 3}},
                # Document kept: the first key, squares, is greater than circles.
                {'_id': 2, 'counts': {'squares': 0}},
                # Document dropped: the first key, arrows, is smaller than circles.
                {'_id': 3, 'counts': {'arrows': 15}},
                # Document dropped: the dicts are equal.
                {'_id': 4, 'counts': {'circles': 1}},
                # Document kept: the first item is equal, and there is an additional item.
                {
                    '_id': 5,
                    'counts': collections.OrderedDict(
                        [
                            ('circles', 1),
                            ('arrows', 15),
                        ]
                    ),
                },
                # Document dropped: same as above, but order matters.
                {
                    '_id': 6,
                    'counts': collections.OrderedDict(
                        [
                            ('arrows', 15),
                            ('circles', 1),
                        ]
                    ),
                },
                # Document dropped: the value is missing.
                {'_id': 7},
                # Document dropped: there is less items.
                {'_id': 8, 'counts': {}},
                # Document kept: strings are greater than numbers.
                {'_id': 9, 'counts': {'circles': 'three'}},
                # Document dropped: None is less than numbers.
                {'_id': 10, 'counts': {'circles': None}},
                # Document kept: ObjectIds are more than numbers.
                {'_id': 11, 'counts': {'circles': mongomock.ObjectId()}},
                # Document kept: datetimes are more than numbers.
                {'_id': 12, 'counts': {'circles': datetime.now()}},
                # Document kept: BinData are more than numbers.
                {'_id': 13, 'counts': {'circles': b'binary'}},
            ]
        )
        results = collection.find(query)
        self.assertEqual({1, 2, 5, 9, 11, 12, 13}, {doc['_id'] for doc in results})

        query = {'counts': {'$gt': {'circles': re.compile('3')}}}
        self.assertFalse(list(collection.find(query)))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__filter_bson_regex(self):
        self.db.collection.insert_many(
            [
                {'_id': 'a'},
                {'_id': 'A'},
                {'_id': 'abc'},
                {'_id': 'b'},
                {'_id': 'ba'},
            ]
        )
        results = self.db.collection.find({'_id': Regex('^a', 'i')})
        self.assertEqual({'a', 'A', 'abc'}, {doc['_id'] for doc in results})

        self.db.tada.drop()
        self.db.tada.insert_one({'a': 'TADA'})
        self.db.tada.insert_one({'a': 'TA\nDA'})
        self.assertTrue(
            self.db.tada.find_one(
                {
                    'a': {
                        '$regex': Regex('tada', re.IGNORECASE),
                    }
                }
            )
        )
        self.assertTrue(
            self.db.tada.find_one(
                {
                    'a': collections.OrderedDict(
                        [
                            ('$regex', Regex('tada')),
                            ('$options', 'i'),
                        ]
                    )
                }
            )
        )
        self.assertTrue(
            self.db.tada.find_one(
                {
                    'a': collections.OrderedDict(
                        [
                            ('$regex', Regex('tada', re.IGNORECASE)),
                            ('$options', 'm'),
                        ]
                    )
                }
            )
        )

    def test__filter_objects_comparison_unknown_type(self):
        self.db.collection.insert_one({'counts': 3})
        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one({'counts': {'$gt': str}})

    def test__filter_objects_nested_comparison(self):
        collection = self.db.collection
        query = {'counts': {'$gt': {'circles': {'blue': 1}}}}
        collection.insert_many(
            [
                # Document kept: circles' value {'blue': 3} is greater than {'blue': 1}.
                {'_id': 1, 'counts': {'circles': {'blue': 3}}},
                # Document kept: the first key, squares, is greater than circles.
                {'_id': 2, 'counts': {'squares': {}}},
                # Document dropped: the first key, arrows, is smaller than circles.
                {'_id': 3, 'counts': {'arrows': {'blue': 2}}},
                # Document dropped: circles' value {} is less than {'blue': 1}.
                {'_id': 4, 'counts': {'circles': {}}},
                # Document kept: the first value type is greater than the type of {'blue' : 1}.
                {'_id': 5, 'counts': {'arrows': True}},
            ]
        )
        results = collection.find(query)
        self.assertEqual({1, 2, 5}, {doc['_id'] for doc in results})

    def test_filter_not_bad_value(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$not': 3}})

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$not': {'b': 3}}})

    def test_filter_not_regex(self):
        self.db.collection.insert_many(
            [
                {'_id': 1, 'a': 'b'},
                # Starts with a: should be excluded.
                {'_id': 2, 'a': 'a'},
                {'_id': 3, 'a': 'ba'},
                {'_id': 4},
            ]
        )
        results = self.db.collection.find({'a': {'$not': {'$regex': '^a'}}})
        self.assertEqual({1, 3, 4}, {doc['_id'] for doc in results})

    def test_insert_many_bulk_write_error(self):
        collection = self.db.collection
        with self.assertRaises(mongomock.BulkWriteError) as cm:
            collection.insert_many([{'_id': 1}, {'_id': 1}])
        self.assertIn('batch op errors occurred', str(cm.exception))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_many_bulk_write_error_details(self):
        collection = self.db.collection
        with self.assertRaises(mongomock.BulkWriteError) as cm:
            collection.insert_many([{'_id': 1}, {'_id': 1}])
        self.assertEqual(65, cm.exception.code)
        write_errors = cm.exception.details['writeErrors']
        self.assertEqual([11000], [error.get('code') for error in write_errors])

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_bson_validation(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert_one({'a': {'b'}})
        self.assertIn("cannot encode object: {'b'}, of type: <class 'set'>", str(cm.exception))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_bson_invalid_encode_type(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert_one({'$foo': 'bar'})
        self.assertEqual(
            str(cm.exception),
            'Top-level field names cannot start with the "$" sign (found: $foo)',
        )
        with self.assertRaises(InvalidDocument):
            collection.insert_one({'foo': {'foo\0bar': 'bar'}})

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test_update_bson_invalid_encode_type(self):
        self.db.collection.insert_one({'a': 1})
        with self.assertRaises(InvalidDocument):
            self.db.collection.update_one(filter={'a': 1}, update={'$set': {'$a': 2}})

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_bson_special_characters(self):
        collection = self.db.collection
        collection.insert_one({'foo.bar.zoo': {'foo.bar': '$zoo'}, 'foo.$bar': 'zoo'})
        actual = self.db.collection.find_one()
        assert actual['foo.bar.zoo'] == {'foo.bar': '$zoo'}
        assert actual['foo.$bar'] == 'zoo'

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__update_invalid_encode_type(self):
        self.db.collection.insert_one({'_id': 1, 'foo': 'bar'})

        with self.assertRaises(InvalidDocument):
            self.db.collection.update_one({}, {'$set': {'foo': {'bar'}}})

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__replace_invalid_encode_type(self):
        self.db.collection.insert_one({'_id': 1, 'foo': 'bar'})

        with self.assertRaises(InvalidDocument):
            self.db.collection.replace_one({}, {'foo': {'bar'}})

    def test_aggregate_unwind_push_first(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {
                    '_id': 1111,
                    'a': [
                        {
                            'class': '03',
                            'a': [
                                {'b': '030502', 'weight': 100.0},
                                {'b': '030207', 'weight': 100.0},
                            ],
                        }
                    ],
                    'id': 'ooo',
                    'update_time': 1111,
                },
                {
                    '_id': 22222,
                    'a': [
                        {
                            'class': '03',
                            'a': [
                                {'b': '030502', 'weight': 99.0},
                                {'b': '0302071', 'weight': 100.0},
                            ],
                        }
                    ],
                    'id': 'ooo',
                    'update_time': 1222,
                },
            ]
        )
        actual = collection.aggregate(
            [
                {'$sort': {'update_time': -1}},
                {'$match': {'a': {'$ne': None}}},
                {
                    '$group': {
                        '_id': '$id',
                        'update_time': {'$first': '$update_time'},
                        'a': {'$first': '$a'},
                    }
                },
                {'$unwind': '$a'},
                {'$unwind': '$a.a'},
                {
                    '$group': {
                        '_id': '$_id',
                        'update_time': {'$first': '$update_time'},
                        'a': {'$push': {'b': '$a.a.b', 'weight': '$a.a.weight'}},
                    }
                },
                {'$out': 'ooo'},
            ],
            allowDiskUse=True,
        )
        expect = [
            {
                'update_time': 1222,
                'a': [{'weight': 99.0, 'b': '030502'}, {'weight': 100.0, 'b': '0302071'}],
                '_id': 'ooo',
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__agregate_first_on_empty(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 1, 'b': 1},
                {'a': 1, 'b': 2},
                {'a': 2},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$group': {
                        '_id': '$a',
                        'firstB': {'$first': '$b'},
                        'lastB': {'$last': '$b'},
                    }
                }
            ]
        )
        expect = [
            {'_id': 1, 'firstB': 1, 'lastB': 2},
            {'_id': 2, 'firstB': None, 'lastB': None},
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_group_scalar_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 2, 'b': 3, 'c': 4},
                {'a': 2, 'b': 3, 'c': 5},
                {'a': 1, 'b': 1, 'c': 1},
            ]
        )
        actual = collection.aggregate(
            [
                {'$group': {'_id': '$a'}},
            ]
        )
        self.assertCountEqual([{'_id': 1}, {'_id': 2}], list(actual))

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_basic(self):
        collection = self.db.collection
        collection.insert_one({'a': 1, 'b': 2})
        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate([{'$setWindowFields': {}}])

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate([{'$setWindowFields': {'output': {'out': {'$doesnt_exist': {}}}}}])

        actual = list(
            collection.aggregate([{'$setWindowFields': {'output': {'field': {'$sum': 1}}}}])
        )
        self.assertEqual([{'_id': actual[0]['_id'], 'a': 1, 'b': 2, 'field': 1}], actual)

        actual = list(
            collection.aggregate(
                [
                    {
                        '$setWindowFields': {
                            'sortBy': {'a': 1},
                            'output': {
                                'field': {'$shift': {'output': '$b', 'by': 1}, 'window': {}}
                            },
                        }
                    }
                ]
            )
        )
        self.assertEqual([{'_id': actual[0]['_id'], 'a': 1, 'b': 2, 'field': None}], actual)

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_shift(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'value': 15},
            {'type': 1, 'value': 10},
            {'type': 2, 'value': 20},
            {'type': 2, 'value': 25},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'value': -1},
                        'output': {'out': {'$shift': {'output': '$value', 'by': 1, 'default': 0}}},
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'out': 10, 'type': 1, 'value': 15},
            {'out': 0, 'type': 1, 'value': 10},
            {'out': 20, 'type': 2, 'value': 25},
            {'out': 0, 'type': 2, 'value': 20},
        ]
        self.assertEqual(expected, list(actual))

        # Test no sortBy field
        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {
                        '$setWindowFields': {
                            'partitionBy': '$type',
                            'output': {
                                'out': {'$shift': {'output': '$value', 'by': 1, 'default': 0}}
                            },
                        }
                    }
                ]
            )

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_sum_avg(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 2, 'val': 5},
            {'type': 2, 'val': 15},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'sum': {
                                '$sum': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                            'avg': {
                                '$avg': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'sum': 10, 'avg': 10.0},
            {'type': 1, 'val': 20, 'sum': 30, 'avg': 15.0},
            {'type': 2, 'val': 5, 'sum': 5, 'avg': 5.0},
            {'type': 2, 'val': 15, 'sum': 20, 'avg': 10.0},
        ]
        self.assertEqual(expected, list(actual))

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_min_max_first_last(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 2, 'val': 5},
            {'type': 2, 'val': 15},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'min_val': {
                                '$min': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                            'max_val': {
                                '$max': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                            'first_val': {
                                '$first': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                            'last_val': {
                                '$last': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'min_val': 10, 'max_val': 10, 'first_val': 10, 'last_val': 10},
            {'type': 1, 'val': 20, 'min_val': 10, 'max_val': 20, 'first_val': 10, 'last_val': 20},
            {'type': 2, 'val': 5, 'min_val': 5, 'max_val': 5, 'first_val': 5, 'last_val': 5},
            {'type': 2, 'val': 15, 'min_val': 5, 'max_val': 15, 'first_val': 5, 'last_val': 15},
        ]
        self.assertEqual(expected, list(actual))

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_push_add_to_set(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 1, 'val': 10},
            {'type': 2, 'val': 30},
        ]
        collection.insert_many(data)
        # Note: type-1 has 3 docs, type-2 has 1 doc → 4 docs total, 4 window outputs
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'pushed': {
                                '$push': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                            'set': {
                                '$addToSet': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                        },
                    },
                },
                {'$sort': {'type': 1, 'val': 1}},
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'pushed': [10], 'set': [10]},
            {'type': 1, 'val': 10, 'pushed': [10, 10], 'set': [10]},
            {'type': 1, 'val': 20, 'pushed': [10, 10, 20], 'set': [10, 20]},
            {'type': 2, 'val': 30, 'pushed': [30], 'set': [30]},
        ]
        result = list(actual)
        for doc in result:
            doc['set'] = sorted(doc['set'])
        for doc in expected:
            doc['set'] = sorted(doc['set'])
        self.assertEqual(expected, result)

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_count(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 2, 'val': 5},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'cnt': {
                                '$count': {},
                                'window': {'documents': ['unbounded', 'current']},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'cnt': 1},
            {'type': 1, 'val': 20, 'cnt': 2},
            {'type': 2, 'val': 5, 'cnt': 1},
        ]
        self.assertEqual(expected, list(actual))

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_document_number(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 2, 'val': 5},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'doc_num': {'$documentNumber': {}},
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'doc_num': 1},
            {'type': 1, 'val': 20, 'doc_num': 2},
            {'type': 2, 'val': 5, 'doc_num': 1},
        ]
        self.assertEqual(expected, list(actual))

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_rank_dense_rank(self):
        collection = self.db.collection
        data = [
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 10},
            {'type': 1, 'val': 20},
            {'type': 2, 'val': 5},
        ]
        collection.insert_many(data)
        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'partitionBy': '$type',
                        'sortBy': {'val': 1},
                        'output': {
                            'rank': {'$rank': {}},
                            'dense_rank': {'$denseRank': {}},
                        },
                    },
                },
                {'$sort': {'type': 1, 'val': 1}},
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'type': 1, 'val': 10, 'rank': 1, 'dense_rank': 1},
            {'type': 1, 'val': 10, 'rank': 1, 'dense_rank': 1},
            {'type': 1, 'val': 20, 'rank': 3, 'dense_rank': 2},
            {'type': 2, 'val': 5, 'rank': 1, 'dense_rank': 1},
        ]
        self.assertEqual(expected, list(actual))

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {
                        '$setWindowFields': {
                            'partitionBy': '$type',
                            'output': {
                                'rank': {'$rank': {}},
                            },
                        },
                    },
                ]
            )

    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$setWindowFields is not supported before MongoDB 5.0',
    )
    def test__aggregate_set_window_fields_window_bounds(self):
        collection = self.db.collection
        data = [
            {'val': 10},
            {'val': 20},
            {'val': 30},
            {'val': 40},
            {'val': 50},
        ]
        collection.insert_many(data)

        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'sortBy': {'val': 1},
                        'output': {
                            'cumulative_sum': {
                                '$sum': '$val',
                                'window': {'documents': ['unbounded', 'current']},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'val': 10, 'cumulative_sum': 10},
            {'val': 20, 'cumulative_sum': 30},
            {'val': 30, 'cumulative_sum': 60},
            {'val': 40, 'cumulative_sum': 100},
            {'val': 50, 'cumulative_sum': 150},
        ]
        self.assertEqual(expected, list(actual))

        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'sortBy': {'val': 1},
                        'output': {
                            'reverse_sum': {
                                '$sum': '$val',
                                'window': {'documents': ['current', 'unbounded']},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'val': 10, 'reverse_sum': 150},
            {'val': 20, 'reverse_sum': 140},
            {'val': 30, 'reverse_sum': 120},
            {'val': 40, 'reverse_sum': 90},
            {'val': 50, 'reverse_sum': 50},
        ]
        self.assertEqual(expected, list(actual))

        actual = collection.aggregate(
            [
                {
                    '$setWindowFields': {
                        'sortBy': {'val': 1},
                        'output': {
                            'sliding_sum': {
                                '$sum': '$val',
                                'window': {'documents': [-1, 0]},
                            },
                        },
                    },
                },
                {'$project': {'_id': 0}},
            ]
        )
        expected = [
            {'val': 10, 'sliding_sum': 10},
            {'val': 20, 'sliding_sum': 30},
            {'val': 30, 'sliding_sum': 50},
            {'val': 40, 'sliding_sum': 70},
            {'val': 50, 'sliding_sum': 90},
        ]
        self.assertEqual(expected, list(actual))

    def test__aggregate_group_missing_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 1},
                {},
                {'a': None},
            ]
        )
        actual = collection.aggregate(
            [
                {'$group': {'_id': '$a'}},
            ]
        )
        self.assertCountEqual([{'_id': 1}, {'_id': None}], list(actual))

    def test__aggregate_group_dict_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 2, 'b': 3, 'c': 4},
                {'a': 2, 'b': 3, 'c': 5},
                {'a': 1, 'b': 1, 'c': 1},
            ]
        )
        actual = collection.aggregate(
            [
                {'$group': {'_id': {'a': '$a', 'b': '$b'}}},
            ]
        )
        self.assertCountEqual([{'_id': {'a': 1, 'b': 1}}, {'_id': {'a': 2, 'b': 3}}], list(actual))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_group_dbref_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'myref': DBRef('a', '1')},
                {'myref': DBRef('a', '2')},
                {'myref': DBRef('b', '1')},
            ]
        )
        actual = collection.aggregate([{'$group': {'_id': '$myref'}}])
        expect = [
            {'_id': DBRef('b', '1')},
            {'_id': DBRef('a', '2')},
            {'_id': DBRef('a', '1')},
        ]
        self.assertCountEqual(expect, list(actual))

    def test__aggregate_group_sum(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'group': 'one'},
                {'group': 'one', 'data': None},
                {'group': 'one', 'data': 0},
                {'group': 'one', 'data': 2},
                {'group': 'one', 'data': {'a': 1}},
                {'group': 'one', 'data': [1, 2]},
                {'group': 'one', 'data': [3, 4]},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$group': {
                        '_id': '$group',
                        'count': {'$sum': 1},
                        'countData': {'$sum': {'$cond': ['$data', 1, 0]}},
                        'countDataExists': {
                            '$sum': {
                                '$cond': {
                                    'if': {'$gt': ['$data', None]},
                                    'then': 1,
                                    'else': 0,
                                }
                            }
                        },
                    }
                }
            ]
        )
        expect = [
            {
                '_id': 'one',
                'count': 7,
                'countData': 4,
                'countDataExists': 5,
            }
        ]
        self.assertEqual(expect, list(actual))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_group_sum_for_decimal(self):
        collection = self.db.collection
        collection.drop()
        decimal_value = decimal128.Decimal128('4')
        collection.insert_one({'_id': 1, 'a': 2, 'b': 3, 'c': '$d', 'd': decimal_value})
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'sum': {'$sum': [4, 2, None, 3, '$a', '$b', '$d', {'$sum': [0, 1, '$b']}]},
                        'sum_no_decimal': {
                            '$sum': [4, 2, None, 3, '$a', '$b', {'$sum': [0, 1, '$b']}]
                        },
                        'sumNone': {'$sum': ['a', None]},
                    }
                }
            ]
        )
        expect = [
            {
                'sum': decimal128.Decimal128('22'),
                'sum_no_decimal': 18,
                'sumNone': 0,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_bucket(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many(
            [
                {
                    '_id': 1,
                    'title': 'The Pillars of Society',
                    'artist': 'Grosz',
                    'year': 1926,
                    'price': 199.99,
                },
                {
                    '_id': 2,
                    'title': 'Melancholy III',
                    'artist': 'Munch',
                    'year': 1902,
                    'price': 200.00,
                },
                {
                    '_id': 3,
                    'title': 'Dancer',
                    'artist': 'Miro',
                    'year': 1925,
                    'price': 76.04,
                },
                {
                    '_id': 4,
                    'title': 'The Great Wave off Kanagawa',
                    'artist': 'Hokusai',
                    'price': 167.30,
                },
                {
                    '_id': 5,
                    'title': 'The Persistence of Memory',
                    'artist': 'Dali',
                    'year': 1931,
                    'price': 483.00,
                },
                {
                    '_id': 6,
                    'title': 'Composition VII',
                    'artist': 'Kandinsky',
                    'year': 1913,
                    'price': 385.00,
                },
                {
                    '_id': 7,
                    'title': 'The Scream',
                    'artist': 'Munch',
                    'year': 1893,
                    # No price
                },
                {
                    '_id': 8,
                    'title': 'Blue Flower',
                    'artist': "O'Keefe",
                    'year': 1918,
                    'price': 118.42,
                },
            ]
        )

        actual = collection.aggregate(
            [
                {
                    '$bucket': {
                        'groupBy': '$price',
                        'boundaries': [0, 200, 400],
                        'default': 'Other',
                        'output': {
                            'count': {'$sum': 1},
                            'titles': {'$push': '$title'},
                        },
                    }
                }
            ]
        )
        expect = [
            {
                '_id': 0,
                'count': 4,
                'titles': [
                    'The Pillars of Society',
                    'Dancer',
                    'The Great Wave off Kanagawa',
                    'Blue Flower',
                ],
            },
            {
                '_id': 200,
                'count': 2,
                'titles': ['Melancholy III', 'Composition VII'],
            },
            {
                '_id': 'Other',
                'count': 2,
                'titles': [
                    'The Persistence of Memory',
                    'The Scream',
                ],
            },
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_bucket_no_default(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many(
            [
                {
                    '_id': 1,
                    'title': 'The Pillars of Society',
                    'artist': 'Grosz',
                    'year': 1926,
                    'price': 199.99,
                },
                {
                    '_id': 2,
                    'title': 'Melancholy III',
                    'artist': 'Munch',
                    'year': 1902,
                    'price': 280.00,
                },
                {
                    '_id': 3,
                    'title': 'Dancer',
                    'artist': 'Miro',
                    'year': 1925,
                    'price': 76.04,
                },
            ]
        )

        actual = collection.aggregate(
            [
                {
                    '$bucket': {
                        'groupBy': '$price',
                        'boundaries': [0, 200, 400, 600],
                    }
                }
            ]
        )
        expect = [
            {
                '_id': 0,
                'count': 2,
            },
            {
                '_id': 200,
                'count': 1,
            },
        ]
        self.assertEqual(expect, list(actual))

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {
                        '$bucket': {
                            'groupBy': '$price',
                            'boundaries': [0, 150],
                        }
                    }
                ]
            )

    def test__aggregate_bucket_wrong_options(self):
        options = [
            {},
            {'groupBy': '$price', 'boundaries': [0, 1], 'extraOption': 2},
            {'groupBy': '$price'},
            {'boundaries': [0, 1]},
            {'groupBy': '$price', 'boundaries': 3},
            {'groupBy': '$price', 'boundaries': [0]},
            {'groupBy': '$price', 'boundaries': [1, 0]},
        ]
        for option in options:
            with self.assertRaises(mongomock.OperationFailure, msg=option):
                self.db.collection.aggregate([{'$bucket': option}])

    def test__aggregate_subtract_dates(self):
        self.db.collection.insert_one(
            {
                'date': datetime(2014, 7, 4, 13, 0, 4, 20000),
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'since': {'$subtract': ['$date', datetime(2014, 7, 4, 13, 0, 0, 20)]},
                    }
                }
            ]
        )
        self.assertEqual([4020], [d['since'] for d in actual])

    def test__aggregate_subtract_milliseconds_from_date(self):
        self.db.collection.insert_one(
            {
                'date': datetime(2014, 7, 4, 13, 0, 4, 20000),
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'since': {'$subtract': ['$date', 1000]},
                    }
                }
            ]
        )
        self.assertEqual([datetime(2014, 7, 4, 13, 0, 3, 20000)], [d['since'] for d in actual])

    def test__aggregate_system_variables(self):
        self.db.collection.insert_many(
            [
                {'_id': 1},
                {'_id': 2, 'parent_id': 1},
                {'_id': 3, 'parent_id': 1},
            ]
        )
        actual = self.db.collection.aggregate(
            [
                {'$match': {'parent_id': {'$in': [1]}}},
                {'$group': {'_id': 1, 'docs': {'$push': '$$ROOT'}}},
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'docs': [
                        {'_id': 2, 'parent_id': 1},
                        {'_id': 3, 'parent_id': 1},
                    ],
                }
            ],
            list(actual),
        )

    def test__aggregate_select_nested(self):
        self.db.collection.insert_one(
            {
                'base_value': 100,
                'values_list': [
                    {'updated_value': 5},
                    {'updated_value': 15},
                ],
                'nested_value': {
                    'updated_value': 7,
                },
            }
        )
        actual = list(
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'select_1': '$values_list.1.updated_value',
                            'select_nested': '$nested_value.updated_value',
                            'select_array': '$values_list.updated_value',
                        }
                    },
                ]
            )
        )
        self.assertEqual(1, len(actual), msg=actual)
        actual[0].pop('_id')
        self.assertEqual(
            {
                'select_1': 15,
                'select_nested': 7,
                'select_array': [5, 15],
            },
            actual[0],
        )

    def test__aggregate_concatArrays(self):
        self.db.collection.insert_one(
            {
                'a': [1, 2],
                'b': ['foo', 'bar', 'baz'],
                'c': {
                    'arr1': [123],
                },
            }
        )
        actual = self.db.collection.aggregate(
            [
                {
                    '$project': {
                        'concat': {'$concatArrays': ['$a', ['#', '*'], '$c.arr1', '$b']},
                        'concat_array_expression': {'$concatArrays': '$b'},
                        'concat_tuples': {'$concatArrays': ((1, 2, 3), (1,))},
                        'concat_none': {'$concatArrays': None},
                        'concat_missing_field': {'$concatArrays': '$foo'},
                        'concat_none_item': {'$concatArrays': ['$a', None, '$b']},
                        'concat_missing_field_item': {'$concatArrays': [[1, 2, 3], '$c.arr2']},
                        'concat_with_variable_inside': {'$concatArrays': ['$a', ['$a']]},
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    'concat': [1, 2, '#', '*', 123, 'foo', 'bar', 'baz'],
                    'concat_array_expression': ['foo', 'bar', 'baz'],
                    'concat_tuples': [1, 2, 3, 1],
                    'concat_none': None,
                    'concat_missing_field': None,
                    'concat_none_item': None,
                    'concat_missing_field_item': None,
                    'concat_with_variable_inside': [1, 2, [1, 2]],
                }
            ],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual],
        )

    def test__aggregate_concatArrays_exceptions(self):
        self.db.collection.insert_one({'a': {'arr1': [123]}})
        pipeline_parameter_not_array = [
            {'$project': {'concat_parameter_not_array': {'$concatArrays': 42}}}
        ]
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(pipeline_parameter_not_array)

        pipeline_item_not_array = [
            {'$project': {'concat_item_not_array': {'$concatArrays': [[1, 2], '$a']}}}
        ]
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(pipeline_item_not_array)

    def test__aggregate_filter(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many(
            [
                {
                    '_id': 0,
                    'items': [
                        {'item_id': 43, 'quantity': 2, 'price': 10},
                        {'item_id': 2, 'quantity': 1, 'price': 240},
                    ],
                },
                {
                    '_id': 1,
                    'items': [
                        {'item_id': 23, 'quantity': 3, 'price': 110},
                        {'item_id': 103, 'quantity': 4, 'price': 5},
                        {'item_id': 38, 'quantity': 1, 'price': 300},
                    ],
                },
                {
                    '_id': 2,
                    'items': [
                        {'item_id': 4, 'quantity': 1, 'price': 23},
                    ],
                },
            ]
        )

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        'filtered_items': {
                            '$filter': {
                                'input': '$items',
                                'as': 'item',
                                'cond': {'$gte': ['$$item.price', 100]},
                            }
                        }
                    }
                }
            ]
        )
        expect = [
            {
                '_id': 0,
                'filtered_items': [
                    {'item_id': 2, 'quantity': 1, 'price': 240},
                ],
            },
            {
                '_id': 1,
                'filtered_items': [
                    {'item_id': 23, 'quantity': 3, 'price': 110},
                    {'item_id': 38, 'quantity': 1, 'price': 300},
                ],
            },
            {'_id': 2, 'filtered_items': []},
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_filter_wrong_options(self):
        options = [
            3,
            ['$items', {'$gte': ['$$item.price', 100]}],
            {},
            {'input': '$items'},
            {'cond': {'$gte': ['$$item.price', 100]}},
            {'input': '$items', 'cond': {'$$this.filter'}, 'extraOption': 2},
        ]
        self.db.collection.insert_one({})
        for option in options:
            with self.assertRaises(mongomock.OperationFailure, msg=option):
                self.db.collection.aggregate(
                    [{'$project': {'filtered_items': {'$filter': option}}}]
                )

    def test__aggregate_map(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'array': [1, 2, 3, 4],
            }
        )
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'array': {
                            '$map': {
                                'input': '$array',
                                'in': {'$multiply': ['$$this', '$$this']},
                            }
                        },
                        'custom_variable': {
                            '$map': {
                                'input': '$array',
                                'as': 'self',
                                'in': {'$multiply': ['$$self', '$$self']},
                            }
                        },
                        'empty': {
                            '$map': {
                                'input': [],
                                'in': {'$multiply': ['$$this', '$$this']},
                            }
                        },
                        'null': {
                            '$map': {
                                'input': None,
                                'in': '$$this',
                            }
                        },
                        'missing': {
                            '$map': {
                                'input': '$missing.key',
                                'in': '$$this',
                            }
                        },
                    }
                }
            ]
        )
        expect = [
            {
                'array': [1, 4, 9, 16],
                'custom_variable': [1, 4, 9, 16],
                'empty': [],
                'null': None,
                'missing': None,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_index_of_array(self):
        collection = self.db.collection
        collection.insert_one({'array': ['one', 'two', 'three', 'four']})
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'two': {'$indexOfArray': ['$array', 'two']},
                        'three': {'$indexOfArray': ['$array', 'three']},
                        'repeated_value': {'$indexOfArray': [[0, 3, 2, 1, 2], 2]},
                        'repeated_value_from_3': {'$indexOfArray': [[0, 3, 2, 1, 2], 2, 3]},
                        'not_found': {'$indexOfArray': ['$array', 'foo']},
                        'two_from_1': {'$indexOfArray': ['$array', 'two', 1]},
                        'two_from_2': {'$indexOfArray': ['$array', 'two', 2]},
                        'two_from_0_to_1': {'$indexOfArray': ['$array', 'two', 0, 1]},
                        'two_from_0_to_2': {'$indexOfArray': ['$array', 'two', 0, 2]},
                        'two_from_10': {'$indexOfArray': ['$array', 'two', 10]},
                        'two_from_0_to_10': {'$indexOfArray': ['$array', 'two', 0, 10]},
                        'array_literal': {'$indexOfArray': [[1, 2, 3], 3]},
                        'missing': {'$indexOfArray': ['$missing.key', 'foo']},
                        'null': {'$indexOfArray': [None, 'foo']},
                        'element_is_missing_type': {'$indexOfArray': [['$missing.key'], None]},
                    }
                }
            ]
        )
        expect = [
            {
                'two': 1,
                'three': 2,
                'repeated_value': 2,
                'repeated_value_from_3': 4,
                'not_found': -1,
                'two_from_1': 1,
                'two_from_2': -1,
                'two_from_0_to_1': -1,
                'two_from_0_to_2': 1,
                'two_from_10': -1,
                'two_from_0_to_10': 1,
                'array_literal': 2,
                'missing': None,
                'null': None,
                'element_is_missing_type': 0,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__aggregate_index_of_array_errors(self):
        collection = self.db.collection
        collection.insert_one({})
        data = (
            (
                [],
                'Expression $indexOfArray takes at least 2 arguments, and at most '
                '4, but 0 were passed in.',
            ),
            (
                'foo',
                'Expression $indexOfArray takes at least 2 arguments, and at most '
                '4, but 1 were passed in.',
            ),
            (
                [1],
                'Expression $indexOfArray takes at least 2 arguments, and at most '
                '4, but 1 were passed in.',
            ),
            (
                ['foo', 'bar'],
                '$indexOfArray requires an array as a first argument, found:',
            ),
            (
                [[], 'foo', 'bar'],
                '$indexOfArrayrequires an integral starting index, found a value of type:',
            ),
            (
                [[], 'foo', -2],
                '$indexOfArray requires a nonnegative starting index, found: -2',
            ),
            (
                [[], 'foo', 1, -4],
                '$indexOfArray requires a nonnegative ending index, found: -4',
            ),
        )
        for operator, message in data:
            with self.assertRaises(mongomock.OperationFailure) as cm:
                collection.aggregate([{'$project': {'foo': {'$indexOfArray': operator}}}])
            self.assertIn(message, str(cm.exception))

    def test__aggregate_reduce(self):
        collection = self.db.collection
        collection.insert_one({'array': [1, 2, 3, 4], 'val': 5})
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'array': {
                            '$reduce': {
                                'initialValue': 0,
                                'input': '$array',
                                'in': {'$add': ['$$value', '$$this']},
                            }
                        },
                        'using_doc_val': {
                            '$reduce': {
                                'initialValue': '$val',
                                'input': '$array',
                                'in': {'$add': ['$$value', '$$this', '$val']},
                            }
                        },
                        'empty_list': {
                            '$reduce': {
                                'initialValue': 0,
                                'input': [],
                                'in': {'$add': ['$$value', 1]},
                            }
                        },
                        'none': {'$reduce': {'initialValue': 0, 'input': None, 'in': 0}},
                        'nested_reduce': {
                            '$reduce': {
                                'initialValue': 0,
                                'input': {
                                    '$reduce': {
                                        'initialValue': [],
                                        'input': '$array',
                                        'in': {'$concatArrays': ['$$value', ['$$this']]},
                                    }
                                },
                                'in': {'$add': ['$$this', '$$value']},
                            }
                        },
                        'missing_key': {
                            '$reduce': {
                                'input': '$missing.key',
                                'initialValue': 0,
                                'in': '$$this',
                            }
                        },
                    }
                }
            ]
        )
        expected = [
            {
                'array': 10,
                'empty_list': 0,
                'missing_key': None,
                'nested_reduce': 10,
                'none': None,
                'using_doc_val': 35,
            }
        ]
        self.assertEqual(expected, list(actual))

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [{'$project': {'field': {'$reduce': {'initialValue': 0, 'in': 0}}}}]
            )

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [{'$project': {'field': {'$reduce': {'input': [1, 2, 3, 4, 5], 'in': 0}}}}]
            )
        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {
                        '$project': {
                            'field': {'$reduce': {'input': [1, 2, 3, 4, 5], 'initialValue': 12}}
                        }
                    }
                ]
            )

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {
                        '$project': {
                            'field': {
                                '$reduce': {
                                    'input': 'string',
                                    'in': {'$add': ['$$this', '$$value']},
                                    'initialValue': 0,
                                }
                            }
                        }
                    }
                ]
            )

    def test__aggregate_map_errors(self):
        collection = self.db.collection
        collection.insert_one({})
        data = (
            (
                [],
                '$map only supports an object as its argument',
            ),
            (
                {},
                "Missing 'input' parameter to $map",
            ),
            (
                # Check that the following message is raised before the error
                # on the type of input
                {'input': 'foo'},
                "Missing 'in' parameter to $map",
            ),
            (
                # NOTE: actual type is omitted in the expected message because
                # of difference in string representations for types between
                # Python 2 and Python 3.
                # TODO(guludo): We should output the type name that is output
                # by the real mongodb.
                {'input': 'foo', 'in': '$$this'},
                'input to $map must be an array not',
            ),
            (
                {'input': [], 'in': '$$this', 'foo': 1},
                'Unrecognized parameter to $map: foo',
            ),
        )
        for op, msg in data:
            with self.assertRaises(mongomock.OperationFailure) as cm:
                collection.aggregate([{'$project': {'x': {'$map': op}}}])
            self.assertIn(msg, str(cm.exception))

    def test__aggregate_slice(self):
        self.db.collection.drop()
        collection = self.db.collection
        self.db.collection.insert_many(
            [
                {
                    '_id': 0,
                    'items': list(range(10)),
                },
                {
                    '_id': 1,
                    'items': list(range(10, 20)),
                },
                {
                    '_id': 2,
                    'items': list(range(20, 30)),
                },
            ]
        )

        empty = [{'_id': 0, 'slice': []}, {'_id': 1, 'slice': []}, {'_id': 2, 'slice': []}]
        self.assertEqual(
            empty, list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 0]}}}]))
        )

        first_five = [
            {'_id': 0, 'slice': list(range(5))},
            {'_id': 1, 'slice': list(range(10, 15))},
            {'_id': 2, 'slice': list(range(20, 25))},
        ]
        self.assertEqual(
            first_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 5]}}}])),
        )
        self.assertEqual(
            first_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 0, 5]}}}])),
        )
        self.assertEqual(
            first_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -10, 5]}}}])),
        )

        full = [
            {'_id': 0, 'slice': list(range(10))},
            {'_id': 1, 'slice': list(range(10, 20))},
            {'_id': 2, 'slice': list(range(20, 30))},
        ]
        self.assertEqual(
            full, list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 10]}}}]))
        )
        self.assertEqual(
            full,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 10000]}}}])),
        )
        self.assertEqual(
            full,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 0, 10000]}}}])),
        )
        self.assertEqual(
            full, list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -10]}}}]))
        )
        self.assertEqual(
            full,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -10000]}}}])),
        )
        self.assertEqual(
            full,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -10, 10]}}}])),
        )

        last_five = [
            {'_id': 0, 'slice': list(range(5, 10))},
            {'_id': 1, 'slice': list(range(15, 20))},
            {'_id': 2, 'slice': list(range(25, 30))},
        ]
        self.assertEqual(
            last_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', 5, 5]}}}])),
        )
        self.assertEqual(
            last_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -5]}}}])),
        )
        self.assertEqual(
            last_five,
            list(collection.aggregate([{'$project': {'slice': {'$slice': ['$items', -5, 5]}}}])),
        )
        self.assertEqual(
            last_five,
            list(
                collection.aggregate(
                    [
                        {
                            '$project': {
                                'slice': {'$slice': ['$items', {'$add': [2, 3]}, {'$add': [1, 4]}]}
                            }
                        }
                    ]
                )
            ),
        )

    def test__aggregate_slice_wrong(self):
        # inserts an item otherwise the slice is not even evaluated
        self.db.collection.insert_one(
            {
                '_id': 0,
                'items': list(range(10)),
            }
        )
        options = [
            {},
            [],
            [0],
            [0, 0],
            ['$items'],
            ['$items', 0, 0],
            ['$items', 1, 0],
            ['$items', 0, -1],
            ['$items', -1, -1],
            ['items', 0],
            ['items', 'foo'],
            ['items', 0, 'bar'],
            '$items',
        ]
        for option in options:
            with self.assertRaises(mongomock.OperationFailure, msg=option):
                self.db.collection.aggregate([{'$project': {'slice': {'$slice': option}}}])

    def test__aggregate_redact(self):
        self.db.a.insert_many(
            [
                {'_id': 1, 'a': 1, 'b': 2},
                {'_id': 2, 'a': 3, 'b': 4},
                {'_id': 3, 'a': 5, 'b': 6},
                {'_id': 4, 'a': 7, 'b': 8},
                {'_id': 5, 'a': 9, 'b': 10},
            ]
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$redact': {
                        '$cond': {'if': {'$lt': ['$a', 5]}, 'then': '$$KEEP', 'else': '$$PRUNE'}
                    }
                }
            ]
        )
        self.assertEqual([{'_id': 1, 'a': 1, 'b': 2}, {'_id': 2, 'a': 3, 'b': 4}], list(actual))

    def test__aggregate_redact_descend_nested_document(self):
        self.db.a.insert_one(
            {
                '_id': 1,
                'a': 1,
                'b': {'a': 4, 'b': {'a': 10, 'c': 'xyz'}},
                'c': 'xyz',
                'd': [{'a': 1, 'b': 2}, {'a': 8, 'b': 3}, {'b': 4}],
                'e': ['a', 'b', 'c'],
            }
        )
        actual = self.db.a.aggregate(
            [
                {
                    '$redact': {
                        '$cond': {'if': {'$lt': ['$a', 5]}, 'then': '$$DESCEND', 'else': '$$PRUNE'}
                    }
                }
            ]
        )
        self.assertEqual(
            [
                {
                    '_id': 1,
                    'a': 1,
                    'b': {'a': 4, 'b': None},
                    'c': 'xyz',
                    'd': [
                        {'a': 1, 'b': 2},
                    ],
                    'e': ['a', 'b', 'c'],
                }
            ],
            list(actual),
        )

    def test__write_concern(self):
        self.assertEqual({}, self.db.collection.write_concern.document)
        self.assertTrue(self.db.collection.write_concern.is_server_default)
        self.assertTrue(self.db.collection.write_concern.acknowledged)

        collection = self.db.get_collection(
            'a', write_concern=WriteConcern(w=2, wtimeout=100, j=True, fsync=False)
        )
        self.assertEqual(
            {
                'fsync': False,
                'j': True,
                'w': 2,
                'wtimeout': 100,
            },
            collection.write_concern.document,
        )

        # http://api.mongodb.com/python/current/api/pymongo/write_concern.html#pymongo.write_concern.WriteConcern.document
        collection.write_concern.document.pop('wtimeout')
        self.assertEqual(
            {
                'fsync': False,
                'j': True,
                'w': 2,
                'wtimeout': 100,
            },
            collection.write_concern.document,
            msg='Write concern is immutable',
        )

    def test__read_preference_default(self):
        # Test various properties of the default read preference.
        self.assertEqual(0, self.db.collection.read_preference.mode)
        self.assertEqual('primary', self.db.collection.read_preference.mongos_mode)
        self.assertEqual({'mode': 'primary'}, self.db.collection.read_preference.document)
        self.assertEqual('Primary', self.db.collection.read_preference.name)
        self.assertEqual([{}], self.db.collection.read_preference.tag_sets)
        self.assertEqual(-1, self.db.collection.read_preference.max_staleness)
        self.assertEqual(0, self.db.collection.read_preference.min_wire_version)

        collection = self.db.get_collection('a', read_preference=self.db.collection.read_preference)
        self.assertEqual('primary', collection.read_preference.mongos_mode)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__read_preference(self):
        collection = self.db.get_collection('a', read_preference=ReadPreference.NEAREST)
        self.assertEqual('nearest', collection.read_preference.mongos_mode)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_unordered_with_bulk_write(self):
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            self.db.collection.bulk_write(
                [
                    pymongo.InsertOne({'_id': 1}),
                    pymongo.InsertOne({'_id': 2}),
                    pymongo.InsertOne({'_id': 1}),
                    pymongo.InsertOne({'_id': 3}),
                    pymongo.InsertOne({'_id': 1}),
                ],
                ordered=False,
            )

        self.assertCountEqual([1, 2, 3], [d['_id'] for d in self.db.collection.find()])
        self.assertEqual(3, err_context.exception.details['nInserted'])
        self.assertEqual([2, 4], [e['index'] for e in err_context.exception.details['writeErrors']])

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_ordered_with_bulk_write(self):
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            self.db.collection.bulk_write(
                [
                    pymongo.InsertOne({'_id': 1}),
                    pymongo.InsertOne({'_id': 2}),
                    pymongo.InsertOne({'_id': 1}),
                    pymongo.InsertOne({'_id': 3}),
                    pymongo.InsertOne({'_id': 1}),
                ]
            )

        self.assertCountEqual([1, 2], [d['_id'] for d in self.db.collection.find()])
        self.assertEqual(2, err_context.exception.details['nInserted'])
        self.assertEqual([2], [e['index'] for e in err_context.exception.details['writeErrors']])

    def test__set_union(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'array': ['one', 'three']},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'array': {'$setUnion': [['one', 'two'], '$array']},
                        'distinct': {'$setUnion': [['one', 'two'], ['three'], ['four']]},
                        'nested': {'$setUnion': [['one', 'two'], [['one', 'two']]]},
                        'objects': {'$setUnion': [[{'a': 1}, {'b': 2}], [{'a': 1}, {'c': 3}]]},
                    }
                }
            ]
        )
        expect = [
            {
                'array': ['one', 'two', 'three'],
                'distinct': ['one', 'two', 'three', 'four'],
                'nested': ['one', 'two', ['one', 'two']],
                'objects': [{'a': 1}, {'b': 2}, {'c': 3}],
            }
        ]
        result = list(actual)
        self.assertEqual(len(expect), len(result))
        for key in expect[0]:
            self.assertCountEqual(expect[0][key], result[0][key])

    def test__set_equals(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'array': ['one', 'three']},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'same_array': {'$setEquals': ['$array', '$array']},
                        'eq_array': {'$setEquals': [['one', 'three'], '$array']},
                        'ne_array': {'$setEquals': [['one', 'two'], '$array']},
                        'eq_in_another_order': {'$setEquals': [['one', 'two'], ['two', 'one']]},
                        'ne_in_another_order': {
                            '$setEquals': [['one', 'two'], ['three', 'one', 'two']]
                        },
                        'three_equal': {
                            '$setEquals': [['one', 'two'], ['two', 'one'], ['one', 'two']]
                        },
                        'three_not_equal': {
                            '$setEquals': [['one', 'three'], ['two', 'one'], ['two', 'one']]
                        },
                    }
                }
            ]
        )
        expect = [
            {
                'same_array': True,
                'eq_array': True,
                'ne_array': False,
                'eq_in_another_order': True,
                'ne_in_another_order': False,
                'three_equal': True,
                'three_not_equal': False,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__set_intersection(self):
        collection = self.db.collection
        collection.insert_many([{'array': ['one', 'three']}])
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'array': {'$setIntersection': [['one', 'two'], '$array']},
                        'distinct': {'$setIntersection': [['one', 'two'], ['three'], ['four']]},
                        'nested': {'$setIntersection': [['one', 'two'], [['one', 'two']]]},
                        'objects': {
                            '$setIntersection': [[{'a': 1}, {'b': 2}], [{'a': 1}, {'c': 3}]]
                        },
                        'empty': {'$setIntersection': []},
                        'missing': {'$setIntersection': [[1], '$missing.key']},
                        'null': {'$setIntersection': [[1], None]},
                    }
                }
            ]
        )
        expect = [
            {
                'array': ['one'],
                'distinct': [],
                'nested': [],
                'objects': [{'a': 1}],
                'empty': [],
                'missing': None,
                'null': None,
            }
        ]
        self.assertEqual(len(expect[0]['array']), len(next(iter(actual))['array']))

    def test__set_difference(self):
        collection = self.db.collection
        collection.insert_many([{'array': ['one', 'three']}])
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'diff': {'$setDifference': [['one', 'two', 'three'], '$array']},
                        'empty_diff': {'$setDifference': [['one'], ['one']]},
                        'no_overlap': {'$setDifference': [['one'], ['two']]},
                    }
                }
            ]
        )
        expect = [
            {
                'diff': ['two'],
                'empty_diff': [],
                'no_overlap': ['one'],
            }
        ]
        self.assertEqual(len(expect[0]['diff']), len(next(iter(actual))['diff']))

    def test__set_is_subset(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {
                    'array': ['one', 'three'],
                    'nested_array': [{'a': 'b', 'c': 'd'}],
                }
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'same_array': {'$setIsSubset': ['$array', '$array']},
                        'eq_array': {'$setIsSubset': [['one', 'three'], '$array']},
                        'ne_array': {'$setIsSubset': [['one', 'two'], '$array']},
                        'eq_in_another_order': {'$setIsSubset': [['one', 'two'], ['two', 'one']]},
                        'ne_in_another_order': {
                            '$setIsSubset': [['one', 'two'], ['three', 'one', 'two']]
                        },
                        'same_nested_array': {'$setIsSubset': ['$nested_array', '$nested_array']},
                        'eq_nested_array': {
                            '$setIsSubset': [[{'a': 'b'}, {'c': 'd'}], [{'a': 'b'}, {'c': 'd'}]],
                        },
                        'ne_nested_array': {
                            '$setIsSubset': [[{'a': 'b'}, {'c': 'e'}], [{'a': 'b'}, {'c': 'd'}]],
                        },
                    }
                }
            ]
        )
        expect = [
            {
                'same_array': True,
                'eq_array': True,
                'ne_array': False,
                'eq_in_another_order': True,
                'ne_in_another_order': True,
                'same_nested_array': True,
                'eq_nested_array': True,
                'ne_nested_array': False,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__any_element_true(self):
        collection = self.db.collection
        collection.insert_many([{}])
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'all_truthy': {'$anyElementTrue': [[1, 2, 3]]},
                        'some_falsy': {'$anyElementTrue': [[0, 1]]},
                        'all_falsy': {'$anyElementTrue': [[None, 0]]},
                        'empty': {'$anyElementTrue': [[]]},
                    }
                }
            ]
        )
        expect = [
            {
                'all_truthy': True,
                'some_falsy': True,
                'all_falsy': False,
                'empty': False,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__all_elements_true(self):
        collection = self.db.collection
        collection.insert_many([{}])
        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'all_truthy': {'$allElementsTrue': [[1, 2, 3]]},
                        'some_falsy': {'$allElementsTrue': [[0, 1]]},
                        'all_falsy': {'$allElementsTrue': [[None, 0]]},
                        'empty': {'$allElementsTrue': [[]]},
                    }
                }
            ]
        )
        expect = [
            {
                'all_truthy': True,
                'some_falsy': False,
                'all_falsy': False,
                'empty': True,
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__add_to_set_missing_value(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'key1': 'a', 'my_key': 1},
                {'key1': 'a'},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$group': {
                        '_id': {'key1': '$key1'},
                        'my_keys': {'$addToSet': '$my_key'},
                    }
                }
            ]
        )
        expect = [
            {
                '_id': {'key1': 'a'},
                'my_keys': [1],
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__add_to_set_falsey_values(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'key1': 'a', 'my_key': 0},
                {'key1': 'a', 'my_key': ''},
                {'key1': 'a', 'my_key': None},
            ]
        )
        actual = collection.aggregate(
            [
                {
                    '$group': {
                        '_id': {'key1': '$key1'},
                        'my_keys': {'$addToSet': '$my_key'},
                    }
                }
            ]
        )
        expect = [
            {
                '_id': {'key1': 'a'},
                'my_keys': [0, '', None],
            }
        ]
        self.assertEqual(expect, list(actual))

    def test__not_implemented_operator(self):
        collection = self.db.collection
        with self.assertRaises(NotImplementedError):
            collection.find_one({'field': {'$bitsAllClear': 5}})

    def test__not_implemented_methods(self):
        collection = self.db.collection
        with self.assertRaises(NotImplementedError):
            collection.find_raw_batches()
        with self.assertRaises(NotImplementedError):
            collection.aggregate_raw_batches([{'$unwind': '$phones'}])

    def test__insert_do_not_modify_input(self):
        collection = self.db.collection
        document = {
            'number': 3,
            'object': {'a': 1},
            'date': datetime(2000, 1, 1, 12, 30, 30, 12745, tzinfo=UTCPlus2()),
        }
        original_document = copy.deepcopy(document)

        collection.insert_one(document)

        self.assertNotEqual(original_document, document, msg='Document has been modified')

        self.assertEqual(
            dict(original_document, _id=None),
            dict(document, _id=None),
            msg='The only modification is adding the _id.',
        )

        # Comparing stored document and the original one: the dates are modified.
        stored_document = collection.find_one()
        del stored_document['_id']
        self.assertEqual(dict(original_document, date=None), dict(stored_document, date=None))
        self.assertNotEqual(
            original_document,
            stored_document,
            msg='The document is not the same because the date TZ has been stripped of and the '
            'microseconds truncated.',
        )
        self.assertNotEqual(
            original_document['date'].timestamp(), stored_document['date'].timestamp()
        )
        self.assertEqual(
            datetime(2000, 1, 1, 10, 30, 30, 12000),
            stored_document['date'],
            msg='The stored document holds a date as timezone naive UTC and without microseconds',
        )

        # The objects are not linked: modifying the inserted document or the fetched one will
        # have no effect on future retrievals.
        document['object']['new_key'] = 42
        fetched_document = stored_document
        fetched_document['object']['new_key'] = 'post-find'

        stored_document = collection.find_one()
        del stored_document['_id']
        self.assertNotEqual(
            document,
            stored_document,
            msg='Modifying the inserted document afterwards does not modify the stored document.',
        )
        self.assertNotEqual(
            fetched_document,
            stored_document,
            msg='Modifying the found document afterwards does not modify the stored document.',
        )
        self.assertEqual(dict(original_document, date=None), dict(stored_document, date=None))

    def test__clone_document_isolation(self):
        doc = {'a': 1, 'b': {'c': 2}, 'd': [3, 4]}
        cloned = helpers._clone_document(doc)
        cloned['b']['c'] = 99
        cloned['d'].append(5)
        self.assertEqual(doc, {'a': 1, 'b': {'c': 2}, 'd': [3, 4]})

    def test__clone_document_bson_types(self):
        doc = {'nested': {'x': 1}, 'items': [1, 2, 3]}
        cloned = helpers._clone_document(doc)
        self.assertEqual(cloned, doc)
        cloned['nested']['x'] = 99
        self.assertEqual(doc['nested']['x'], 1)

    def test__clone_document_tuple(self):
        doc = {'t': (1, 2, 3)}
        cloned = helpers._clone_document(doc)
        self.assertEqual(cloned, {'t': (1, 2, 3)})
        self.assertIsNot(cloned['t'], doc['t'])

    def test__clone_document_set(self):
        doc = {'s': {1, 2, 3}}
        cloned = helpers._clone_document(doc)
        self.assertEqual(cloned, {'s': {1, 2, 3}})
        self.assertIsNot(cloned['s'], doc['s'])

    def test__clone_document_unknown_type(self):
        class CustomType:
            def __init__(self, v):
                self.v = v

        obj = CustomType(42)
        cloned = helpers._clone_document({'x': obj})
        self.assertIsNot(cloned['x'], obj)
        self.assertEqual(cloned['x'].v, 42)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__clone_document_objectid(self):
        from bson import ObjectId as BsonObjectId

        oid = BsonObjectId()
        doc = {'_id': oid}
        cloned = helpers._clone_document(doc)
        self.assertEqual(cloned['_id'], oid)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__clone_document_decimal128(self):
        from bson.decimal128 import Decimal128

        val = Decimal128('10.5')
        doc = {'price': val}
        cloned = helpers._clone_document(doc)
        self.assertEqual(cloned['price'], val)

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_to_string(self):
        collection = self.db.collection
        collection.insert_one(
            {
                '_id': ObjectId('5dd6a8f302c91829ef248162'),
                'boolean_true': True,
                'boolean_false': False,
                'integer': 100,
                'date': datetime(2018, 3, 27, 0, 58, 51, 538000),
            }
        )

        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        '_id': {'$toString': '$_id'},
                        'boolean_true': {'$toString': '$boolean_true'},
                        'boolean_false': {'$toString': '$boolean_false'},
                        'integer': {'$toString': '$integer'},
                        'date': {'$toString': '$date'},
                        'none': {'$toString': '$notexist'},
                    }
                }
            ]
        )
        expect = [
            {
                '_id': '5dd6a8f302c91829ef248162',
                'boolean_true': 'true',
                'boolean_false': 'false',
                'integer': '100',
                'date': '2018-03-27T00:58:51.538Z',
                'none': None,
            }
        ]
        self.assertEqual(expect, list(actual))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_to_decimal(self):
        collection = self.db.collection
        collection.insert_one(
            {
                '_id': ObjectId('5dd6a8f302c91829ef248161'),
                'boolean_true': True,
                'boolean_false': False,
                'integer': 100,
                'double': 1.999,
                'decimal': decimal128.Decimal128('5.5000'),
                'str_base_10_numeric': '123',
                'str_negative_number': '-23',
                'str_decimal_number': '1.99',
                'str_not_numeric': '123a123',
                'datetime': datetime(1970, 1, 1),
            }
        )
        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'boolean_true': {'$toDecimal': '$boolean_true'},
                        'boolean_false': {'$toDecimal': '$boolean_false'},
                        'integer': {'$toDecimal': '$integer'},
                        'double': {'$toDecimal': '$double'},
                        'decimal': {'$toDecimal': '$decimal'},
                        'str_base_10_numeric': {'$toDecimal': '$str_base_10_numeric'},
                        'str_negative_number': {'$toDecimal': '$str_negative_number'},
                        'str_decimal_number': {'$toDecimal': '$str_decimal_number'},
                        'datetime': {'$toDecimal': '$datetime'},
                        'not_exist_field': {'$toDecimal': '$not_exist_field'},
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        expect = [
            {
                'boolean_true': decimal128.Decimal128('1'),
                'boolean_false': decimal128.Decimal128('0'),
                'integer': decimal128.Decimal128('100'),
                'double': decimal128.Decimal128('1.99900000000000'),
                'decimal': decimal128.Decimal128('5.5000'),
                'str_base_10_numeric': decimal128.Decimal128('123'),
                'str_negative_number': decimal128.Decimal128('-23'),
                'str_decimal_number': decimal128.Decimal128('1.99'),
                'str_not_numeric': '123a123',
                'datetime': decimal128.Decimal128('0'),
                'not_exist_field': None,
            }
        ]
        self.assertEqual(expect, list(actual))

        with self.assertRaises(mongomock.OperationFailure):
            collection.aggregate(
                [
                    {'$addFields': {'str_not_numeric': {'$toDecimal': '$str_not_numeric'}}},
                    {'$project': {'_id': 0}},
                ]
            )
        with self.assertRaises(TypeError):
            collection.aggregate(
                [{'$addFields': {'_id': {'$toDecimal': '$_id'}}}, {'$project': {'_id': 0}}]
            )

    @skipIf(helpers.HAVE_PYMONGO, 'pymongo installed')
    def test__aggregate_to_decimal_without_pymongo(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'boolean_true': True,
                'boolean_false': False,
            }
        )
        with self.assertRaises(NotImplementedError):
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'boolean_true': {'$toDecimal': '$boolean_true'},
                            'boolean_false': {'$toDecimal': '$boolean_false'},
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_to_int(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'boolean_true': True,
                'boolean_false': False,
                'integer': 100,
                'double': 1.999,
                'decimal': decimal128.Decimal128('5.5000'),
            }
        )
        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'boolean_true': {'$toInt': '$boolean_true'},
                        'boolean_false': {'$toInt': '$boolean_false'},
                        'integer': {'$toInt': '$integer'},
                        'double': {'$toInt': '$double'},
                        'decimal': {'$toInt': '$decimal'},
                        'not_exist': {'$toInt': '$not_exist'},
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        expect = [
            {
                'boolean_true': 1,
                'boolean_false': 0,
                'integer': 100,
                'double': 1,
                'decimal': 5,
                'not_exist': None,
            }
        ]
        self.assertEqual(expect, list(actual))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_to_long(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'boolean_true': True,
                'boolean_false': False,
                'integer': 100,
                'double': 1.999,
                'decimal': decimal128.Decimal128('5.5000'),
            }
        )
        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'boolean_true': {'$toLong': '$boolean_true'},
                        'boolean_false': {'$toLong': '$boolean_false'},
                        'integer': {'$toLong': '$integer'},
                        'double': {'$toLong': '$double'},
                        'decimal': {'$toLong': '$decimal'},
                        'not_exist': {'$toLong': '$not_exist'},
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        expect = [
            {
                'boolean_true': 1,
                'boolean_false': 0,
                'integer': 100,
                'double': 1,
                'decimal': 5,
                'not_exist': None,
            }
        ]
        self.assertEqual(expect, list(actual))

    @skipIf(helpers.HAVE_PYMONGO, 'pymongo installed')
    def test__aggregate_to_long_no_pymongo(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_one(
            {
                'double': 1.999,
            }
        )
        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'double': {'$toLong': '$double'},
                            }
                        },
                        {'$project': {'_id': 0}},
                    ]
                )
            )

    def test__aggregate_to_object_id(self):
        collection = self.db.collection
        object_id_str = '507f1f77bcf86cd799439011'
        object_id = ObjectId(object_id_str)

        collection.insert_one(
            {
                '_id': 1,
                'string_id': object_id_str,
                'object_id': object_id,
                'null_value': None,
            }
        )

        # Test basic conversion from string to ObjectId
        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'converted_from_string': {'$toObjectId': '$string_id'},
                        'converted_from_object_id': {'$toObjectId': '$object_id'},
                        'converted_from_null': {'$toObjectId': '$null_value'},
                        'converted_from_missing': {'$toObjectId': '$missing_field'},
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        result = list(actual)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0]['converted_from_string'], ObjectId)
        self.assertEqual(str(result[0]['converted_from_string']), object_id_str)
        self.assertIsInstance(result[0]['converted_from_object_id'], ObjectId)
        self.assertEqual(result[0]['converted_from_object_id'], object_id)
        self.assertIsNone(result[0]['converted_from_null'])
        self.assertIsNone(result[0]['converted_from_missing'])

        # Test the user's specific use case
        collection.drop()
        collection.insert_one(
            {
                '_id': 'assignments',
                'entity': 'entity',
                'entity_id': object_id_str,
            }
        )

        actual = collection.aggregate(
            [
                {'$match': {'_id': {'$in': ['assignments']}, 'entity': 'entity'}},
                {'$addFields': {'entity_id': {'$toObjectId': '$entity_id'}}},
            ]
        )
        result = list(actual)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0]['entity_id'], ObjectId)
        self.assertEqual(str(result[0]['entity_id']), object_id_str)

        # Test error cases
        collection.drop()
        collection.insert_one({'_id': 1, 'invalid_string': 'not_a_valid_objectid'})

        with self.assertRaises(mongomock.OperationFailure) as context:
            list(
                collection.aggregate(
                    [{'$addFields': {'converted': {'$toObjectId': '$invalid_string'}}}]
                )
            )
        self.assertIn('Failed to parse objectId', str(context.exception))

        # Test with integer (should raise error)
        collection.drop()
        collection.insert_one({'_id': 1, 'number': 123})

        with self.assertRaises(mongomock.OperationFailure) as context:
            list(collection.aggregate([{'$addFields': {'converted': {'$toObjectId': '$number'}}}]))
        self.assertIn('requires a string, ObjectId, or null input', str(context.exception))

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_date_to_string(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'start_date': datetime(2011, 11, 4, 0, 5, 23),
            }
        )
        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'start_date': {
                            '$dateToString': {'format': '%Y/%m/%d %H:%M', 'date': '$start_date'}
                        }
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )
        expect = [
            {
                'start_date': '2011/11/04 00:05',
            }
        ]
        self.assertEqual(expect, list(actual))

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'a': {'$dateToString': {'date': datetime.now(), 'format': '%L'}}
                        }
                    },
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'a': {
                                '$dateToString': {
                                    'date': datetime.now(),
                                    'format': '%m',
                                    'onNull': 'a',
                                }
                            }
                        }
                    },
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'a': {
                                '$dateToString': {
                                    'date': datetime.now(),
                                    'format': '%m',
                                    'timezone': 'America/New_York',
                                }
                            }
                        }
                    },
                ]
            )

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'a': {
                                '$dateToString': {
                                    'date': datetime.now(),
                                }
                            }
                        }
                    },
                ]
            )

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(
                [
                    {
                        '$project': {
                            'a': {
                                '$dateToString': {
                                    'format': '%m',
                                }
                            }
                        }
                    },
                ]
            )

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'a': {'$dateToString': '10'}}}])

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    @skipIf(version.parse('5.0') > SERVER_VERSION, '$dateAdd is not supported prior to MongoDB 5.0')
    def test__aggregate_date_add_and_subtract(self):
        collection = self.db.collection
        start = datetime(2022, 11, 6, 20, 4, 1, 123000)
        collection.insert_one({})

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'add_month': {
                                '$dateAdd': {'startDate': start, 'unit': 'month', 'amount': 1}
                            },
                            'add_quarter': {
                                '$dateAdd': {'startDate': start, 'unit': 'quarter', 'amount': 1}
                            },
                            'subtract_hour': {
                                '$dateSubtract': {
                                    'startDate': start,
                                    'unit': 'hour',
                                    'amount': 2,
                                }
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'add_month': datetime(2022, 12, 6, 20, 4, 1, 123000),
                    'add_quarter': datetime(2023, 2, 6, 20, 4, 1, 123000),
                    'subtract_hour': datetime(2022, 11, 6, 18, 4, 1, 123000),
                }
            ],
            actual,
        )

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateAdd': {
                                        'startDate': start,
                                        'unit': 'day',
                                        'amount': 1.5,
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'add_millisecond': {
                                '$dateAdd': {
                                    'startDate': start,
                                    'unit': 'millisecond',
                                    'amount': 7,
                                }
                            },
                            'add_second': {
                                '$dateAdd': {'startDate': start, 'unit': 'second', 'amount': 8}
                            },
                            'add_minute': {
                                '$dateAdd': {'startDate': start, 'unit': 'minute', 'amount': 9}
                            },
                            'add_day': {
                                '$dateAdd': {'startDate': start, 'unit': 'day', 'amount': 10}
                            },
                            'add_week': {
                                '$dateAdd': {'startDate': start, 'unit': 'week', 'amount': 2}
                            },
                            'add_year': {
                                '$dateAdd': {'startDate': start, 'unit': 'year', 'amount': 1}
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'add_millisecond': datetime(2022, 11, 6, 20, 4, 1, 130000),
                    'add_second': datetime(2022, 11, 6, 20, 4, 9, 123000),
                    'add_minute': datetime(2022, 11, 6, 20, 13, 1, 123000),
                    'add_day': datetime(2022, 11, 16, 20, 4, 1, 123000),
                    'add_week': datetime(2022, 11, 20, 20, 4, 1, 123000),
                    'add_year': datetime(2023, 11, 6, 20, 4, 1, 123000),
                }
            ],
            actual,
        )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateAdd': {
                                        'startDate': start,
                                        'unit': 'day',
                                        'amount': 1,
                                        'timezone': 'UTC',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(mongomock.OperationFailure):
            list(collection.aggregate([{'$addFields': {'bad': {'$dateAdd': 'bad'}}}]))

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateAdd': {
                                        'startDate': start,
                                        'unit': 'nope',
                                        'amount': 1,
                                    }
                                }
                            }
                        }
                    ]
                )
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$dateDiff is not supported prior to MongoDB 5.0',
    )
    def test__aggregate_date_diff(self):
        collection = self.db.collection
        collection.insert_one({})
        start = datetime(2022, 11, 7, 12, 54, 32, 543000)

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'milliseconds': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': start + timedelta(milliseconds=123),
                                    'unit': 'millisecond',
                                }
                            },
                            'hours': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': start + timedelta(hours=56),
                                    'unit': 'hour',
                                }
                            },
                            'quarters': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': datetime(2023, 5, 7, 12, 54, 32, 543000),
                                    'unit': 'quarter',
                                }
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual([{'milliseconds': 123, 'hours': 56, 'quarters': 2}], actual)

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'weeks': {
                                    '$dateDiff': {
                                        'startDate': start,
                                        'endDate': start + timedelta(days=14),
                                        'unit': 'week',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'seconds': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': start + timedelta(seconds=22),
                                    'unit': 'second',
                                }
                            },
                            'minutes': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': start + timedelta(minutes=17),
                                    'unit': 'minute',
                                }
                            },
                            'days': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': start + timedelta(days=28),
                                    'unit': 'day',
                                }
                            },
                            'months': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': datetime(2023, 1, 7, 12, 54, 32, 543000),
                                    'unit': 'month',
                                }
                            },
                            'years': {
                                '$dateDiff': {
                                    'startDate': start,
                                    'endDate': datetime(2025, 11, 7, 12, 54, 32, 543000),
                                    'unit': 'year',
                                }
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual(
            [{'seconds': 22, 'minutes': 17, 'days': 28, 'months': 2, 'years': 3}],
            actual,
        )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateDiff': {
                                        'startDate': start,
                                        'endDate': start + timedelta(days=1),
                                        'unit': 'day',
                                        'timezone': 'UTC',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateDiff': {
                                        'startDate': start,
                                        'endDate': start + timedelta(days=1),
                                        'unit': 'day',
                                        'startOfWeek': 'monday',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(mongomock.OperationFailure):
            list(collection.aggregate([{'$addFields': {'bad': {'$dateDiff': 'bad'}}}]))

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateDiff': {
                                        'startDate': start,
                                        'endDate': start + timedelta(days=1),
                                        'unit': 'nope',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    @skipIf(
        version.parse('5.0') > SERVER_VERSION,
        '$dateTrunc is not supported prior to MongoDB 5.0',
    )
    def test__aggregate_date_trunc(self):
        collection = self.db.collection
        collection.insert_one({'start_date': datetime(2011, 11, 4, 15, 6, 7, 890123)})

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'day': {'$dateTrunc': {'date': '$start_date', 'unit': 'day'}},
                            'month': {'$dateTrunc': {'date': '$start_date', 'unit': 'month'}},
                            'year': {'$dateTrunc': {'date': '$start_date', 'unit': 'year'}},
                        }
                    },
                    {'$project': {'_id': 0, 'start_date': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'day': datetime(2011, 11, 4, 0, 0),
                    'month': datetime(2011, 11, 1, 0, 0),
                    'year': datetime(2011, 1, 1, 0, 0),
                }
            ],
            actual,
        )

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'millisecond': {
                                '$dateTrunc': {'date': '$start_date', 'unit': 'millisecond'}
                            },
                            'second': {'$dateTrunc': {'date': '$start_date', 'unit': 'second'}},
                            'minute': {'$dateTrunc': {'date': '$start_date', 'unit': 'minute'}},
                            'hour': {'$dateTrunc': {'date': '$start_date', 'unit': 'hour'}},
                            'week': {'$dateTrunc': {'date': '$start_date', 'unit': 'week'}},
                            'quarter': {'$dateTrunc': {'date': '$start_date', 'unit': 'quarter'}},
                        }
                    },
                    {'$project': {'_id': 0, 'start_date': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'millisecond': datetime(2011, 11, 4, 15, 6, 7, 890000),
                    'second': datetime(2011, 11, 4, 15, 6, 7),
                    'minute': datetime(2011, 11, 4, 15, 6),
                    'hour': datetime(2011, 11, 4, 15, 0),
                    'week': datetime(2011, 10, 30, 0, 0),
                    'quarter': datetime(2011, 10, 1, 0, 0),
                }
            ],
            actual,
        )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateTrunc': {
                                        'date': '$start_date',
                                        'unit': 'day',
                                        'timezone': 'UTC',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateTrunc': {
                                        'date': '$start_date',
                                        'unit': 'day',
                                        'startOfWeek': 'monday',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateTrunc': {
                                        'date': '$start_date',
                                        'unit': 'day',
                                        'binSize': 2,
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(mongomock.OperationFailure):
            list(collection.aggregate([{'$addFields': {'bad': {'$dateTrunc': 'bad'}}}]))

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {'$dateTrunc': {'date': '$start_date', 'unit': 'nope'}}
                            }
                        }
                    ]
                )
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_date_from_string(self):
        collection = self.db.collection
        collection.insert_one({})

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'parsed': {'$dateFromString': {'dateString': '2023-01-15T10:30:00Z'}},
                            'on_null': {
                                '$dateFromString': {'dateString': None, 'onNull': 'missing'}
                            },
                            'on_error': {
                                '$dateFromString': {'dateString': 'not-a-date', 'onError': 'bad'}
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'parsed': datetime(2023, 1, 15, 10, 30, 0),
                    'on_null': 'missing',
                    'on_error': 'bad',
                }
            ],
            actual,
        )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'parsed': {
                                    '$dateFromString': {
                                        'dateString': '2023-01-15',
                                        'format': '%Y-%m-%d',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        actual = list(
            collection.aggregate(
                [
                    {
                        '$addFields': {
                            'naive': {'$dateFromString': {'dateString': '2023-01-15T10:30:00'}},
                            'offset': {
                                '$dateFromString': {'dateString': '2023-01-15T10:30:00+02:00'}
                            },
                        }
                    },
                    {'$project': {'_id': 0}},
                ]
            )
        )

        self.assertEqual(
            [
                {
                    'naive': datetime(2023, 1, 15, 10, 30, 0),
                    'offset': datetime(2023, 1, 15, 8, 30, 0),
                }
            ],
            actual,
        )

        with self.assertRaises(NotImplementedError):
            list(
                collection.aggregate(
                    [
                        {
                            '$addFields': {
                                'bad': {
                                    '$dateFromString': {
                                        'dateString': '2023-01-15',
                                        'timezone': 'UTC',
                                    }
                                }
                            }
                        }
                    ]
                )
            )

        with self.assertRaises(mongomock.OperationFailure):
            list(collection.aggregate([{'$addFields': {'bad': {'$dateFromString': 'bad'}}}]))

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [{'$addFields': {'bad': {'$dateFromString': {'dateString': 123}}}}]
                )
            )

        with self.assertRaises(mongomock.OperationFailure):
            list(
                collection.aggregate(
                    [{'$addFields': {'bad': {'$dateFromString': {'dateString': 'not-a-date'}}}}]
                )
            )

    @skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_date_from_parts(self):
        collection = self.db.collection
        collection.insert_one(
            {
                'start_date': datetime(2022, 8, 3, 0, 5, 23),
            }
        )

        actual = collection.aggregate(
            [
                {
                    '$addFields': {
                        'start_date': {
                            '$dateFromParts': {
                                'year': {'$year': '$start_date'},
                                'month': {'$month': '$start_date'},
                                'day': {'$dayOfMonth': '$start_date'},
                            }
                        }
                    }
                },
                {'$project': {'_id': 0}},
            ]
        )

        expect = [
            {
                'start_date': datetime(2022, 8, 3),
            }
        ]

        self.assertEqual(expect, list(actual))

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'start_date': {
                                '$dateFromParts': {
                                    'day': 1,
                                }
                            }
                        }
                    }
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'start_date': {
                                '$dateFromParts': {
                                    'isoWeekYear': 1,
                                }
                            }
                        }
                    }
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'start_date': {
                                '$dateFromParts': {
                                    'isoWeekYear': 1,
                                    'isoWeek': 53,
                                }
                            }
                        }
                    }
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'start_date': {
                                '$dateFromParts': {
                                    'isoWeekYear': 1,
                                    'isoDayOfWeek': 7,
                                }
                            }
                        }
                    }
                ]
            )

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [
                    {
                        '$addFields': {
                            'start_date': {
                                '$dateFromParts': {
                                    'year': {'$year': '$start_date'},
                                    'timezone': 'America/New_York',
                                }
                            }
                        }
                    }
                ]
            )

    def test__aggregate_array_to_object(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'items': [['a', 1], ['b', 2], ['c', 3], ['a', 4]]},
                {'items': (['a', 1], ['b', 2], ['c', 3], ['a', 4])},
                {'items': [('a', 1), ('b', 2), ('c', 3), ('a', 4)]},
                {'items': (('a', 1), ('b', 2), ('c', 3), ('a', 4))},
                {'items': [['a', 1], ('b', 2), ['c', 3], ('a', 4)]},
                {'items': (['a', 1], ('b', 2), ['c', 3], ('a', 4))},
                {
                    'items': [
                        {'k': 'a', 'v': 1},
                        {'k': 'b', 'v': 2},
                        {'k': 'c', 'v': 3},
                        {'k': 'a', 'v': 4},
                    ],
                },
                {
                    'items': [],
                },
                {
                    'items': (),
                },
                {
                    'items': None,
                },
            ]
        )

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        'items': {'$arrayToObject': '$items'},
                        'not_exists': {'$arrayToObject': '$nothing'},
                        '_id': 0,
                    }
                }
            ]
        )

        expect = [
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {'a': 4, 'b': 2, 'c': 3}, 'not_exists': None},
            {'items': {}, 'not_exists': None},
            {'items': {}, 'not_exists': None},
            {'items': None, 'not_exists': None},
        ]
        self.assertEqual(expect, list(actual))

        # All of these items should trigger an error
        items = [
            [
                {'$addFields': {'items': ''}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': 100}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [['a', 'b', 'c'], ['d', 2]]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [['a'], ['b', 2]]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [[]]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [{'k': 'a', 'v': 1, 't': 't'}, {'k': 'b', 'v': 2}]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [{'v': 1, 't': 't'}]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [{}]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [['a', 1], {'k': 'b', 'v': 2}]}},
                {'$project': {'items': {'$arrayToObject': '$items'}, '_id': 0}},
            ],
        ]

        for item in items:
            with self.assertRaises(mongomock.OperationFailure):
                collection.aggregate(item)

    def test_aggregate_object_to_array(self):
        collection = self.db.collection

        collection.insert_many(
            [
                {'items': None},
                {'items': {'qty': 25}},
                {
                    'items': {
                        'size': {'len': 25, 'w': 10, 'uom': 'cm'},
                    }
                },
            ]
        )

        expect = [
            {'items': None, 'not_exists': None},
            {
                'items': [
                    {'k': 'qty', 'v': 25},
                ],
                'not_exists': None,
            },
            {
                'items': [
                    {'k': 'size', 'v': {'len': 25, 'w': 10, 'uom': 'cm'}},
                ],
                'not_exists': None,
            },
        ]

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        'items': {'$objectToArray': '$items'},
                        'not_exists': {'$objectToArray': '$nothing'},
                        '_id': 0,
                    }
                }
            ]
        )
        self.assertEqual(expect, list(actual))

        # All of these items should trigger an error
        items = [
            [
                {'$addFields': {'items': ''}},
                {'$project': {'items': {'$objectToArray': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': 100}},
                {'$project': {'items': {'$objectToArray': '$items'}, '_id': 0}},
            ],
            [
                {'$addFields': {'items': [[]]}},
                {'$project': {'items': {'$objectToArray': '$items'}, '_id': 0}},
            ],
        ]

        for item in items:
            with self.assertRaises(mongomock.OperationFailure):
                collection.aggregate(item)

    def test_aggregate_object_to_array_with_example(self):
        collection = self.db.collection

        collection.insert_many(
            [
                {
                    '_id': 1,
                    'item': 'ABC1',
                    'dimensions': collections.OrderedDict(
                        [
                            ('l', 25),
                            ('w', 10),
                            ('uom', 'cm'),
                        ]
                    ),
                },
                {
                    '_id': 2,
                    'item': 'ABC2',
                    'dimensions': collections.OrderedDict(
                        [
                            ('l', 50),
                            ('w', 25),
                            ('uom', 'cm'),
                        ]
                    ),
                },
                {
                    '_id': 3,
                    'item': 'XYZ1',
                    'dimensions': collections.OrderedDict(
                        [
                            ('l', 70),
                            ('w', 75),
                            ('uom', 'cm'),
                        ]
                    ),
                },
            ]
        )

        expect = [
            {
                '_id': 1,
                'item': 'ABC1',
                'dims': [
                    {'k': 'l', 'v': 25},
                    {'k': 'w', 'v': 10},
                    {'k': 'uom', 'v': 'cm'},
                ],
            },
            {
                '_id': 2,
                'item': 'ABC2',
                'dims': [
                    {'k': 'l', 'v': 50},
                    {'k': 'w', 'v': 25},
                    {'k': 'uom', 'v': 'cm'},
                ],
            },
            {
                '_id': 3,
                'item': 'XYZ1',
                'dims': [
                    {'k': 'l', 'v': 70},
                    {'k': 'w', 'v': 75},
                    {'k': 'uom', 'v': 'cm'},
                ],
            },
        ]

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        'item': 1,
                        'dims': {'$objectToArray': '$dimensions'},
                    },
                }
            ]
        )

        self.assertEqual(expect, list(actual))

    def test_aggregate_is_number(self):
        collection = self.db.collection

        collection.insert_one(
            {
                '_id': 1,
                'int': 3,
                'big_int': 3**10,
                'negative': -3,
                'str': 'not_a_number',
                'str_numeric': '3',
                'float': 3.3,
                'negative_float': -3.3,
                'bool': True,
                'none': None,
            }
        )

        expect = [
            {
                'int': True,
                'big_int': True,
                'negative': True,
                'str': False,
                'str_numeric': False,
                'float': True,
                'negative_float': True,
                'bool': False,
                'none': False,
            },
        ]

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': False,
                        'int': {'$isNumber': '$int'},
                        'big_int': {'$isNumber': '$big_int'},
                        'negative': {'$isNumber': '$negative'},
                        'str': {'$isNumber': '$str'},
                        'str_numeric': {'$isNumber': '$str_numeric'},
                        'float': {'$isNumber': '$float'},
                        'negative_float': {'$isNumber': '$negative_float'},
                        'bool': {'$isNumber': '$bool'},
                        'none': {'$isNumber': '$none'},
                    },
                }
            ]
        )

        self.assertEqual(expect, list(actual))

    def test_aggregate_is_array(self):
        collection = self.db.collection

        collection.insert_one(
            {
                '_id': 1,
                'list': [1, 2, 3],
                'tuple': (1, 2, 3),
                'empty_list': [],
                'empty_tuple': (),
                'int': 3,
                'str': '123',
                'bool': True,
                'none': None,
            }
        )

        expect = [
            {
                'list': True,
                'tuple': True,
                'empty_list': True,
                'empty_tuple': True,
                'int': False,
                'str': False,
                'bool': False,
                'none': False,
            },
        ]

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': False,
                        'list': {'$isArray': '$list'},
                        'tuple': {'$isArray': '$tuple'},
                        'empty_list': {'$isArray': '$empty_list'},
                        'empty_tuple': {'$isArray': '$empty_tuple'},
                        'int': {'$isArray': '$int'},
                        'str': {'$isArray': '$str'},
                        'bool': {'$isArray': '$bool'},
                        'none': {'$isArray': '$none'},
                    },
                }
            ]
        )

        self.assertEqual(expect, list(actual))

    def test_aggregate_type(self):
        collection = self.db.collection

        collection.insert_one(
            {
                '_id': 1,
                'list': [1, 2, 3],
                'tuple': (1, 2, 3),
                'string': 'lol',
                'int': 10,
                'long': 2**32,
                'bool': True,
                'double': 10.5,
                'object': {},
                'null': None,
                'date': datetime(1999, 12, 19, 1, 2, 3),
            }
        )

        expected = [
            {
                'list': 'array',
                'tuple': 'array',
                'string': 'string',
                'int': 'int',
                'long': 'long',
                'bool': 'bool',
                'double': 'double',
                'object': 'object',
                'null': 'null',
                'date': 'date',
                'missing': 'missing',
            }
        ]

        actual = collection.aggregate(
            [
                {
                    '$project': {
                        '_id': False,
                        'list': {'$type': '$list'},
                        'tuple': {'$type': '$tuple'},
                        'string': {'$type': '$string'},
                        'int': {'$type': '$int'},
                        'long': {'$type': '$long'},
                        'bool': {'$type': '$bool'},
                        'double': {'$type': '$double'},
                        'object': {'$type': '$object'},
                        'null': {'$type': '$null'},
                        'date': {'$type': '$date'},
                        'missing': {'$type': '$object.doesnt_exist'},
                    }
                }
            ]
        )

        self.assertListEqual(expected, list(actual))

    def test_aggregate_project_with_boolean(self):
        collection = self.db.collection

        # Test with no items
        expect = []
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': []}}}])
        self.assertEqual(expect, list(actual))

        expect = []
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': []}}}])
        self.assertEqual(expect, list(actual))

        expect = []
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$not': {}}}}])
        self.assertEqual(expect, list(actual))

        # Tests following are with one item
        collection.insert_one({'items': []})

        # Test with 0 arguments
        expect = [{'items': True}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': []}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': []}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$not': {}}}}])
        self.assertEqual(expect, list(actual))

        # Test with one argument
        expect = [{'items': True}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': [True]}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': True}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': [True]}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$not': True}}}])
        self.assertEqual(expect, list(actual))

        # Test with two arguments
        expect = [{'items': True}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': [True, True]}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': [False, True]}}}])
        self.assertEqual(expect, list(actual))

        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': [True, False]}}}])
        self.assertEqual(expect, list(actual))

        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': [False, False]}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': True}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': [True, True]}}}])
        self.assertEqual(expect, list(actual))

        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': [False, True]}}}])
        self.assertEqual(expect, list(actual))

        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': [True, False]}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}]
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': [False, False]}}}])
        self.assertEqual(expect, list(actual))

        # Following tests are with more than two items
        collection.insert_many([{'items': []}, {'items': []}])

        expect = [{'items': True}] * 3
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$and': []}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}] * 3
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$or': []}}}])
        self.assertEqual(expect, list(actual))

        expect = [{'items': False}] * 3
        actual = collection.aggregate([{'$project': {'_id': 0, 'items': {'$not': {}}}}])
        self.assertEqual(expect, list(actual))

        # Test with something else than boolean
        collection.insert_one({'items': ['foo']})

        expect = [{'items': False}] * 3 + [{'items': True}]
        actual = collection.aggregate(
            [{'$project': {'_id': 0, 'items': {'$and': [{'$eq': ['$items', ['foo']]}]}}}]
        )
        self.assertEqual(expect, list(actual))

        actual = collection.aggregate(
            [{'$project': {'_id': 0, 'items': {'$or': [{'$eq': ['$items', ['foo']]}]}}}]
        )
        self.assertEqual(expect, list(actual))

        expect = [{'items': True}] * 3 + [{'items': False}]
        actual = collection.aggregate(
            [{'$project': {'_id': 0, 'items': {'$not': {'$eq': ['$items', ['foo']]}}}}]
        )
        self.assertEqual(expect, list(actual))

    def test_set_no_content(self):
        collection = self.db.collection
        collection.insert_one({'a': 1})

        if version.parse('5.0') <= SERVER_VERSION:
            collection.update_one({}, {'$set': {}})
            collection.update_one({'b': 'will-never-exist'}, {'$set': {}})
            return

        with self.assertRaises(mongomock.WriteError):
            collection.update_one({}, {'$set': {}})

        with self.assertRaises(mongomock.WriteError):
            collection.update_one({'b': 'will-never-exist'}, {'$set': {}})

    def test_snapshot_arg(self):
        self.db.collection.find(snapshot=False)

    def test_elem_match(self):
        self.db.collection.insert_many(
            [
                {'_id': 0, 'arr': [0, 1, 2, 3, 10]},
                {'_id': 1, 'arr': [0, 2, 4, 6]},
                {'_id': 2, 'arr': [1, 3, 5, 7]},
            ]
        )
        ids = {
            doc['_id']
            for doc in self.db.collection.find(
                {'arr': {'$elemMatch': {'$lt': 10, '$gt': 4}}}, {'_id': 1}
            )
        }
        self.assertEqual({1, 2}, ids)

    def test_list_collection_names_filter(self):
        now = datetime.now()
        self.db.create_collection('aggregator')
        for day in range(10):
            new_date = now - timedelta(day)
            self.db.create_collection('historical_{}'.format(new_date.strftime('%Y_%m_%d')))

        # test without filter
        self.assertEqual(len(self.db.list_collection_names()), 11)

        # test regex
        assert (
            len(
                self.db.list_collection_names(
                    filter={'name': {'$regex': r'historical_\d{4}_\d{2}_\d{2}'}}
                )
            )
            == 10
        )

        new_date = datetime.now() - timedelta(1)
        col_name = 'historical_{}'.format(new_date.strftime('%Y_%m_%d'))

        # test not equal
        self.assertEqual(len(self.db.list_collection_names(filter={'name': {'$ne': col_name}})), 10)

        # test equal
        assert col_name in self.db.list_collection_names(filter={'name': col_name})

        # neg invalid field
        with self.assertRaises(NotImplementedError):
            self.db.list_collection_names(filter={'_id': {'$ne': col_name}})

        # neg invalid operator
        with self.assertRaises(NotImplementedError):
            self.db.list_collection_names(filter={'name': {'$ge': col_name}})

    def test__equality(self):
        self.assertEqual(self.db.a, self.db.a)
        self.assertNotEqual(self.db.a, self.db.b)
        self.assertEqual(self.db.a, self.db.get_collection('a'))
        self.assertNotEqual(self.db.a, self.client.other_db.a)
        client = mongomock.MongoClient('localhost')
        self.assertEqual(client.db.collection, mongomock.MongoClient('localhost').db.collection)
        client = mongomock.MongoClient('localhost')
        self.assertEqual(client.db.collection, mongomock.MongoClient('localhost').db.collection)
        self.assertNotEqual(
            client.db.collection, mongomock.MongoClient('example.com').db.collection
        )

    @skipIf(sys.version_info < (3,), 'Older versions of Python do not handle hashing the same way')
    def test__hashable(self):
        {self.db.a, self.db.b}  # noqa: B018

    def test__bad_type_as_a_read_concern_returns_type_error(self):
        with self.assertRaises(
            TypeError, msg='read_concern must be an instance of pymongo.read_concern.ReadConcern'
        ):
            mongomock.collection.Collection(self.db, 'foo', None, read_concern='bar')

    def test__cursor_allow_disk_use(self):
        col = self.db.col
        col.find().allow_disk_use(True)
        col.find().allow_disk_use(False)
        col.find().allow_disk_use()
        with self.assertRaises(TypeError):
            col.find().allow_disk_use(1)
        # use the keyword argument
        col.find(allow_disk_use=True)
        col.find(allow_disk_use=False)
        col.find()
        with self.assertRaises(TypeError):
            col.find(allow_disk_use=1)

    def test__aggregate_immutable_output(self):
        import types

        collection = self.db.collection
        frozen = types.MappingProxyType({'b': 1})
        collection.insert_one({'_id': 1, 'a': frozen})
        doc = collection.find_one({'_id': 1})
        doc['a']['b'] = 999
        doc_2 = collection.find_one({'_id': 1})
        self.assertEqual(doc_2['a']['b'], 1)

    def test__find_mappingproxytype_query(self):
        import types

        collection = self.db.collection
        frozen = types.MappingProxyType({'b': 1})
        collection.insert_one({'_id': 1, 'a': frozen})
        docs = list(collection.find({'a.b': 1}))
        self.assertEqual(len(docs), 1)

    def test__find_dot_notation_nested_arrays(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [[1, 2], [3, 4]]})
        collection.insert_one({'_id': 2, 'arr': [[5, 6], [7, 8]]})
        actual = list(collection.find({'arr.0.0': 1}, projection=['_id']))
        self.assertEqual([{'_id': 1}], actual)

    def test__find_one_and_update_nested_projection(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': {'b': 1, 'c': 2}})
        doc = collection.find_one_and_update(
            {'_id': 1}, {'$set': {'a.b': 3}}, projection={'a.b': 1}
        )
        self.assertIn('a', doc)
        self.assertEqual(doc['a']['b'], 1)

    def test__find_one_and_replace_nested_projection(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': {'b': 1, 'c': 2}})
        doc = collection.find_one_and_replace(
            {'_id': 1}, {'a': {'b': 10, 'c': 20}}, projection={'a.b': 1}
        )
        self.assertIn('a', doc)
        self.assertEqual(doc['a']['b'], 1)

    def test__find_one_and_delete_nested_projection(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': {'b': 1, 'c': 2}})
        doc = collection.find_one_and_delete({'_id': 1}, projection={'a.b': 1})
        self.assertIn('a', doc)
        self.assertEqual(doc['a']['b'], 1)

    def test__collection_bool(self):
        with self.assertRaises(NotImplementedError):
            bool(self.db.collection)

    def test__aggregate_exp_overflow(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$exp': 10000}}}])

    def test__aggregate_pow_overflow(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$pow': [10000, 10000]}}}])

    def test__aggregate_mod_overflow(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$mod': [10**1000, 3]}}}])

    def test__aggregate_to_double_overflow(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$toDouble': 10**1000}}}])

    def test__aggregate_concatArrays_empty(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$concatArrays': []}}}])

    def test__aggregate_in_non_array(self):
        self.db.collection.insert_one({'_id': 1, 'v': 5})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$in': ['$v', 5]}}}])

    def test__update_all_positional_array(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[].x': 10}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 10}, {'x': 10}])

    def test__update_all_positional_array_top_level(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 10}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [10, 10, 10])

    def test__update_all_positional_remaining_path(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': {'y': 1}}, {'x': {'y': 2}}]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[].x.y': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': {'y': 99}}, {'x': {'y': 99}}])

    def test__update_all_positional_mixed_keys(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'title': 'hello', 'arr': [1, 2]})
        collection.update_one({'_id': 1}, {'$set': {'title': 'world', 'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['title'], 'world')
        self.assertEqual(doc['arr'], [99, 99])

    def test__update_array_filter_not_found(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        with self.assertRaises(mongomock.WriteError):
            collection.update_one(
                {'_id': 1},
                {'$set': {'arr.$[bad].x': 10}},
                array_filters=[{'good.x': {'$gte': 0}}],
            )

    def test__update_array_filter_non_dict_item(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[e]': 10}},
            array_filters=[{'e': {'$gte': 2}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [1, 10, 10])

    def test__update_array_filter_remaining_path(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'nested': {'v': 1}}, {'nested': {'v': 2}}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].nested.v': 99}},
            array_filters=[{'elem': {'nested.v': {'$gte': 2}}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'nested': {'v': 1}}, {'nested': {'v': 99}}])

    def test__update_array_index_error(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}]})
        with self.assertRaises((ValueError, mongomock.WriteError)):
            collection.update_one({'_id': 1}, {'$set': {'arr.5.x': 99}})

    def test__aggregate_binary_operator_wrong_args(self):
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([{'$project': {'x': {'$round': [1, 2, 3]}}}])

    def test__aggregate_union_with(self):
        self.db.collection.insert_many([{'_id': 1, 'x': 1}, {'_id': 2, 'x': 2}])
        self.db.other_collection.insert_many([{'_id': 3, 'x': 3}, {'_id': 4, 'x': 4}])
        result = list(self.db.collection.aggregate([{'$unionWith': 'other_collection'}]))
        self.assertEqual(4, len(result))
        self.assertCountEqual([1, 2, 3, 4], [doc['_id'] for doc in result])

    def test__aggregate_get_field_string_syntax(self):
        self.db.collection.insert_one({'_id': 1, 'val': 42})
        result = list(self.db.collection.aggregate([{'$project': {'v': {'$getField': '$val'}}}]))
        self.assertEqual(42, result[0]['v'])

    def test__aggregate_get_field_object_syntax(self):
        self.db.collection.insert_one({'_id': 1, 'nested': {'value': 42}})
        result = list(
            self.db.collection.aggregate(
                [{'$project': {'v': {'$getField': {'field': 'value', 'input': '$nested'}}}}]
            )
        )
        self.assertEqual(42, result[0]['v'])

    def test__aggregate_trim(self):
        self.db.collection.insert_one({'_id': 1, 's': '  hello  '})
        result = list(
            self.db.collection.aggregate([{'$project': {'trimmed': {'$trim': {'input': '$s'}}}}])
        )
        self.assertEqual('hello', result[0]['trimmed'])

    def test__aggregate_ltrim(self):
        self.db.collection.insert_one({'_id': 1, 's': '  hello  '})
        result = list(
            self.db.collection.aggregate([{'$project': {'trimmed': {'$ltrim': {'input': '$s'}}}}])
        )
        self.assertEqual('hello  ', result[0]['trimmed'])

    def test__aggregate_rtrim(self):
        self.db.collection.insert_one({'_id': 1, 's': '  hello  '})
        result = list(
            self.db.collection.aggregate([{'$project': {'trimmed': {'$rtrim': {'input': '$s'}}}}])
        )
        self.assertEqual('  hello', result[0]['trimmed'])

    def test__aggregate_to_date(self):
        self.db.collection.insert_one({'_id': 1, 's': '2026-01-15T10:30:00Z'})
        result = list(self.db.collection.aggregate([{'$project': {'dt': {'$toDate': '$s'}}}]))
        self.assertIsInstance(result[0]['dt'], datetime)

    @skipIf(platform.python_implementation() == 'PyPy', 'pandas not supported on PyPy')
    def test__find_nat_comparison(self):
        import pandas as pd

        collection = self.db.collection
        oid1 = mongomock.ObjectId()
        oid2 = mongomock.ObjectId()
        collection._store[oid1] = {'_id': oid1, 'dt': pd.NaT}
        collection._store[oid2] = {'_id': oid2, 'dt': pd.NaT}
        docs = list(collection.find({'dt': pd.NaT}))
        self.assertEqual(len(docs), 2)

    @skipIf(platform.python_implementation() == 'PyPy', 'pandas not supported on PyPy')
    def test__find_nat_sort(self):
        import pandas as pd

        collection = self.db.collection
        oid1 = mongomock.ObjectId()
        oid2 = mongomock.ObjectId()
        oid3 = mongomock.ObjectId()
        collection._store[oid1] = {'_id': oid1, 'dt': pd.NaT}
        collection._store[oid2] = {'_id': oid2, 'dt': pd.NaT}
        collection._store[oid3] = {'_id': oid3, 'dt': pd.Timestamp('2020-01-01')}
        docs = list(collection.find().sort('dt', -1))
        self.assertEqual(len(docs), 3)

    def test__update_all_positional(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [99, 99, 99])

    def test__update_all_positional_with_query(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one({'arr': [1, 2, 3]}, {'$set': {'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [99, 99, 99])

    def test__update_all_positional_multiple_fields(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'x': [1, 2], 'y': [3, 4]})
        collection.update_one({'_id': 1}, {'$set': {'x.$[]': 99, 'y.$[]': 88}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['x'], [99, 99])
        self.assertEqual(doc['y'], [88, 88])

    def test__update_all_positional_multiple_arrays(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr1': [1, 2], 'arr2': [3, 4]})
        collection.update_one({'_id': 1}, {'$set': {'arr1.$[]': 10, 'arr2.$[]': 20}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr1'], [10, 10])
        self.assertEqual(doc['arr2'], [20, 20])

    def test__update_all_positional_with_inc(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one({'_id': 1}, {'$inc': {'arr.$[]': 1}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [2, 3, 4])

    def test__update_all_positional_with_unset(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}, {'x': 3}]})
        collection.update_one({'_id': 1}, {'$unset': {'arr.$[].x': ''}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{}, {}, {}])

    def test__update_all_positional_nested_arrays(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'items': [1, 2]}, {'items': [3, 4]}]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[].items': [99]}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'items': [99]}, {'items': [99]}])

    def test__update_all_positional_empty_array(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': []})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 10}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [])

    def test__update_all_positional_upsert(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [99, 99, 99])

    def test__update_all_positional_mixed(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2], 'other': 'keep'})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [99, 99])
        self.assertEqual(doc['other'], 'keep')

    def test__update_all_positional_with_positional(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one({'arr.x': 1}, {'$set': {'arr.$.x': 99, 'arr.$[].x': 88}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 88}, {'x': 88}])

    def test__update_all_positional_with_array_filters(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[].x': 99}},
            array_filters=[{'elem.x': {'$gte': 0}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}, {'x': 99}])

    def test__update_all_positional_nested_doc(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'a': 1}, {'a': 2}]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[].a': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'a': 99}, {'a': 99}])

    def test__update_all_positional_subdocument_array(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'data': {'items': [1, 2, 3]}})
        collection.update_one({'_id': 1}, {'$set': {'data.items.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['data'], {'items': [99, 99, 99]})

    def test__update_all_positional_regex_array_filter(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': ['abc', 'def', 'abc']})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem]': 'xxx'}},
            array_filters=[{'elem': {'$regex': '^abc'}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], ['xxx', 'def', 'xxx'])

    def test__update_all_positional_push_combined(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 2]})
        with self.assertRaises(InvalidDocument):
            collection.update_one({'_id': 1}, {'$push': {'arr': 3}, '$set': {'arr.$[]': 99}})

    def test__update_all_positional_deeply_nested(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'outer': [{'inner': [1, 2]}, {'inner': [3, 4]}]})
        collection.update_one({'_id': 1}, {'$set': {'outer.$[].inner': [99]}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['outer'], [{'inner': [99]}, {'inner': [99]}])

    def test__update_all_positional_dotted_path_atom(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'e': {'x': 1}}, {'e': {'x': 2}}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].e.x': 99}},
            array_filters=[{'elem.e.x': {'$gte': 1}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'e': {'x': 99}}, {'e': {'x': 99}}])

    def test__update_all_positional_type_consistency(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [1, 'two', 3.0]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': None}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [None, None, None])

    def test__update_all_positional_none_values(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [None, 1, None]})
        collection.update_one({'_id': 1}, {'$set': {'arr.$[]': 99}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [99, 99, 99])

    def test__update_all_positional_empty_update(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': []})
        collection.update_one({'_id': 1}, {'$min': {'arr.$[]': 1}})
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [])

    def test__array_filter_update_positional(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}, {'x': 2}])

    def test__array_filter_multiple_conditions(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1, 'y': 10}, {'x': 2, 'y': 5}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].y': 99}},
            array_filters=[{'elem.x': {'$gte': 1}, 'elem.y': {'$gte': 8}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 1, 'y': 99}, {'x': 2, 'y': 5}])

    def test__array_filter_with_update(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$inc': {'arr.$[elem].x': 10}},
            array_filters=[{'elem.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 11}, {'x': 2}])

    def test__array_filter_multiple_filters(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1, 'y': 10}, {'x': 2, 'y': 20}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99, 'arr.$[elem].y': 88}},
            array_filters=[{'elem.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99, 'y': 10}, {'x': 2, 'y': 20}])

    def test__array_filter_with_and(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1, 'y': 10}, {'x': 2, 'y': 5}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].y': 99}},
            array_filters=[{'$and': [{'elem.x': 1}, {'elem.y': 10}]}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 1, 'y': 99}, {'x': 2, 'y': 5}])

    def test__array_filter_with_or(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}, {'x': 3}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'$or': [{'elem.x': 1}, {'elem.x': 3}]}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}, {'x': 2}, {'x': 99}])

    def test__array_filter_nested_doc(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'data': [{'nested': {'val': 1}}, {'nested': {'val': 2}}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'data.$[elem].nested.val': 99}},
            array_filters=[{'elem.nested.val': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['data'], [{'nested': {'val': 99}}, {'nested': {'val': 2}}])

    def test__array_filter_multiple_array_fields(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': [{'x': 1}], 'b': [{'y': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'a.$[elem].x': 99}},
            array_filters=[{'elem.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['a'], [{'x': 99}])

    def test__array_filter_no_match(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': 100}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 1}, {'x': 2}])

    def test__array_filter_all_match(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 1}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}, {'x': 99}])

    def test__array_filter_positional_combined(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1, 'y': 2}, {'x': 2, 'y': 3}]})
        collection.update_one(
            {'arr.x': 1},
            {'$set': {'arr.$.y': 99, 'arr.$[elem].x': 88}},
            array_filters=[{'elem.x': 2}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 1, 'y': 99}, {'x': 88, 'y': 3}])

    def test__array_filter_dotted_path_filter(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'e': {'x': 1}}, {'e': {'x': 2}}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].e.x': 99}},
            array_filters=[{'elem.e.x': {'$gte': 1}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'e': {'x': 99}}, {'e': {'x': 99}}])

    def test__array_filter_dotted_path_filter_mixed(self):
        collection = self.db.collection
        collection.insert_one(
            {'_id': 1, 'arr': [{'e': {'x': 1}, 'y': 10}, {'e': {'x': 2}, 'y': 5}]}
        )
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].y': 99}},
            array_filters=[{'elem.e.x': {'$gte': 1}, 'elem.y': {'$gte': 8}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'e': {'x': 1}, 'y': 99}, {'e': {'x': 2}, 'y': 5}])

    def test__array_filter_gte_lte(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 5}, {'x': 15}, {'x': 25}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': {'$gte': 10, '$lte': 20}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 5}, {'x': 99}, {'x': 25}])

    def test__array_filter_identifier_named_x(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 1}, {'x': 2}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[x].x': 99}},
            array_filters=[{'x.x': 1}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}, {'x': 2}])

    def test__array_filter_upsert(self):
        collection = self.db.collection
        collection.update_one(
            {'_id': 1, 'arr': [{'x': 1}, {'x': 2}]},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': 1}],
            upsert=True,
        )
        doc = collection.find_one({'_id': 1})
        self.assertIsNotNone(doc)

    def test__array_filter_upsert_no_match(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 10}]})
        collection.update_one(
            {'_id': 1},
            {'$set': {'arr.$[elem].x': 99}},
            array_filters=[{'elem.x': {'$gte': 0}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [{'x': 99}])

    def test__array_filter_update_uses_mul(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [{'x': 5}, {'x': 10}]})
        with self.assertRaises(ValueError):
            collection.update_one(
                {'_id': 1},
                {'$mul': {'arr.$[elem].x': 2}},
                array_filters=[{'elem.x': 5}],
            )

    def test__array_filter_update_rename_dotted(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': {'b': {'c': 'deep'}, 'd': 'old'}})
        collection.update_one(
            {'_id': 1},
            {'$rename': {'a.b.c': 'a.b.x'}},
        )
        doc = collection.find_one({'_id': 1})
        self.assertNotIn('c', doc['a']['b'])
        self.assertEqual(doc['a']['b']['x'], 'deep')

    def test__array_filter_update_rename_dotted_to_top(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'a': {'b': {'c': 'deep'}}})
        collection.update_one(
            {'_id': 1},
            {'$rename': {'a.b.c': 'x'}},
        )
        doc = collection.find_one({'_id': 1})
        self.assertNotIn('c', doc['a']['b'])
        self.assertEqual(doc['x'], 'deep')

    def test__array_filter_update_pop(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [[1, 2, 3], [4, 5, 6]]})
        collection.update_one(
            {'_id': 1},
            {'$pop': {'arr.$[elem]': -1}},
            array_filters=[{'elem': {'$exists': True}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['arr'], [[2, 3], [5, 6]])

    def test__array_filter_update_pull(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [[1, 2, 3], [4, 5, 6]]})
        collection.update_one(
            {'_id': 1},
            {'$pull': {'arr.$[elem]': {'$gte': 5}}},
            array_filters=[{'elem': {'$exists': True}}],
        )
        doc = collection.find_one({'_id': 1})
        self.assertEqual(len(doc['arr']), 1)

    def test__array_filter_update_add_to_set(self):
        collection = self.db.collection
        collection.insert_one({'_id': 1, 'arr': [[1, 2], [3, 4]]})
        with self.assertRaises(ValueError):
            collection.update_one(
                {'_id': 1},
                {'$addToSet': {'arr.$[elem]': 99}},
                array_filters=[{'elem': {'$exists': True}}],
            )

    def test__options_returns_collection_options(self):
        coll = self.db.create_collection('opts1', validator={'a': {'$type': 'int'}})
        opts = coll.options()
        self.assertIn('validator', opts)
        self.assertEqual(opts['validator'], {'a': {'$type': 'int'}})

    def test__options_without_validator(self):
        coll = self.db.create_collection('opts2')
        opts = coll.options()
        self.assertNotIn('validator', opts)

    def test__insert_one_validation(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        with self.assertRaises(WriteError):
            coll.insert_one({'a': 'abc'})

    def test__insert_one_validation_error_code(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        with self.assertRaises(WriteError) as ctx:
            coll.insert_one({'a': 'abc'})
        self.assertEqual(ctx.exception.code, 121)

    def test__insert_many_validation_error(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        with self.assertRaises(BulkWriteError):
            coll.insert_many([{'a': 1}, {'a': 'bad'}])

    def test__insert_many_ordered_stops_on_first_failure(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        with self.assertRaises(BulkWriteError) as ctx:
            coll.insert_many([{'a': 'bad1'}, {'a': 2}])
        errors = ctx.exception.details.get('writeErrors', [])
        self.assertEqual(len(errors), 1)

    def test__update_one_validation_error(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        with self.assertRaises(WriteError):
            coll.update_one({'a': 1}, {'$set': {'a': 'str'}})

    def test__update_many_validation_error(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_many([{'a': 1}, {'a': 2}])
        with self.assertRaises(WriteError):
            coll.update_many({}, {'$set': {'a': 'str'}})

    def test__replace_one_validation_error(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        with self.assertRaises(WriteError):
            coll.replace_one({'a': 1}, {'a': 'str'})

    def test__bypass_document_validation_insert(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 'should pass'}, bypass_document_validation=True)
        self.assertEqual(coll.count_documents({}), 1)

    def test__bypass_document_validation_update(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        coll.update_one(
            {'a': 1},
            {'$set': {'a': 'str'}},
            bypass_document_validation=True,
        )
        doc = coll.find_one()
        self.assertEqual(doc['a'], 'str')

    def test__validation_collmod_updates_validator(self):
        coll = self.db.create_collection('opts1')
        self.db.command('collMod', 'opts1', validator={'b': {'$type': 'string'}})
        coll.insert_one({'b': 'hello'})
        with self.assertRaises(WriteError):
            coll.insert_one({'b': 123})

    def test__validation_level_off_skips_validation(self):
        coll = self.db.create_collection(
            'validated',
            validator={'a': {'$type': 'int'}},
            validationLevel='off',
        )
        coll.insert_one({'a': 'should pass'})
        self.assertEqual(coll.count_documents({}), 1)

    def test__validation_action_warn_skips_validation(self):
        coll = self.db.create_collection(
            'validated',
            validator={'a': {'$type': 'int'}},
            validationAction='warn',
        )
        coll.insert_one({'a': 'should pass'})
        self.assertEqual(coll.count_documents({}), 1)

    def test__update_rollback_on_validation_failure(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1, 'x': 'original'})
        with self.assertRaises(WriteError):
            coll.update_one({'a': 1}, {'$set': {'a': 'bad', 'x': 'changed'}})
        doc = coll.find_one()
        self.assertEqual(doc['a'], 1)
        self.assertEqual(doc['x'], 'original')

    def test__validation_level_moderate_valid_original_rejects_invalid(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        self.db.command('collMod', 'validated', validationLevel='moderate')
        with self.assertRaises(WriteError):
            coll.update_one({'a': 1}, {'$set': {'a': 'bad'}})
        doc = coll.find_one()
        self.assertEqual(doc['a'], 1)

    def test__validation_level_moderate_invalid_original_skips_validation(self):
        coll = self.db.create_collection('validated', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        coll.update_one({'a': 1}, {'$set': {'a': 'bad'}}, bypass_document_validation=True)
        self.db.command('collMod', 'validated', validationLevel='moderate')
        coll.update_one({'a': 'bad'}, {'$set': {'a': 'still bad'}})
        doc = coll.find_one()
        self.assertEqual(doc['a'], 'still bad')

    def test__validation_on_find_one_and_update(self):
        coll = self.db.create_collection('v', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        coll.find_one_and_update({'a': 1}, {'$set': {'a': 2}})
        self.assertEqual(coll.find_one()['a'], 2)

    def test__validation_on_find_one_and_replace(self):
        coll = self.db.create_collection('v', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        coll.find_one_and_replace({'a': 1}, {'a': 2})
        self.assertEqual(coll.find_one()['a'], 2)

    def test__validation_on_find_one_and_update_rejects(self):
        coll = self.db.create_collection('v', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        with self.assertRaises(WriteError):
            coll.find_one_and_update({'a': 1}, {'$set': {'a': 'bad'}})
        self.assertEqual(coll.find_one()['a'], 1)

    def test__validation_on_find_one_and_replace_rejects(self):
        coll = self.db.create_collection('v', validator={'a': {'$type': 'int'}})
        coll.insert_one({'a': 1})
        with self.assertRaises(WriteError):
            coll.find_one_and_replace({'a': 1}, {'a': 'bad'})
        self.assertEqual(coll.find_one()['a'], 1)

    def test__aggregate_function_not_implemented(self):
        """Gap 1: aggregate.py $function NotImplementedError"""
        self.db.collection.insert_one({'_id': 1})
        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate(
                [{'$project': {'x': {'$function': {'body': 'return 1', 'args': [], 'lang': 'js'}}}}]
            )

    def test__aggregate_sort_by_count_dict(self):
        """Gap 2: aggregate.py $sortByCount with dict type"""
        self.db.collection.insert_one({'_id': 1, 'x': 'a'})
        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([{'$sortByCount': {'key': 'value'}}])

    def test__aggregate_union_with_pipeline(self):
        """Gap 3: aggregate.py $unionWith pipeline variant"""
        self.db.collection.insert_many([{'_id': 1, 'x': 1}, {'_id': 2, 'x': 2}])
        self.db.other_collection.insert_many([{'_id': 3, 'x': 3}, {'_id': 4, 'x': 4}])
        result = list(
            self.db.collection.aggregate(
                [
                    {
                        '$unionWith': {
                            'coll': 'other_collection',
                            'pipeline': [{'$match': {'x': 3}}],
                        }
                    }
                ]
            )
        )
        self.assertEqual(3, len(result))
        self.assertCountEqual([1, 2, 3], [doc['_id'] for doc in result])

    def test__aggregate_trim_with_chars(self):
        """Gap 4: aggregate.py $trim with chars parameter"""
        self.db.collection.insert_one({'_id': 1, 's': 'xxHello Worldxx'})
        result = list(
            self.db.collection.aggregate(
                [{'$project': {'trimmed': {'$trim': {'input': '$s', 'chars': 'x'}}}}]
            )
        )
        self.assertEqual('Hello World', result[0]['trimmed'])

    def test__aggregate_lookup_with_let_no_pipeline(self):
        """Gap 5: aggregate.py $lookup with let without pipeline"""
        self.db.orders.insert_many(
            [
                {'_id': 1, 'item': 'almonds', 'price': 12},
            ]
        )
        self.db.warehouses.insert_many(
            [
                {'_id': 1, 'stock_item': 'almonds', 'warehouse': 'A', 'instock': 120},
            ]
        )
        pipeline = [
            {
                '$lookup': {
                    'from': 'warehouses',
                    'let': {'order_item': '$item'},
                    'localField': 'item',
                    'foreignField': 'stock_item',
                    'as': 'stockdata',
                }
            }
        ]
        result = list(self.db.orders.aggregate(pipeline))
        self.assertEqual(1, len(result))
        self.assertIn('stockdata', result[0])
        self.assertEqual(1, len(result[0]['stockdata']))

    def test__find_projection_path_collision(self):
        """Gap 6: collection.py PathCollision in projection"""
        self.db.collection.insert_one({'_id': 1, 'a': {'b': 1}})
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.find({}, {'a': 1, 'a.b': 1}))

    def test__find_projection_elem_match(self):
        """Gap 7: $elemMatch in find projection"""
        self.db.collection.insert_one({'_id': 1, 'items': [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]})
        result = list(self.db.collection.find({}, {'items': {'$elemMatch': {'x': {'$gte': 2}}}}))
        self.assertEqual([{'x': 3, 'y': 4}], result[0]['items'])

    def test__find_projection_size_match(self):
        """Issue #78: $size projection keeps array when length matches exactly."""
        self.db.collection.insert_many(
            [
                {'_id': 1, 'tags': ['a', 'b']},
                {'_id': 2, 'tags': ['a', 'b', 'c']},
                {'_id': 3, 'tags': ['a']},
            ]
        )
        results = list(self.db.collection.find({}, {'_id': 0, 'tags': {'$size': 2}}))
        self.assertEqual(1, len(results))
        self.assertEqual(['a', 'b'], results[0]['tags'])

    def test__find_projection_size_no_match(self):
        """Issue #78: $size projection excludes document when array length != N."""
        self.db.collection.insert_one({'_id': 1, 'tags': ['a', 'b', 'c']})
        results = list(self.db.collection.find({}, {'tags': {'$size': 2}}))
        self.assertEqual(0, len(results))

    def test__find_projection_size_non_array(self):
        """Issue #78: $size projection excludes document when value is not an array."""
        self.db.collection.insert_one({'_id': 1, 'tags': 'not_an_array'})
        results = list(self.db.collection.find({}, {'tags': {'$size': 2}}))
        self.assertEqual(0, len(results))

    def test__find_projection_size_missing_field(self):
        """Issue #78: $size projection handles missing field gracefully."""
        self.db.collection.insert_one({'_id': 1, 'other': 'value'})
        results = list(self.db.collection.find({}, {'tags': {'$size': 2}}))
        self.assertEqual(1, len(results))
        self.assertNotIn('tags', results[0])

    def test__find_projection_size_with_id_zero(self):
        """Issue #78: $size projection works with _id: 0."""
        self.db.collection.insert_many(
            [
                {'_id': 1, 'tags': ['a', 'b']},
                {'_id': 2, 'tags': ['a', 'b', 'c']},
            ]
        )
        results = list(self.db.collection.find({}, {'_id': 0, 'tags': {'$size': 2}}))
        self.assertEqual(1, len(results))
        self.assertEqual(['a', 'b'], results[0]['tags'])
        self.assertNotIn('_id', results[0])

    def test__find_non_mapping_filter(self):
        """Gap 9: filtering.py non-Mapping filter -> TypeError"""
        with self.assertRaises(TypeError):
            list(self.db.collection.find(['not', 'a', 'dict']))

    def test__collection_remove_deprecated_not_type_error(self):
        """After adding deprecated remove(), it no longer raises TypeError."""
        with self.assertWarns(DeprecationWarning):
            self.db.collection.remove({})

    def test__deprecated_remove(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1})
        self.db.collection.insert_one({'_id': 2, 'a': 2})
        with self.assertWarns(DeprecationWarning):
            self.db.collection.remove({'_id': 1})
        self.assertEqual(1, self.db.collection.count_documents({}))

    def test__deprecated_save_insert(self):
        with self.assertWarns(DeprecationWarning):
            self.db.collection.save({'_id': 'new', 'x': 1})
        self.assertEqual(1, self.db.collection.count_documents({}))

    def test__deprecated_save_replace(self):
        self.db.collection.insert_one({'_id': 'existing', 'x': 1})
        with self.assertWarns(DeprecationWarning):
            self.db.collection.save({'_id': 'existing', 'x': 2})
        doc = self.db.collection.find_one({'_id': 'existing'})
        self.assertEqual(2, doc['x'])

    def test__deprecated_count(self):
        self.db.collection.insert_one({'a': 1})
        self.db.collection.insert_one({'a': 2})
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(2, self.db.collection.count())

    def test__deprecated_update(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1})
        with self.assertWarns(DeprecationWarning):
            self.db.collection.update({'_id': 1}, {'$set': {'a': 2}}, multi=False)
        doc = self.db.collection.find_one({'_id': 1})
        self.assertEqual(2, doc['a'])

    def test__deprecated_insert(self):
        with self.assertWarns(DeprecationWarning):
            self.db.collection.insert({'_id': 'new', 'x': 1})
        self.assertEqual(1, self.db.collection.count_documents({}))

    def test__deprecated_insert_many(self):
        with self.assertWarns(DeprecationWarning):
            self.db.collection.insert([{'_id': 'a'}, {'_id': 'b'}])
        self.assertEqual(2, self.db.collection.count_documents({}))

    def test__deprecated_ensure_index(self):
        with self.assertWarns(DeprecationWarning):
            self.db.collection.ensure_index('x')
        self.assertIn('x_1', self.db.collection.index_information())

    def test__duplicate_key_error_details_id(self):
        self.db.collection.insert_one({'_id': 'dup', 'x': 1})
        with self.assertRaises(mongomock.DuplicateKeyError) as ctx:
            self.db.collection.insert_one({'_id': 'dup', 'x': 2})
        self.assertEqual(ctx.exception.details['keyPattern'], {'_id': 1})
        self.assertEqual(ctx.exception.details['keyValue'], {'_id': 'dup'})

    def test__duplicate_key_error_details_unique_index(self):
        self.db.collection.create_index('email', unique=True)
        self.db.collection.insert_one({'email': 'a@b.com'})
        with self.assertRaises(mongomock.DuplicateKeyError) as ctx:
            self.db.collection.insert_one({'email': 'a@b.com'})
        self.assertEqual(ctx.exception.details['keyPattern'], {'email': 1})
        self.assertEqual(ctx.exception.details['keyValue'], {'email': 'a@b.com'})

    def test__duplicate_key_error_details_compound_index(self):
        self.db.collection.create_index([('a', 1), ('b', -1)], unique=True)
        self.db.collection.insert_one({'a': 1, 'b': 2})
        with self.assertRaises(mongomock.DuplicateKeyError) as ctx:
            self.db.collection.insert_one({'a': 1, 'b': 2})
        self.assertEqual(ctx.exception.details['keyPattern'], {'a': 1, 'b': -1})
        self.assertEqual(ctx.exception.details['keyValue'], {'a': 1, 'b': 2})


class TestAggregationBugfixesMock(TestCase):
    def setUp(self):
        self.client = mongomock.MongoClient()
        self.db = self.client.testdb
        self.collection = self.db.collection

    def test__group_last_all_missing(self):
        self.collection.insert_one({'item': 'apple', 'quantity': 8})
        result = list(
            self.collection.aggregate(
                [
                    {
                        '$group': {
                            '_id': '$item',
                            'data': {
                                '$last': {'quantity': '$quantity', 'description': '$description'}
                            },
                        }
                    }
                ]
            )
        )
        self.assertEqual(result[0]['data'], {'quantity': 8})

    def test__group_id_null_groups_all(self):
        self.collection.insert_many([{'x': 1}, {'x': 2}, {'x': 3}])
        result = list(self.collection.aggregate([{'$group': {'_id': None, 'count': {'$sum': 1}}}]))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], None)
        self.assertEqual(result[0]['count'], 3)

    def test__toLong_tz_aware(self):
        from datetime import datetime
        from datetime import timezone

        self.collection.insert_one({'ts': datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)})
        result = list(self.collection.aggregate([{'$project': {'ts_long': {'$toLong': '$ts'}}}]))
        self.assertIsInstance(result[0]['ts_long'], int)
        self.assertGreater(result[0]['ts_long'], 0)

    def test__find_projection_computed_field(self):
        self.collection.insert_one({'a': 'test_a', 'embedded': {'embedded_key': 'test_embedded'}})
        result = self.collection.find_one({}, projection={'embedded': '$embedded.embedded_key'})
        self.assertEqual(result['embedded'], 'test_embedded')

    def test__slice_only_keeps_other_fields(self):
        self.collection.insert_one({'test': [1, 2, 3], 'value': 42})
        doc = self.collection.find_one({}, projection={'test': {'$slice': 1}})
        self.assertEqual(doc['test'], [1])
        self.assertEqual(doc['value'], 42)

    def test__slice_no_mutation(self):
        self.collection.insert_one({'n': 1, 'elements': [{'x': 1}]})
        doc = self.collection.find_one({'n': 1}, {'elements': {'$slice': 1}})
        doc['elements'][0]['x'] += 1
        doc2 = self.collection.find_one({'n': 1})
        self.assertEqual(doc2['elements'][0]['x'], 1)

    def test__project_deeply_nested(self):
        self.collection.insert_one({'a': 1, 'b': {'c': 10, 'd': 20}})
        result = list(self.collection.aggregate([{'$project': {'_id': 0, 'b': {'c': 1}}}]))
        self.assertEqual(result[0], {'b': {'c': 10}})

    def test__add_datetime_multiply(self):
        from datetime import datetime

        self.collection.insert_one({'date': datetime(2024, 1, 1), 'ms': 3600000})
        result = list(self.collection.aggregate([{'$set': {'result': {'$add': ['$date', '$ms']}}}]))
        self.assertEqual(result[0]['result'], datetime(2024, 1, 1, 1, 0, 0))

    def test__count_empty_group(self):
        self.collection.insert_one({'x': 1})
        result = list(self.collection.aggregate([{'$group': {'_id': '$x', 'cnt': {'$count': {}}}}]))
        self.assertEqual(result[0]['cnt'], 1)

    def test__sum_flattens_arrays(self):
        self.collection.insert_one({'arr': [1, 2, 3]})
        result = list(self.collection.aggregate([{'$project': {'total': {'$sum': '$arr'}}}]))
        self.assertEqual(result[0]['total'], 6)

    def test__multiply_decimal128(self):
        from decimal import Decimal

        from bson import Decimal128

        self.collection.insert_one({'val': Decimal128('5')})
        result = list(
            self.collection.aggregate([{'$project': {'result': {'$multiply': ['$val', 2]}}}])
        )
        self.assertEqual(result[0]['result'], Decimal('10'))

    def test__project_boolean_not_nested_projection(self):
        self.collection.insert_one({'items': [1, 2, 3]})
        result = list(self.collection.aggregate([{'$project': {'_id': 0, 'items': {'$not': []}}}]))
        self.assertIsInstance(result[0]['items'], bool)
