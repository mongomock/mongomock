import collections
import copy
from datetime import datetime, tzinfo, timedelta
import platform
import random
import re
import six
from six import assertCountEqual, text_type
import time
from unittest import TestCase, skipIf
import warnings

import mongomock
from mongomock.write_concern import WriteConcern

try:
    from bson import codec_options
    from bson.errors import InvalidDocument
    from bson import tz_util, ObjectId
    import pymongo
    from pymongo.collation import Collation
    from pymongo.read_preferences import ReadPreference
    from pymongo import ReturnDocument
    _HAVE_PYMONGO = True
except ImportError:
    from mongomock.collection import ReturnDocument
    _HAVE_PYMONGO = False


warnings.simplefilter('ignore', DeprecationWarning)
IS_PYPY = platform.python_implementation() != 'CPython'


class CollectionAPITest(TestCase):

    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test__get_subcollections(self):
        self.db.create_collection('a.b')
        self.assertEqual(self.db.a.b.full_name, 'somedb.a.b')
        self.assertEqual(self.db.a.b.name, 'a.b')

        self.assertEqual(set(self.db.list_collection_names()), set(['a.b']))

    def test__get_subcollections_by_attribute_underscore(self):
        with self.assertRaises(AttributeError) as err_context:
            self.db.a._b  # pylint: disable=pointless-statement

        self.assertIn("Collection has no attribute '_b'", str(err_context.exception))

        # No problem accessing it through __get_item__.
        self.db.a['_b'].insert_one({'a': 1})
        self.assertEqual(1, self.db.a['_b'].find_one().get('a'))

    def test__get_sibling_collection(self):
        self.assertEqual(self.db.a.database.b.full_name, 'somedb.b')
        self.assertEqual(self.db.a.database.b.name, 'b')

    def test__get_collection_read_concern_option(self):
        """Ensure read_concern option isn't rejected."""
        self.assertTrue(self.db.get_collection('new_collection', read_concern=None))

    def test__get_collection_full_name(self):
        self.assertEqual(self.db.coll.name, 'coll')
        self.assertEqual(self.db.coll.full_name, 'somedb.coll')

    def test__collection_names(self):
        self.db.create_collection('a')
        self.db.create_collection('b')
        self.assertEqual(set(self.db.collection_names()), set(['a', 'b']))

        self.db.c.drop()
        self.assertEqual(set(self.db.collection_names()), set(['a', 'b']))

    def test__list_collection_names(self):
        self.db.create_collection('a')
        self.db.create_collection('b')
        self.assertEqual(set(self.db.list_collection_names()), set(['a', 'b']))

        self.db.c.drop()
        self.assertEqual(set(self.db.list_collection_names()), set(['a', 'b']))

    def test__create_collection(self):
        coll = self.db.create_collection('c')
        self.assertIs(self.db.c, coll)
        self.assertRaises(mongomock.CollectionInvalid,
                          self.db.create_collection, 'c')

    def test__create_collection_bad_names(self):
        with self.assertRaises(TypeError):
            self.db.create_collection(3)

        bad_names = (
            '',
            'foo..bar',
            '...',
            '$foo',
            '.foo',
            'bar.',
            'foo\x00bar',
        )
        for name in bad_names:
            with self.assertRaises(mongomock.InvalidName, msg=name):
                self.db.create_collection(name)

    def test__lazy_create_collection(self):
        col = self.db.a
        self.assertEqual(set(self.db.list_collection_names()), set())
        col.insert({'foo': 'bar'})
        self.assertEqual(set(self.db.list_collection_names()), set(['a']))

    def test__cursor_collection(self):
        self.assertIs(self.db.a.find().collection, self.db.a)

    def test__cursor_alive(self):
        self.db.collection.insert_one({'foo': 'bar'})
        cursor = self.db.collection.find()
        self.assertTrue(cursor.alive)
        next(cursor)
        self.assertFalse(cursor.alive)

    def test__drop_collection(self):
        self.db.create_collection('a')
        self.db.create_collection('b')
        self.db.create_collection('c')
        self.db.drop_collection('b')
        self.db.drop_collection('b')
        self.db.drop_collection(self.db.c)
        self.assertEqual(set(self.db.list_collection_names()), set(['a']))

        col = self.db.a
        r = col.insert({'aa': 'bb'})
        qr = col.find({'_id': r})
        self.assertEqual(qr.count(), 1)

        self.db.drop_collection('a')
        qr = col.find({'_id': r})
        self.assertEqual(qr.count(), 0)

        col = self.db.a
        r = col.insert({'aa': 'bb'})
        qr = col.find({'_id': r})
        self.assertEqual(qr.count(), 1)

        self.assertTrue(isinstance(col._store._documents, collections.OrderedDict))
        self.db.drop_collection(col)
        self.assertTrue(isinstance(col._store._documents, collections.OrderedDict))
        qr = col.find({'_id': r})
        self.assertEqual(qr.count(), 0)

    def test__drop_collection_indexes(self):
        col = self.db.a
        col.create_index('simple')
        col.create_index([('value', 1)], unique=True)
        col.ensure_index([('sparsed', 1)], unique=True, sparse=True)

        self.db.drop_collection(col)

        # Make sure indexes' rules no longer apply
        col.insert({'value': 'not_unique_but_ok', 'sparsed': 'not_unique_but_ok'})
        col.insert({'value': 'not_unique_but_ok'})
        col.insert({'sparsed': 'not_unique_but_ok'})
        result = col.find({})
        self.assertEqual(result.count(), 3)

    def test__drop_n_recreate_collection(self):
        col_a = self.db.create_collection('a')
        col_a2 = self.db.a
        col_a.insert({'foo': 'bar'})
        self.assertEqual(col_a.find().count(), 1)
        self.assertEqual(col_a2.find().count(), 1)
        self.assertEqual(self.db.a.find().count(), 1)

        self.db.drop_collection('a')
        self.assertEqual(col_a.find().count(), 0)
        self.assertEqual(col_a2.find().count(), 0)
        self.assertEqual(self.db.a.find().count(), 0)

        col_a2.insert({'foo2': 'bar2'})
        self.assertEqual(col_a.find().count(), 1)
        self.assertEqual(col_a2.find().count(), 1)
        self.assertEqual(self.db.a.find().count(), 1)

    def test__cursor_hint(self):
        self.db.collection.insert({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()

        self.assertEqual(cursor, cursor.hint(None))

        cursor.hint('unknownIndex')
        self.assertEqual([{'f2': 'v'}], [d['f1'] for d in cursor])

        with self.assertRaises(mongomock.InvalidOperation):
            cursor.hint(None)

    def test__distinct_nested_field(self):
        self.db.collection.insert({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1.f2'), ['v'])

    def test__distinct_array_field(self):
        self.db.collection.insert(
            [{'f1': ['v1', 'v2', 'v1']}, {'f1': ['v2', 'v3']}])
        cursor = self.db.collection.find()
        self.assertEqual(set(cursor.distinct('f1')), set(['v1', 'v2', 'v3']))

    def test__distinct_array_nested_field(self):
        self.db.collection.insert({'f1': [{'f2': 'v'}, {'f2': 'w'}]})
        cursor = self.db.collection.find()
        self.assertEqual(set(cursor.distinct('f1.f2')), {'v', 'w'})

    def test__distinct_document_field(self):
        self.db.collection.insert_many([
            {'f1': {'f2': 'v2', 'f3': 'v3'}},
            {'f1': {'f2': 'v2', 'f3': 'v3'}}
        ])
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1'), [{'f2': 'v2', 'f3': 'v3'}])

    def test__distinct_filter_field(self):
        self.db.collection.insert([{'f1': 'v1', 'k1': 'v1'}, {'f1': 'v2', 'k1': 'v1'},
                                   {'f1': 'v3', 'k1': 'v2'}])
        self.assertEqual(set(self.db.collection.distinct('f1', {'k1': 'v1'})), set(['v1', 'v2']))

    def test__distinct_error(self):
        with self.assertRaises(TypeError):
            self.db.collection.distinct({'f1': 1})

    def test__cursor_clone(self):
        self.db.collection.insert([{'a': 'b'}, {'b': 'c'}, {'c': 'd'}])
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
        self.db.collection.insert([{'a': 'b'}, {'b': 'c'}, {'c': 'd'}])
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
        self.db.collection.insert(obj)
        fetched_obj = self.db.collection.find_one({'a': 1})
        self.assertEqual(fetched_obj, obj)
        fetched_obj['b'] = 3
        refetched_obj = self.db.collection.find_one({'a': 1})
        self.assertNotEqual(fetched_obj, refetched_obj)

    def test__update_retval(self):
        self.db.col.save({'a': 1})
        retval = self.db.col.update({'a': 1}, {'b': 2})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[text_type('connectionId')], int)
        self.assertIsNone(retval[text_type('err')])
        self.assertEqual(retval[text_type('n')], 1)
        self.assertTrue(retval[text_type('updatedExisting')])
        self.assertEqual(retval['ok'], 1.0)

        self.assertEqual(self.db.col.update({'bla': 1}, {'bla': 2})['n'], 0)

    def test__remove_retval(self):
        self.db.col.save({'a': 1})
        retval = self.db.col.remove({'a': 1})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[text_type('connectionId')], int)
        self.assertIsNone(retval[text_type('err')])
        self.assertEqual(retval[text_type('n')], 1)
        self.assertEqual(retval[text_type('ok')], 1.0)

        self.assertEqual(self.db.col.remove({'bla': 1})['n'], 0)

    def test__remove_write_concern(self):
        self.db.col.remove({'a': 1}, w=None, wtimeout=None, j=None, fsync=None)

    def test__remove_bad_write_concern(self):
        with self.assertRaises(TypeError):
            self.db.col.remove({'a': 1}, bad_kwarg=1)

    def test__getting_collection_via_getattr(self):
        col1 = self.db.some_collection_here
        col2 = self.db.some_collection_here
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db['some_collection_here'])
        self.assertIsInstance(col1, mongomock.Collection)

    def test__save_class_deriving_from_dict(self):
        # See https://github.com/vmalloc/mongomock/issues/52
        class Document(dict):

            def __init__(self, collection):
                self.collection = collection
                super(Document, self).__init__()
                self.save()

            def save(self):
                self.collection.save(self)

        doc = Document(self.db.collection)
        self.assertIn('_id', doc)
        self.assertNotIn('collection', doc)

    def test__getting_collection_via_getitem(self):
        col1 = self.db['some_collection_here']
        col2 = self.db['some_collection_here']
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db.some_collection_here)
        self.assertIsInstance(col1, mongomock.Collection)

    def test__cannot_save_non_string_keys(self):
        for key in [2, 2.0, True, object()]:
            with self.assertRaises(ValueError):
                self.db.col1.save({key: 'value'})

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
        self.assertListEqual(
            list(self.db.collection.find(projection=projection)), documents)

    def test__insert(self):
        self.db.collection.insert({'a': 1})
        self.assert_document_count(1)

        self.db.collection.insert([{'a': 2}, {'a': 3}])
        self.assert_document_count(3)

        self.db.collection.insert(
            {'a': 4}, check_keys=False, continue_on_error=True)
        self.assert_document_count(4)

        self.db.collection.insert({'a': 4}, w=1)
        self.assert_document_count(5)

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

    def test__count(self):
        self.db.collection.insert_many([
            {'a': 1, 's': 0},
            {'a': 2, 's': 0},
            {'a': 3, 's': 1}
        ])
        self.assertEqual(self.db.collection.count(), 3)
        self.assertEqual(self.db.collection.count({'s': 0}), 2)
        self.assertEqual(self.db.collection.count({'s': 1}), 1)

    def test__count_documents(self):
        self.db.collection.insert_many([
            {'a': 1, 's': 0},
            {'a': 2, 's': 0},
            {'_id': 'unique', 'a': 3, 's': 1}
        ])
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
            self.db.collection.count_documents({}, collation='fr')

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.count_documents('unique')

    def test__find_returns_cursors(self):
        collection = self.db.collection
        self.assertEqual(type(collection.find()).__name__, 'Cursor')
        self.assertNotIsInstance(collection.find(), list)
        self.assertNotIsInstance(collection.find(), tuple)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
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
            {'partial': True}
        ]

        for option in options:
            with self.assertRaises(TypeError):
                self.db.collection.find({}, **option)

    def test__find_and_modify_cannot_remove_and_new(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_and_modify({}, remove=True, new=True)

    def test__find_and_modify_cannot_remove_and_update(self):
        with self.assertRaises(ValueError):  # this is also what pymongo raises
            self.db.collection.find_and_modify({'a': 2}, {'a': 3}, remove=True)

    def test__find_one_and_update_doc_with_zero_ids(self):
        ret = self.db.col_a.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}},
            upsert=True, return_document=ReturnDocument.AFTER)
        self.assertEqual(ret, {'_id': 0, 'counter': 1})
        ret = self.db.col_a.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}},
            upsert=True, return_document=ReturnDocument.AFTER)
        self.assertEqual(ret, {'_id': 0, 'counter': 2})

        ret = self.db.col_b.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}},
            upsert=True, return_document=ReturnDocument.BEFORE)
        self.assertIsNone(ret)
        ret = self.db.col_b.find_one_and_update(
            {'_id': 0}, {'$inc': {'counter': 1}},
            upsert=True, return_document=ReturnDocument.BEFORE)
        self.assertEqual(ret, {'_id': 0, 'counter': 1})

    def test__find_and_modify_no_projection_kwarg(self):
        with self.assertRaises(TypeError):  # unlike pymongo, we warn about this
            self.db.collection.find_and_modify({'a': 2}, {'a': 3}, projection=['a'])

    def test__find_one_and_delete(self):
        documents = [
            {'x': 1, 's': 0},
            {'x': 2, 's': 1}
        ]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_delete({'x': 3})
        self.assert_documents(documents, ignore_ids=False)
        self.assertIsNone(doc)

        doc = self.db.collection.find_one_and_delete({'x': 2})
        self.assert_documents(documents[:-1], ignore_ids=False)
        self.assertDictEqual(doc, documents[1])

        doc = self.db.collection.find_one_and_delete(
            {'s': 0}, {'_id': False, 'x': True})
        self.assertEqual(doc, {'x': 1})

    def test__find_one_and_replace(self):
        documents = [
            {'x': 1, 's': 0},
            {'x': 1, 's': 1}
        ]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_replace(
            {'s': 3}, {'x': 2, 's': 1})
        self.assert_documents(documents, ignore_ids=False)
        self.assertIsNone(doc)

        doc = self.db.collection.find_one_and_replace(
            {'s': 1}, {'x': 2, 's': 1})
        self.assertDictEqual(doc, documents[1])
        self.assert_document_count(2)

        doc = self.db.collection.find_one_and_replace(
            {'s': 2}, {'x': 3, 's': 0}, upsert=True)
        self.assertIsNone(doc)
        self.assertIsNotNone(self.db.collection.find_one({'x': 3}))
        self.assert_document_count(3)

        replacement = {'x': 4, 's': 1}
        doc = self.db.collection.find_one_and_replace(
            {'s': 1}, replacement,
            return_document=ReturnDocument.AFTER)
        doc.pop('_id')
        self.assertDictEqual(doc, replacement)

    def test__find_one_and_update(self):
        documents = [
            {'x': 1, 's': 0},
            {'x': 1, 's': 1}
        ]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_update(
            {'s': 3}, {'$set': {'x': 2}})
        self.assertIsNone(doc)
        self.assert_documents(documents, ignore_ids=False)

        doc = self.db.collection.find_one_and_update(
            {'s': 1}, {'$set': {'x': 2}})
        self.assertDictEqual(doc, documents[1])

        doc = self.db.collection.find_one_and_update(
            {'s': 3}, {'$set': {'x': 3, 's': 2}}, upsert=True)
        self.assertIsNone(doc)
        self.assertIsNotNone(self.db.collection.find_one({'x': 3}))

        update = {'x': 4, 's': 1}
        doc = self.db.collection.find_one_and_update(
            {'s': 1}, {'$set': update},
            return_document=ReturnDocument.AFTER)
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
        self.assertTrue(self.db.collection.find_one({'a': {
            '$regex': re.compile('tada', re.IGNORECASE),
        }}))

        self.assertTrue(self.db.collection.find_one({'a': {'$regex': 'tada', '$options': 'i'}}))
        self.assertTrue(self.db.collection.find_one({'a': {'$regex': '^da', '$options': 'im'}}))
        self.assertFalse(self.db.collection.find_one({'a': {'$regex': 'tada', '$options': 'I'}}))
        self.assertTrue(self.db.collection.find_one({'a': {'$regex': 'TADA', '$options': 'z'}}))
        self.assertTrue(self.db.collection.find_one({'a': collections.OrderedDict([
            ('$regex', re.compile('tada')),
            ('$options', 'i'),
        ])}))
        self.assertTrue(self.db.collection.find_one({'a': collections.OrderedDict([
            ('$regex', re.compile('tada', re.IGNORECASE)),
            ('$options', 'm'),
        ])}))

        # Bad type for $options.
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$regex': 'tada', '$options': re.I}})

        # Bug https://jira.mongodb.org/browse/SERVER-38621
        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one({'a': collections.OrderedDict([
                ('$options', 'i'),
                ('$regex', re.compile('tada')),
            ])})

    def test__iterate_on_find_and_update(self):
        documents = [
            {'x': 1, 's': 0},
            {'x': 1, 's': 1},
            {'x': 1, 's': 2},
            {'x': 1, 's': 3}
        ]
        self.db.collection.insert_many(documents)
        self.assert_documents(documents, ignore_ids=False)

        cursor = self.db.collection.find({'x': 1})
        self.assertEqual(cursor.count(), 4)

        # Update the field used by the cursor's filter should not upset the iteration
        for doc in cursor:
            self.db.collection.update_one({'_id': doc['_id']}, {'$set': {'x': 2}})

        cursor = self.db.collection.find({'x': 1})
        self.assertEqual(cursor.count(), 0)
        cursor = self.db.collection.find({'x': 2})
        self.assertEqual(cursor.count(), 4)

    def test__update_interns_lists_and_dicts(self):
        obj = {}
        obj_id = self.db.collection.save(obj)
        d = {}
        l = []
        self.db.collection.update({'_id': obj_id}, {'d': d, 'l': l})
        d['a'] = 'b'
        l.append(1)
        self.assertEqual(
            list(self.db.collection.find()),
            [{'_id': obj_id, 'd': {}, 'l': []}])

    def test__update_cannot_change__id(self):
        self.db.collection.insert({'_id': 1, 'a': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.update({'_id': 1}, {'_id': 2, 'b': 2})

    def test__update_empty_id(self):
        self.db.collection.save({'_id': '', 'a': 1})
        self.db.collection.replace_one({'_id': ''}, {'b': 1})
        doc = self.db.collection.find_one({'_id': ''})
        self.assertEqual(1, doc['b'])

    def test__update_one(self):
        insert_result = self.db.collection.insert_one({'a': 1})
        update_result = self.db.collection.update_one(
            filter={'a': 1},
            update={'$set': {'a': 2}}
        )
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
            filter={'a': 1},
            update={'$set': {'a': 1}},
            upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'a': 1})

    def test__update_one_upsert_dots(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_one(
            filter={'a.b': 1},
            update={'$set': {'c': 2}},
            upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'a': {'b': 1}, 'c': 2})

    def test__update_one_upsert_match_subdocuments(self):
        update_result = self.db.collection.update_one(
            filter={'b.c.': 1, 'b.d': 3},
            update={'$set': {'a': 1}},
            upsert=True
        )

        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(
            update_result.upserted_id, {'a': 1, 'b': {'c': {'': 1}, 'd': 3}})

    def test__update_one_upsert_operators(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_one(
            filter={'a.b': {'$eq': 1}, 'e.f': {'$gt': 3}, 'd': {}},
            update={'$set': {'c': 2}},
            upsert=True
        )
        self.assertEqual(update_result.modified_count, 0)
        self.assertEqual(update_result.matched_count, 0)
        self.assertIsNotNone(update_result.upserted_id)
        self.assert_document_stored(update_result.upserted_id, {'c': 2, 'd': {}, 'a': {'b': 1}})

    def test__update_one_unset_position(self):
        insert_result = self.db.collection.insert_one({'a': 1, 'b': [{'c': 2, 'd': 3}]})
        update_result = self.db.collection.update_one(
            filter={'a': 1, 'b': {'$elemMatch': {'c': 2, 'd': 3}}},
            update={'$unset': {'b.$.c': ''}}
        )
        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(update_result.matched_count, 1)
        self.assert_document_stored(insert_result.inserted_id, {'a': 1, 'b': [{'d': 3}]})

    def test__update_one_no_change(self):
        self.db.collection.insert_one({'a': 1})
        update_result = self.db.collection.update_one(
            filter={'a': 1},
            update={'$set': {'a': 1}}
        )
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

    def test__rename_unsupported(self):
        input_ = {'_id': 1, 'foo': 'bar'}
        insert_result = self.db.collection.insert_one(input_)
        self.assert_document_stored(insert_result.inserted_id, input_)

        query = {'_id': 1}
        update = {'$rename': {'foo': 'f.o.o.'}}
        self.assertRaises(NotImplementedError,
                          self.db.collection.update_one, query, update=update)

    def test__update_one_upsert_invalid_filter(self):
        with self.assertRaises(mongomock.WriteError):
            self.db.collection.update_one(
                filter={'a.b': 1, 'a': 3},
                update={'$set': {'c': 2}},
                upsert=True
            )

    def test__update_many(self):
        self.db.collection.insert_many([
            {'a': 1, 'c': 2},
            {'a': 1, 'c': 3},
            {'a': 2, 'c': 4}
        ])
        update_result = self.db.collection.update_many(
            filter={'a': 1},
            update={'$set': {'c': 0}}
        )
        self.assertEqual(update_result.modified_count, 2)
        self.assertEqual(update_result.matched_count, 2)
        self.assertIsNone(update_result.upserted_id)
        self.assert_documents([{'a': 1, 'c': 0},
                               {'a': 1, 'c': 0},
                               {'a': 2, 'c': 4}])

    def test__update_many_upsert(self):
        self.assert_document_count(0)
        update_result = self.db.collection.update_many(
            filter={'a': 1},
            update={'$set': {'a': 1, 'c': 0}},
            upsert=True
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
        self.db.collection.update_one({}, {'$push': {'scores': {
            '$each': [80, 78, 86],
            '$slice': -5,
        }}})
        self.assertEqual([50, 60, 80, 78, 86], self.db.collection.find_one()['scores'])

    def test__update_push_slice_from_the_front(self):
        self.db.collection.insert_one({'scores': [89, 90]})
        self.db.collection.update_one({}, {'$push': {'scores': {
            '$each': [100, 20],
            '$slice': 3,
        }}})
        self.assertEqual([89, 90, 100], self.db.collection.find_one()['scores'])

    def test__update_push_slice_to_zero(self):
        self.db.collection.insert_one({'scores': [40, 50, 60]})
        self.db.collection.update_one({}, {'$push': {'scores': {
            '$each': [80, 78, 86],
            '$slice': 0,
        }}})
        self.assertEqual([], self.db.collection.find_one()['scores'])

    def test__update_push_slice_only(self):
        self.db.collection.insert_one({'scores': [89, 70, 100, 20]})
        self.db.collection.update_one({}, {'$push': {'scores': {
            '$each': [],
            '$slice': -3,
        }}})
        self.assertEqual([70, 100, 20], self.db.collection.find_one()['scores'])

    def test__update_push_slice_nested_field(self):
        self.db.collection.insert_one({'games': [{'scores': [89, 70, 100, 20]}]})
        self.db.collection.update_one({}, {'$push': {'games.0.scores': {
            '$each': [15],
            '$slice': -3,
        }}})
        self.assertEqual([100, 20, 15], self.db.collection.find_one()['games'][0]['scores'])

    def test__update_push_slice_positional_nested_field(self):
        self.db.collection.insert_one({'games': [{'scores': [0, 1]}, {'scores': [2, 3]}]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'scores.0': 2}}},
            {'$push': {'games.$.scores': {
                '$each': [15],
                '$slice': -2,
            }}})
        self.assertEqual([0, 1], self.db.collection.find_one()['games'][0]['scores'])
        self.assertEqual([3, 15], self.db.collection.find_one()['games'][1]['scores'])

    def test__update_push_sort(self):
        self.db.collection.insert_one(
            {'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one({}, {'$push': {'a.b': {
            '$each': [{'value': 4}],
            '$sort': {'value': 1},
        }}})
        self.assertEqual(
            {'b': [{'value': 1}, {'value': 2}, {'value': 3}, {'value': 4}]},
            self.db.collection.find_one()['a'])

    def test__update_push_sort_document(self):
        self.db.collection.insert_one({'a': {'b': [3, 1, 2]}})
        self.db.collection.update_one({}, {'$push': {'a.b': {
            '$each': [4, 5],
            '$sort': -1,
        }}})
        self.assertEqual({'b': [5, 4, 3, 2, 1]}, self.db.collection.find_one()['a'])

    def test__update_push_position(self):
        self.db.collection.insert_one(
            {'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one({}, {'$push': {'a.b': {
            '$each': [{'value': 4}],
            '$position': 1,
        }}})
        self.assertEqual(
            {'b': [{'value': 3}, {'value': 4}, {'value': 1}, {'value': 2}]},
            self.db.collection.find_one()['a'])

    def test__update_push_negative_position(self):
        self.db.collection.insert_one(
            {'a': {'b': [{'value': 3}, {'value': 1}, {'value': 2}]}})
        self.db.collection.update_one({}, {'$push': {'a.b': {
            '$each': [{'value': 4}],
            '$position': -2,
        }}})
        self.assertEqual(
            {'b': [{'value': 3}, {'value': 4}, {'value': 1}, {'value': 2}]},
            self.db.collection.find_one()['a'])

    def test__update_push_other_clauses(self):
        self.db.collection.insert_one({'games': [{'scores': [0, 1]}, {'scores': [2, 3]}]})
        with self.assertRaises(mongomock.WriteError):
            self.db.collection.update_one(
                {'games': {'$elemMatch': {'scores.0': 2}}},
                {'$push': {'games.$.scores': {
                    '$each': [15, 13],
                    '$a_clause_that_does_not_exit': 1,
                }}})

    def test__update_push_positional_nested_field(self):
        self.db.collection.insert_one({'games': [{}]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'player.scores': {'$exists': False}}}},
            {'$push': {'games.$.player.scores': 15}})
        self.assertEqual([{'player': {'scores': [15]}}], self.db.collection.find_one()['games'])

    def test__update_push_array_of_arrays(self):
        self.db.collection.insert_one({'games': [[0], [1]]})
        self.db.collection.update_one(
            {'games': {'$elemMatch': {'0': 1}}},
            {'$push': {'games.$': 15}})
        self.assertEqual([[0], [1, 15]], self.db.collection.find_one()['games'])

    def test__replace_one(self):
        self.db.collection.insert({'a': 1, 'b': 2})
        self.assert_documents([{'a': 1, 'b': 2}])

        result = self.db.collection.replace_one(
            filter={'a': 2},
            replacement={'x': 1, 'y': 2}
        )
        self.assert_documents([{'a': 1, 'b': 2}])
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

        result = self.db.collection.replace_one(
            filter={'a': 1},
            replacement={'x': 1, 'y': 2}
        )
        self.assert_documents([{'x': 1, 'y': 2}])
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)

    def test__replace_one_upsert(self):
        self.assert_document_count(0)
        result = self.db.collection.replace_one(
            filter={'a': 2},
            replacement={'x': 1, 'y': 2},
            upsert=True
        )
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsNotNone(result.upserted_id)
        self.assert_document_stored(result.upserted_id, {'x': 1, 'y': 2})

    def test__replace_one_invalid(self):
        with self.assertRaises(ValueError):
            self.db.collection.replace_one(
                filter={'a': 2}, replacement={'$set': {'x': 1, 'y': 2}})

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
        self.db.collection.insert_many([
            {'a': 1, 'c': 2},
            {'a': 1, 'c': 3},
            {'a': 2, 'c': 4}
        ])
        self.assert_document_count(3)

        self.db.collection.delete_many({'a': 2})
        self.assert_document_count(2)

        self.db.collection.delete_many({'a': 1})
        self.assert_document_count(0)

    def test__delete_many_collation_option(self):
        """Ensure collation delete_many's option is not rejected."""
        self.assertTrue(self.db.collection.delete_many({}, collation=None))

    def test__string_matching(self):
        """Make sure strings are not treated as collections on find"""
        self.db['abc'].save({'name': 'test1'})
        self.db['abc'].save({'name': 'test2'})
        # now searching for 'name':'e' returns test1
        self.assertIsNone(self.db['abc'].find_one({'name': 'e'}))

    def test__collection_is_indexable(self):
        self.db['def'].save({'name': 'test1'})
        self.assertTrue(self.db['def'].find({'name': 'test1'}).count() > 0)
        self.assertEqual(self.db['def'].find({'name': 'test1'})[0]['name'], 'test1')

    def test__cursor_distinct(self):
        larry_bob = {'name': 'larry'}
        larry = {'name': 'larry'}
        gary = {'name': 'gary'}
        self.db['coll_name'].insert([larry_bob, larry, gary])
        ret_val = self.db['coll_name'].find().distinct('name')
        self.assertTrue(isinstance(ret_val, list))
        self.assertTrue(set(ret_val) == set(['larry', 'gary']))

    def test__cursor_count_with_limit(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        count = self.db['coll_name'].find().limit(
            2).count(with_limit_and_skip=True)
        self.assertEqual(count, 2)
        count = self.db['coll_name'].find().limit(
            0).count(with_limit_and_skip=True)
        self.assertEqual(count, 3)

    def test__cursor_count_with_skip(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        count = self.db['coll_name'].find().skip(
            1).count(with_limit_and_skip=True)
        self.assertEqual(count, 2)

    def test__cursor_count_with_skip_init(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        count = self.db['coll_name'].find(skip=1).count(with_limit_and_skip=True)
        self.assertEqual(count, 2)

    def test__cursor_count_when_db_changes(self):
        self.db['coll_name'].insert({})
        cursor = self.db['coll_name'].find()
        self.db['coll_name'].insert({})
        self.assertEqual(cursor.count(), 2)

    def test__cursor_getitem_when_db_changes(self):
        self.db['coll_name'].insert({})
        cursor = self.db['coll_name'].find()
        self.db['coll_name'].insert({})
        cursor_items = [x for x in cursor]
        self.assertEqual(len(cursor_items), 2)

    def test__cursor_getitem(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        item = cursor[0]
        self.assertEqual(item['name'], 'first')

    def test__cursor_getitem_slice(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        ret = cursor[1:4]
        self.assertIs(ret, cursor)
        count = cursor.count()
        self.assertEqual(count, 3)
        count = cursor.count(with_limit_and_skip=True)
        self.assertEqual(count, 2)

    def test__cursor_getitem_negative_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(IndexError):
            cursor[-1]  # pylint: disable=pointless-statement

    def test__cursor_getitem_bad_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(TypeError):
            cursor['not_a_number']  # pylint: disable=pointless-statement

    def test__find_with_skip_param(self):
        """Make sure that find() will take in account skip parameter"""

        u1 = {'name': 'first'}
        u2 = {'name': 'second'}
        self.db['users'].insert([u1, u2])
        self.assertEqual(
            self.db['users'].find(
                sort=[
                    ('name', 1)], skip=1).count(with_limit_and_skip=True), 1)
        self.assertEqual(
            self.db['users'].find(
                sort=[
                    ('name', 1)], skip=1)[0]['name'], 'second')

    def test__ordered_insert_find(self):
        """Tests ordered inserts

        If we insert values 1, 2, 3 and find them, we must see them in order as
        we inserted them.
        """

        values = list(range(20))
        random.shuffle(values)
        for val in values:
            self.db.collection.insert({'_id': val})

        find_cursor = self.db.collection.find()

        for val in values:
            in_db_val = find_cursor.next()
            expected = {'_id': val}
            self.assertEqual(in_db_val, expected)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__create_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.create_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__create_uniq_idxs_with_descending_ordering(self):
        self.db.collection.create_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__create_uniq_idxs_without_ordering(self):
        self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__create_index_wrong_type(self):
        with self.assertRaises(TypeError):
            self.db.collection.create_index({'value': 1})

    def test__create_indexes_wrong_type(self):
        indexes = [('value', 1), ('name', 1)]
        with self.assertRaises(TypeError):
            self.db.collection.create_indexes(indexes)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__create_indexes_uniq_idxs(self):
        indexes = [
            pymongo.operations.IndexModel([('value', pymongo.ASCENDING)], unique=True),
            pymongo.operations.IndexModel([('name', pymongo.ASCENDING)], unique=True)
        ]
        index_names = self.db.collection.create_indexes(indexes)
        self.assertEqual(2, len(index_names))

        self.db.collection.insert({'value': 1, 'name': 'bob'})
        # Ensure both uniq indexes have been created
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1, 'name': 'different'})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 0, 'name': 'bob'})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__ensure_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.ensure_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__ensure_uniq_idxs_with_descending_ordering(self):
        self.db.collection.ensure_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__ensure_uniq_idxs_on_nested_field(self):
        self.db.collection.ensure_index([('a.b', 1)], unique=True)

        self.db.collection.insert({'a': 1})
        self.db.collection.insert({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'a': {'b': 1}})

        self.assertEqual(self.db.collection.find({}).count(), 2)

    def test__ensure_sparse_uniq_idxs_on_nested_field(self):
        self.db.collection.ensure_index([('a.b', 1)], unique=True, sparse=True)
        self.db.collection.ensure_index([('c', 1)], unique=True, sparse=True)

        self.db.collection.insert({})
        self.db.collection.insert({})
        self.db.collection.insert({'c': 1})
        self.db.collection.insert({'a': 1})
        self.db.collection.insert({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'a': {'b': 1}})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'c': 1})

        self.assertEqual(self.db.collection.find({}).count(), 5)

    def test__ensure_uniq_idxs_without_ordering(self):
        self.db.collection.ensure_index([('value', 1)], unique=True)

        self.db.collection.insert({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__insert_empty_doc_uniq_idx(self):
        self.db.collection.ensure_index([('value', 1)], unique=True)

        self.db.collection.insert({'value': 1})
        self.db.collection.insert({})

        self.assertEqual(self.db.collection.find({}).count(), 2)

    def test__insert_empty_doc_twice_uniq_idx(self):
        self.db.collection.ensure_index([('value', 1)], unique=True)

        self.db.collection.insert({})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test_sparse_unique_index(self):
        self.db.collection.ensure_index([('value', 1)], unique=True, sparse=True)

        self.db.collection.insert({})
        self.db.collection.insert({})
        self.db.collection.insert({'value': None})
        self.db.collection.insert({'value': None})

        self.assertEqual(self.db.collection.find({}).count(), 4)

    def test_unique_index_with_upsert_insertion(self):
        self.db.collection.ensure_index([('value', 1)], unique=True)

        self.db.collection.save({'_id': 1, 'value': 1})
        # Updating document should not trigger error
        self.db.collection.save({'_id': 1, 'value': 1})
        self.db.collection.update({'value': 1}, {'value': 1}, upsert=True)
        # Creating new documents with same value should
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.save({'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update({'bad': 'condition'}, {'value': 1}, upsert=True)
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.save({'_id': 2, 'value': 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update({'_id': 2}, {'$set': {'value': 1}}, upsert=True)

    def test_unique_index_with_update(self):
        self.db.collection.ensure_index([('value', 1)], unique=True)

        self.db.collection.save({'_id': 1, 'value': 1})
        self.db.collection.save({'_id': 2, 'value': 2})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update({'value': 1}, {'value': 2})

    def test_unique_index_with_update_on_nested_field(self):
        self.db.collection.ensure_index([('a.b', 1)], unique=True)

        self.db.collection.save({'_id': 1, 'a': {'b': 1}})
        self.db.collection.save({'_id': 2, 'a': {'b': 2}})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.update({'_id': 1}, {'$set': {'a.b': 2}})

    def test_sparse_unique_index_dup(self):
        self.db.collection.ensure_index([('value', 1)], unique=True, sparse=True)

        self.db.collection.insert({'value': 'a'})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 'a'})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__create_uniq_idxs_with_dupes_already_there(self):
        self.db.collection.insert({'value': 1})
        self.db.collection.insert({'value': 1})

        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.create_index([('value', 1)], unique=True)

        self.db.collection.insert({'value': 1})
        self.assertEqual(self.db.collection.find({}).count(), 3)

    def test__insert_empty_doc_idx_information(self):
        self.db.collection.insert({})

        index_information = self.db.collection.index_information()
        self.assertEqual(
            {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': self.db.collection.full_name}},
            index_information,
        )
        self.assertEqual(
            [{'name': '_id_', 'key': {'_id': 1}, 'ns': 'somedb.collection', 'v': 2}],
            list(self.db.collection.list_indexes()),
        )

        del index_information['_id_']

        self.assertEqual(
            {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': self.db.collection.full_name}},
            self.db.collection.index_information(),
            msg='index_information is immutable',
        )

    def test__empty_table_idx_information(self):
        self.db.collection.drop()
        index_information = self.db.collection.index_information()
        self.assertEqual({}, index_information)

    def test__create_idx_information(self):
        index = self.db.collection.create_index([('value', 1)])

        self.db.collection.insert({})

        self.assertDictEqual(
            {
                'key': [('value', 1)],
                'ns': self.db.collection.full_name,
                'v': 2,
            },
            self.db.collection.index_information()[index])
        self.assertEqual({'_id_', index}, set(self.db.collection.index_information().keys()))

        self.db.collection.drop_index(index)
        self.assertEqual({'_id_'}, set(self.db.collection.index_information().keys()))

    def test__drop_index_not_found(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.drop_index('unknownIndex')

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__create_unique_idx_information_with_ascending_ordering(self):
        index = self.db.collection.create_index([('value', pymongo.ASCENDING)], unique=True)

        self.db.collection.insert({'value': 1})

        self.assertDictEqual(
            {
                'key': [('value', pymongo.ASCENDING)],
                'ns': self.db.collection.full_name,
                'unique': True,
                'v': 2,
            },
            self.db.collection.index_information()[index])

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__create_unique_idx_information_with_descending_ordering(self):
        index = self.db.collection.create_index([('value', pymongo.DESCENDING)], unique=True)

        self.db.collection.insert({'value': 1})

        self.assertDictEqual(
            self.db.collection.index_information()[index],
            {
                'key': [('value', pymongo.DESCENDING)],
                'ns': self.db.collection.full_name,
                'unique': True,
                'v': 2,
            })

    def test__set_with_positional_operator(self):
        """Real mongodb support positional operator $ for $set operation"""
        base_document = {'int_field': 1,
                         'list_field': [{'str_field': 'a'},
                                        {'str_field': 'b'},
                                        {'str_field': 'c'}]}

        self.db.collection.insert(base_document)
        self.db.collection.update({'int_field': 1, 'list_field.str_field': 'b'},
                                  {'$set': {'list_field.$.marker': True}})

        expected_document = copy.deepcopy(base_document)
        expected_document['list_field'][1]['marker'] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

        self.db.collection.update({'int_field': 1, 'list_field.str_field': 'a'},
                                  {'$set': {'list_field.$.marker': True}})

        self.db.collection.update({'int_field': 1, 'list_field.str_field': 'c'},
                                  {'$set': {'list_field.$.marker': True}})

        expected_document['list_field'][0]['marker'] = True
        expected_document['list_field'][2]['marker'] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__set_replace_subdocument(self):
        base_document = {
            'int_field': 1,
            'list_field': [
                {'str_field': 'a'},
                {'str_field': 'b', 'int_field': 1},
                {'str_field': 'c'}
            ]}
        new_subdoc = {'str_field': 'x'}
        self.db.collection.insert(base_document)
        self.db.collection.update(
            {'int_field': 1},
            {'$set': {'list_field.1': new_subdoc}})

        self.db.collection.update(
            {'int_field': 1, 'list_field.2.str_field': 'c'},
            {'$set': {'list_field.2': new_subdoc}})

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
                {'str_field': 'c'}
            ]}
        new_subdoc = {'str_field': 'x'}
        self.db.collection.insert(base_document)
        self.db.collection.update(
            {'int_field': 1, 'list_field.str_field': 'b'},
            {'$set': {'list_field.$': new_subdoc}})

        expected_document = copy.deepcopy(base_document)
        expected_document['list_field'][1] = new_subdoc

        self.assertEqual(list(self.db.collection.find()), [expected_document])

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__find_and_modify_with_sort(self):
        self.db.collection.insert({'time_check': float(time.time())})
        self.db.collection.insert({'time_check': float(time.time())})
        self.db.collection.insert({'time_check': float(time.time())})

        start_check_time = float(time.time())
        self.db.collection.find_and_modify(
            {'time_check': {'$lt': start_check_time}},
            {'$set': {'time_check': float(time.time()), 'checked': True}},
            sort=[('time_check', pymongo.ASCENDING)])
        sorted_records = sorted(list(self.db.collection.find()), key=lambda x: x['time_check'])
        self.assertEqual(sorted_records[-1]['checked'], True)

        self.db.collection.find_and_modify(
            {'time_check': {'$lt': start_check_time}},
            {'$set': {'time_check': float(time.time()), 'checked': True}},
            sort=[('time_check', pymongo.ASCENDING)])

        self.db.collection.find_and_modify(
            {'time_check': {'$lt': start_check_time}},
            {'$set': {'time_check': float(time.time()), 'checked': True}},
            sort=[('time_check', pymongo.ASCENDING)])

        expected = list(filter(lambda x: 'checked' in x, list(self.db.collection.find())))
        self.assertEqual(self.db.collection.find().count(), len(expected))
        self.assertEqual(
            list(self.db.collection.find({'checked': True})), list(self.db.collection.find()))

    def test__cursor_sort_kept_after_clone(self):
        self.db.collection.insert({'time_check': float(time.time())})
        self.db.collection.insert({'time_check': float(time.time())})
        self.db.collection.insert({'time_check': float(time.time())})

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
        self.db.collection.insert({'_id': 1})
        self.db.collection.update({'_id': 1}, {'$set': test_data})

        self.db.collection.update(
            {'_id': 1}, {'$addToSet': {'test': 'another_one'}})
        data_in_db = self.db.collection.find_one({'_id': 1})
        self.assertNotEqual(data_in_db['test'], test_data['test'])
        self.assertEqual(len(test_data['test']), 1)
        self.assertEqual(len(data_in_db['test']), 2)

    def test__filter_with_ne(self):
        self.db.collection.insert({'_id': 1, 'test_list': [{'data': 'val'}]})
        data_in_db = self.db.collection.find(
            {'test_list.marker_field': {'$ne': True}})
        self.assertEqual(
            list(data_in_db), [{'_id': 1, 'test_list': [{'data': 'val'}]}])

    def test__filter_with_ne_none(self):
        self.db.collection.insert_many([
            {'_id': 1, 'field1': 'baz', 'field2': 'bar'},
            {'_id': 2, 'field1': 'baz'},
            {'_id': 3, 'field1': 'baz', 'field2': None},
            {'_id': 4, 'field1': 'baz', 'field2': False},
            {'_id': 5, 'field1': 'baz', 'field2': 0},
        ])
        data_in_db = self.db.collection.find({'field1': 'baz', 'field2': {'$ne': None}})
        self.assertEqual([1, 4, 5], [d['_id'] for d in data_in_db])

    def test__filter_unknown_top_level(self):
        with self.assertRaises(mongomock.OperationFailure) as error:
            self.db.collection.find_one({'$and': [{'$ne': False}]})
        self.assertEqual('unknown top level operator: $ne', str(error.exception))

    def test__find_or(self):
        self.db.collection.insert_many([
            {'x': 4},
            {'x': [2, 4, 6, 8]},
            {'x': [2, 3, 5, 7]},
            {'x': {}},
        ])
        self.assertEqual(
            [4, [2, 4, 6, 8], [2, 3, 5, 7]],
            [d['x'] for d in self.db.collection.find({'$or': [{'x': 4}, {'x': 2}]})])

    def test__find_with_max_time_ms(self):
        self.db.collection.insert_many([{'x': 1}, {'x': 2}])
        self.assertEqual(
            [1, 2],
            [d['x'] for d in self.db.collection.find({}, max_time_ms=1000)])

        with self.assertRaises(TypeError):
            self.db.collection.find({}, max_time_ms='1000')

    def test__find_and_project_3_level_deep_nested_field(self):
        self.db.collection.insert({'_id': 1, 'a': {'b': {'c': 2}}})
        data_in_db = self.db.collection.find(projection=['a.b.c'])
        self.assertEqual(
            list(data_in_db), [{'_id': 1, 'a': {'b': {'c': 2}}}])

    def test__find_and_project_wrong_types(self):
        self.db.collection.insert({'_id': 1, 'a': {'b': {'c': 2}}})
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

        result = self.db.collection.find_one(
            {'a': 1}, collections.OrderedDict([('a', 1), ('b.c', 1), ('b', 1)]))
        self.assertEqual(result, doc)

        result = self.db.collection.find_one(
            {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 1), ('b', 1), ('b.c', 1)]))
        self.assertEqual(result, {'a': 1, 'b': [{'c': 2}, {'c': 5}]})

        result = self.db.collection.find_one(
            {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 0), ('b', 0), ('b.c', 0)]))
        self.assertEqual(result, {'b': [{'d': 3, 'e': 4}, {'d': 6, 'e': 7}]})

        # This one is tricky: the refinement 'b' overrides the previous 'b.c'
        # but it is not the equivalent of having only 'b'.
        with self.assertRaises(NotImplementedError):
            result = self.db.collection.find_one(
                {'a': 1}, collections.OrderedDict([('_id', 0), ('a', 0), ('b.c', 0), ('b', 0)]))

    def test__find_and_project(self):
        self.db.collection.insert({'_id': 1, 'a': 42, 'b': 'other', 'c': {'d': 'nested'}})

        self.assertEqual(
            [{'_id': 1, 'a': 42}],
            list(self.db.collection.find({}, projection={'a': 1})))
        self.assertEqual(
            [{'_id': 1, 'a': 42}],
            list(self.db.collection.find({}, projection={'a': '1'})))
        self.assertEqual(
            [{'_id': 1, 'a': 42}],
            list(self.db.collection.find({}, projection={'a': '0'})))
        self.assertEqual(
            [{'_id': 1, 'a': 42}],
            list(self.db.collection.find({}, projection={'a': 'other'})))

        self.assertEqual(
            [{'_id': 1, 'b': 'other', 'c': {'d': 'nested'}}],
            list(self.db.collection.find({}, projection={'a': 0})))
        self.assertEqual(
            [{'_id': 1, 'b': 'other', 'c': {'d': 'nested'}}],
            list(self.db.collection.find({}, projection={'a': False})))

    def test__find_and_project_positional(self):
        self.db.collection.insert({'_id': 1, 'a': [{'b': 1}, {'b': 2}]})

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a.b': {'$exists': True}}, projection={'a.$.b': 0})

        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one({'a.b': {'$exists': True}}, projection={'a.$.b': 1})

    def test__with_options(self):
        self.db.collection.with_options(read_preference=None)
        self.db.collection.with_options(write_concern=self.db.collection.write_concern)
        self.db.collection.with_options(write_concern=WriteConcern(w=1))

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

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
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

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__codec_options(self):
        self.assertEqual(codec_options.CodecOptions(), self.db.collection.codec_options)

    @skipIf(_HAVE_PYMONGO, 'pymongo installed')
    def test__codec_options_without_pymongo(self):
        with self.assertRaises(NotImplementedError):
            self.db.collection.codec_options  # pylint: disable=pointless-statement

    def test__with_options_wrong_kwarg(self):
        self.assertRaises(TypeError, self.db.collection.with_options, red_preference=None)

    def test__with_options_not_implemented(self):
        _CodecOptions = collections.namedtuple(
            'CodecOptions', ['document_class', 'tz_aware', 'uuid_representation'])
        with self.assertRaises(NotImplementedError):
            self.db.collection.with_options(codec_options=_CodecOptions(None, True, 3))

    def test__with_options_wrong_type(self):
        with self.assertRaises(TypeError):
            self.db.collection.with_options(write_concern=1)

    def test__update_current_date(self):
        for type_specification in [True, {'$type': 'date'}]:
            self.db.collection.update_one(
                {}, {'$currentDate': {'updated_at': type_specification}}, upsert=True)
            self.assertIsInstance(
                self.db.collection.find_one({})['updated_at'], datetime)

    def test_datetime_precision(self):
        too_precise_dt = datetime(2000, 1, 1, 12, 30, 30, 123456)
        mongo_dt = datetime(2000, 1, 1, 12, 30, 30, 123000)
        objid = self.db.collection.insert({'date_too_precise': too_precise_dt, 'date': mongo_dt})
        self.assert_document_count(1)
        # Given both date are equivalent, we can mix them
        self.db.collection.update_one(
            {'date_too_precise': mongo_dt, 'date': too_precise_dt},
            {'$set': {'new_date_too_precise': too_precise_dt, 'new_date': mongo_dt}},
            upsert=True
        )
        self.assert_document_count(1)
        doc = self.db.collection.find_one({
            'new_date_too_precise': mongo_dt, 'new_date': too_precise_dt})
        assert doc == {
            '_id': objid,
            'date_too_precise': mongo_dt,
            'date': mongo_dt,
            'new_date_too_precise': mongo_dt,
            'new_date': mongo_dt
        }
        self.db.collection.delete_one({
            'new_date_too_precise': mongo_dt, 'new_date': too_precise_dt})
        self.assert_document_count(0)

    def test__mix_tz_naive_aware(self):
        class TZ(tzinfo):
            def fromutc(self, dt):
                return dt + self.utcoffset(dt)

            def tzname(self, dt):
                return '<dummy UTC+2>'

            def utcoffset(self, dt):
                return timedelta(seconds=2 * 3600)

            def dst(self, dt):
                return timedelta()

        utc2tz = TZ()
        naive = datetime(1999, 12, 31, 22)
        aware = datetime(2000, 1, 1, tzinfo=utc2tz)
        self.db.collection.insert({'date_aware': aware, 'date_naive': naive})
        self.assert_document_count(1)
        # Given both date are equivalent, we can mix them
        self.db.collection.update_one(
            {'date_aware': naive, 'date_naive': aware},
            {'$set': {'new_aware': aware, 'new_naive': naive}},
            upsert=True
        )
        self.assert_document_count(1)
        self.db.collection.find_one({'new_aware': naive, 'new_naive': aware})
        self.db.collection.delete_one({'new_aware': naive, 'new_naive': aware})
        self.assert_document_count(0)

    def test__configure_client_tz_aware(self):
        for tz_awarness in (True, False):
            client = mongomock.MongoClient(tz_aware=tz_awarness)
            db = client['somedb']

            class TZ(tzinfo):
                def fromutc(self, dt):
                    return dt + self.utcoffset(dt)

                def tzname(self, dt):
                    return '<dummy UTC+2>'

                def utcoffset(self, dt):
                    return timedelta(seconds=2 * 3600)

                def dst(self, dt):
                    return timedelta()

            utc2tz = TZ()
            naive = datetime(2000, 1, 1, 2, 0, 0)
            aware = datetime(2000, 1, 1, 4, 0, 0, tzinfo=utc2tz)
            if tz_awarness:
                returned = datetime(2000, 1, 1, 2, 0, 0, tzinfo=mongomock.helpers.utc)
            else:
                returned = datetime(2000, 1, 1, 2, 0, 0)
            objid = db.collection.insert({'date_aware': aware, 'date_naive': naive})

            objs = list(db.collection.find())
            assert objs == [{'_id': objid, 'date_aware': returned, 'date_naive': returned}]

            if tz_awarness:
                self.assertEqual('UTC', returned.tzinfo.tzname(returned))
                self.assertEqual(timedelta(0), returned.tzinfo.utcoffset(returned))
                self.assertEqual(timedelta(0), returned.tzinfo.dst(returned))
                self.assertEqual((timedelta(0), 'UTC'), returned.tzinfo.__getinitargs__())

            # Given both date are equivalent, we can mix them
            db.collection.update_one(
                {'date_aware': naive, 'date_naive': aware},
                {'$set': {'new_aware': aware, 'new_naive': naive}},
                upsert=True
            )

            objs = list(db.collection.find())
            assert objs == [
                {'_id': objid, 'date_aware': returned, 'date_naive': returned,
                 'new_aware': returned, 'new_naive': returned}
            ]

            ret = db.collection.find_one({'new_aware': naive, 'new_naive': aware})
            assert ret == objs[0]

            db.collection.delete_one({'new_aware': naive, 'new_naive': aware})
            objs = list(db.collection.find())
            assert not objs

    def test__list_of_dates(self):
        client = mongomock.MongoClient(tz_aware=True)
        client.db.collection.insert_one({'dates': [datetime.now(), datetime.now()]})
        dates = client.db.collection.find_one()['dates']
        self.assertTrue(dates[0].tzinfo)
        self.assertEqual(dates[0].tzinfo, dates[1].tzinfo)

    @skipIf(_HAVE_PYMONGO, 'pymongo installed')
    def test__current_date_timestamp_requires_pymongo(self):
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_one(
                {}, {'$currentDate': {
                    'updated_at': {'$type': 'timestamp'},
                    'updated_again': {'$type': 'timestamp'},
                }}, upsert=True)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__current_date_timestamp(self):
        before = datetime.now(tz_util.utc) - timedelta(seconds=1)
        self.db.collection.update_one(
            {}, {'$currentDate': {
                'updated_at': {'$type': 'timestamp'},
                'updated_again': {'$type': 'timestamp'},
            }}, upsert=True)
        after = datetime.now(tz_util.utc)

        doc = self.db.collection.find_one()
        self.assertTrue(doc.get('updated_at'))
        self.assertTrue(doc.get('updated_again'))
        self.assertNotEqual(doc['updated_at'], doc['updated_again'])

        self.assertLessEqual(before, doc['updated_at'].as_datetime())
        self.assertLessEqual(doc['updated_at'].as_datetime(), after)

    def test__rename_collection(self):
        self.db.collection.insert({'_id': 1, 'test_list': [{'data': 'val'}]})
        coll = self.db.collection

        coll.rename('other_name')

        self.assertEqual('collection', coll.name)
        self.assertEqual(
            set(['other_name']), set(self.db.list_collection_names()))
        self.assertNotEqual(coll, self.db.other_name)
        self.assertEqual([], list(coll.find()))
        data_in_db = self.db.other_name.find()
        self.assertEqual(
            [({'_id': 1, 'test_list': [{'data': 'val'}]})], list(data_in_db))

    def test__rename_collection_to_bad_names(self):
        coll = self.db.create_collection('a')
        self.assertRaises(TypeError, coll.rename, ['a'])
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
        self.assertEqual(set(['c']), set(self.db.list_collection_names()))

    def test__cursor_rewind(self):
        coll = self.db.create_collection('a')
        coll.insert({'a': 1})
        coll.insert({'a': 2})
        coll.insert({'a': 3})

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

    def test__cursor_sort_composed(self):
        coll = self.db.create_collection('a')
        coll.insert_many([
            {'_id': 1, 'a': 1, 'b': 2},
            {'_id': 2, 'a': 1, 'b': 0},
            {'_id': 3, 'a': 2, 'b': 1},
        ])

        self.assertEqual(
            [2, 1, 3], [doc['_id'] for doc in coll.find().sort((('a', 1), ('b', 1)))])
        self.assertEqual(
            [1, 2, 3],
            [doc['_id'] for doc in coll.find().sort((('a', 1), ('b', -1)))])
        self.assertEqual(
            [2, 3, 1], [doc['_id'] for doc in coll.find().sort((('b', 1), ('a', 1)))])

    def test__cursor_sort_projection(self):
        col = self.db.col
        col.insert_many([{'a': 1, 'b': 1}, {'a': 3, 'b': 3}, {'a': 2, 'b': 2}])

        self.assertEqual([1, 2, 3], [doc['b'] for doc in col.find().sort('a')])
        self.assertEqual([1, 2, 3], [doc['b'] for doc in col.find(projection=['b']).sort('a')])

    def test__cursor_max_time_ms(self):
        col = self.db.col
        col.find().max_time_ms(15)
        col.find().max_time_ms(None)

        with self.assertRaises(TypeError):
            col.find().max_time_ms(3.4)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_insert_one(self):
        operations = [pymongo.InsertOne({'a': 1, 'b': 2})]
        result = self.db.collection.bulk_write(operations)

        self.assert_document_count(1)
        doc = next(self.db.collection.find({}))
        self.assert_document_stored(doc['_id'], {'a': 1, 'b': 2})
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 0, 'nMatched': 0,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 1})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_update_one(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.UpdateOne({'a': 1}, {'$set': {'a': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 2}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 1, 'nUpserted': 0, 'nMatched': 1,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 0})

        # Upsert == True
        operations = [pymongo.UpdateOne({'a': 1}, {'$set': {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 1, 'nMatched': 0,
            'writeErrors': [], 'writeConcernErrors': [],
            'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
            'nRemoved': 0, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_update_many(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1, 'b': 1})
        self.db.collection.insert_one({'a': 1, 'b': 0})
        operations = [pymongo.UpdateMany({'a': 1}, {'$set': {'b': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'b': 2}))
        self.assertEqual(len(docs), 2)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 2, 'nUpserted': 0, 'nMatched': 2,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 0})

        # Upsert == True
        operations = [pymongo.UpdateMany({'a': 2}, {'$set': {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 1, 'nMatched': 0,
            'writeErrors': [], 'writeConcernErrors': [],
            'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
            'nRemoved': 0, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
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
        self.assertEqual(result.bulk_api_result, {
            'nModified': 1, 'nUpserted': 0, 'nMatched': 1,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 0})

        # Upsert == True
        operations = [pymongo.ReplaceOne({'a': 1}, {'a': 3}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 1, 'nMatched': 0,
            'writeErrors': [], 'writeConcernErrors': [],
            'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
            'nRemoved': 0, 'nInserted': 0})

    def test__bulk_write_update_id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1})
        bulk = self.db.collection.initialize_unordered_bulk_op()
        bulk.add_update({'a': 1}, {'$set': {'a': 2, '_id': 42}})
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            bulk.execute()
        self.assertEqual({'_id': 1, 'a': 1}, self.db.collection.find_one())
        self.assertEqual(
            ["After applying the update, the (immutable) field '_id' was found to have been "
             'altered to _id: 42'],
            [e['errmsg'] for e in err_context.exception.details['writeErrors']])

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_delete_one(self):
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.DeleteOne({'a': 1})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({}))
        self.assertEqual(len(docs), 0)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 0, 'nMatched': 0,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 1, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_delete_many(self):
        self.db.collection.insert_one({'a': 1})
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.DeleteMany({'a': 1})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({}))
        self.assertEqual(len(docs), 0)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 0, 'nMatched': 0,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 2, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_matched_count_no_changes(self):
        self.db.collection.insert_one({'name': 'luke'})
        result = self.db.collection.bulk_write([
            pymongo.ReplaceOne({'name': 'luke'}, {'name': 'luke'}),
        ])
        self.assertEqual(1, result.matched_count)
        self.assertEqual(0, result.modified_count)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__bulk_write_matched_count_replace_multiple_objects(self):
        self.db.collection.insert_one({'name': 'luke'})
        self.db.collection.insert_one({'name': 'anna'})
        result = self.db.collection.bulk_write([
            pymongo.ReplaceOne({'name': 'luke'}, {'name': 'Luke'}),
            pymongo.ReplaceOne({'name': 'anna'}, {'name': 'anna'}),
        ])
        self.assertEqual(2, result.matched_count)
        self.assertEqual(1, result.modified_count)

    def test_find_with_comment(self):
        self.db.collection.insert_one({'_id': 1})
        actual = list(self.db.collection.find({'_id': 1, '$comment': 'test'}))
        self.assertEqual([{'_id': 1}], actual)

    def test__find_or_and(self):
        self.db.collection.insert_many([
            {'x': 1, 'y': 1},
            {'x': 2, 'y': 2},
        ])
        search_filter = collections.OrderedDict([
            ('$or', [{'x': 1}, {'x': 2}]),
            ('y', 2),
        ])
        self.assertEqual([2], [d['x'] for d in self.db.collection.find(search_filter)])

    def test__aggregate_lookup(self):
        self.db.a.insert_one({'_id': 1, 'arr': [2, 4]})
        self.db.b.insert_many([
            {'_id': 2, 'should': 'include'},
            {'_id': 3, 'should': 'skip'},
            {'_id': 4, 'should': 'include'}
        ])
        actual = self.db.a.aggregate([
            {'$lookup': {
                'from': 'b',
                'localField': 'arr',
                'foreignField': '_id',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'arr': [2, 4],
            'b': [
                {'_id': 2, 'should': 'include'},
                {'_id': 4, 'should': 'include'}
            ]
        }], list(actual))

    def test__aggregate_lookup_reverse(self):
        self.db.a.insert_many([
            {'_id': 1},
            {'_id': 2},
            {'_id': 3}
        ])
        self.db.b.insert_one({'_id': 4, 'arr': [1, 3]})
        actual = self.db.a.aggregate([
            {'$lookup': {
                'from': 'b',
                'localField': '_id',
                'foreignField': 'arr',
                'as': 'b'
            }}
        ])
        self.assertEqual([
            {'_id': 1, 'b': [{'_id': 4, 'arr': [1, 3]}]},
            {'_id': 2, 'b': []},
            {'_id': 3, 'b': [{'_id': 4, 'arr': [1, 3]}]}
        ], list(actual))

    def test__aggregate_lookup_not_implemented_operators(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate([
                {'$lookup': {
                    'let': '_id'
                }}
            ])
        self.assertIn(
            "Although 'let' is a valid lookup operator for the",
            str(err.exception))

    def test__aggregate_lookup_missing_operator(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$lookup': {
                    'localField': '_id',
                    'foreignField': 'arr',
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Must specify 'from' field for a $lookup",
            str(err.exception))

    def test__aggregate_lookup_operator_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$lookup': {
                    'from': 'b',
                    'localField': 1,
                    'foreignField': 'arr',
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            'Arguments to $lookup must be strings',
            str(err.exception))

    def test__aggregate_lookup_dot_in_local_field(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate([
                {'$lookup': {
                    'from': 'b',
                    'localField': 'should.fail',
                    'foreignField': 'arr',
                    'as': 'b'
                }}
            ])
        self.assertIn(
            "Although '.' is valid in the 'localField' and 'as' parameters",
            str(err.exception))

    def test__aggregate_lookup_dot_in_as(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate([
                {'$lookup': {
                    'from': 'b',
                    'localField': '_id',
                    'foreignField': 'arr',
                    'as': 'should.fail'
                }}
            ])
        self.assertIn(
            "Although '.' is valid in the 'localField' and 'as' parameters ",
            str(err.exception))

    def test__aggregate_graph_lookup_behaves_as_lookup(self):
        self.db.a.insert_one({'_id': 1, 'arr': [2, 4]})
        self.db.b.insert_many([
            {'_id': 2, 'should': 'include'},
            {'_id': 3, 'should': 'skip'},
            {'_id': 4, 'should': 'include'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$arr',
                'connectFromField': 'should',
                'connectToField': '_id',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'arr': [2, 4],
            'b': [
                {'_id': 2, 'should': 'include'},
                {'_id': 4, 'should': 'include'}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_basic(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'include'},
            {'_id': 5, 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$item',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'item': 2,
            'b': [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'},
                {'_id': 4, 'should': 'include'}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_depth_field(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'include'},
            {'_id': 5, 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$item',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'depthField': 'dpth',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'item': 2,
            'b': [
                {'_id': 2, 'parent': 3, 'should': 'include', 'dpth': 0},
                {'_id': 3, 'parent': 4, 'should': 'include', 'dpth': 1},
                {'_id': 4, 'should': 'include', 'dpth': 2}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_multiple_connections(self):
        self.db.a.insert_one({'_id': 1, 'parent_name': 'b'})
        self.db.b.insert_many([
            {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include'},
            {'_id': 3, 'name': 'b', 'should': 'skip'},
            {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include'},
            {'_id': 5, 'name': 'd', 'parent': 'c', 'should': 'include'},
            {'_id': 6, 'name': 'e', 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$parent_name',
                'connectFromField': 'name',
                'connectToField': 'parent',
                'depthField': 'dpth',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'parent_name': 'b',
            'b': [
                {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include', 'dpth': 0},
                {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include', 'dpth': 0},
                {'_id': 5, 'name': 'd', 'parent': 'c', 'should': 'include', 'dpth': 1},
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_cyclic_pointers(self):
        self.db.a.insert_one({'_id': 1, 'parent_name': 'b'})
        self.db.b.insert_many([
            {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include'},
            {'_id': 3, 'name': 'b', 'parent': 'a', 'should': 'include'},
            {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include'},
            {'_id': 5, 'name': 'd', 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$parent_name',
                'connectFromField': 'name',
                'connectToField': 'parent',
                'depthField': 'dpth',
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'parent_name': 'b',
            'b': [
                {'_id': 2, 'name': 'a', 'parent': 'b', 'should': 'include', 'dpth': 0},
                {'_id': 4, 'name': 'c', 'parent': 'b', 'should': 'include', 'dpth': 0},
                {'_id': 3, 'name': 'b', 'parent': 'a', 'should': 'include', 'dpth': 1}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_restrict_search(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'skip'},
            {'_id': 5, 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$item',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'restrictSearchWithMatch': {'should': 'include'},
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'item': 2,
            'b': [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_max_depth(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'skip'},
            {'_id': 5, 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$item',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'maxDepth': 1,
                'as': 'b'
            }}
        ])
        self.assertEqual([{
            '_id': 1,
            'item': 2,
            'b': [
                {'_id': 2, 'parent': 3, 'should': 'include'},
                {'_id': 3, 'parent': 4, 'should': 'include'}
            ]
        }], list(actual))

    def test__aggregate_graph_lookup_max_depth_0(self):
        self.db.a.insert_one({'_id': 1, 'item': 2})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'skip'},
            {'_id': 5, 'should': 'skip'}
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$item',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'maxDepth': 0,
                'as': 'b'
            }}
        ])
        lookup_res = self.db.a.aggregate([
            {'$lookup': {
                'from': 'b',
                'localField': 'item',
                'foreignField': '_id',
                'as': 'b'
            }}
        ])
        self.assertEqual(list(lookup_res), list(actual))

    def test__aggregate_graph_lookup_from_array(self):
        self.db.a.insert_one({'_id': 1, 'items': [2, 8]})
        self.db.b.insert_many([
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'include'},
            {'_id': 5, 'should': 'skip'},
            {'_id': 6, 'should': 'include'},
            {'_id': 7, 'should': 'skip'},
            {'_id': 8, 'parent': 6, 'should': 'include'},
        ])
        actual = self.db.a.aggregate([
            {'$graphLookup': {
                'from': 'b',
                'startWith': '$items',
                'connectFromField': 'parent',
                'connectToField': '_id',
                'as': 'b'
            }}
        ])
        expected_list = [
            {'_id': 2, 'parent': 3, 'should': 'include'},
            {'_id': 3, 'parent': 4, 'should': 'include'},
            {'_id': 4, 'should': 'include'},
            {'_id': 6, 'should': 'include'},
            {'_id': 8, 'parent': 6, 'should': 'include'}
        ]
        result_list = list(actual)[0]['b']

        def sorter(doc):
            return doc['_id']
        self.assertTrue(len(expected_list) == len(result_list) and
                        sorted(expected_list, key=sorter) == sorted(result_list, key=sorter))

    def test__aggregate_graph_lookup_missing_operator(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 'arr',
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Must specify 'connectToField' field for a $graphLookup",
            str(err.exception))

    def test__aggregate_graphlookup_operator_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 1,
                    'connectToField': '_id',
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Argument 'connectFromField' to $graphLookup must be string",
            str(err.exception))

    def test__aggregate_graph_lookup_restrict_not_dict(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 'parent',
                    'connectToField': '_id',
                    'restrictSearchWithMatch': 3,
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Argument 'restrictSearchWithMatch' to $graphLookup must be a Dictionary",
            str(err.exception))

    def test__aggregate_graph_lookup_max_depth_not_number(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 'parent',
                    'connectToField': '_id',
                    'maxDepth': 's',
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Argument 'maxDepth' to $graphLookup must be a number",
            str(err.exception))

    def test__aggregate_graph_lookup_depth_filed_not_string(self):
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 'parent',
                    'connectToField': '_id',
                    'depthField': 4,
                    'as': 'b'
                }}
            ])
        self.assertEqual(
            "Argument 'depthField' to $graphlookup must be a string",
            str(err.exception))

    def test__aggregate_graph_lookup_dot_in_connect_from_field(self):
        with self.assertRaises(NotImplementedError) as err:
            self.db.a.aggregate([
                {'$graphLookup': {
                    'from': 'arr',
                    'startWith': '$_id',
                    'connectFromField': 'parent.id',
                    'connectToField': '_id',
                    'as': 'b'
                }}
            ])
        self.assertIn(
            "Although '.' is valid in the 'connectFromField' parameter",
            str(err.exception))

    def test__aggregate_sample(self):
        self.db.a.insert_many([
            {'_id': i}
            for i in range(5)
        ])

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
        self.db.a.insert_many([
            {'_id': i}
            for i in range(5)
        ])
        # Many cases for '$sample' options that should raise an operation failure.
        cases = (None, 3, {}, {'size': 2, 'otherUnknownOption': 3})
        for case in cases:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.a.aggregate([{'$sample': case}])

    def test__aggregate_count(self):
        self.db.a.insert_many([
            {'_id': 1, 'a': 1},
            {'_id': 2, 'a': 2},
            {'_id': 3, 'a': 1}
        ])

        actual = list(self.db.a.aggregate([
            {'$match': {'a': 1}},
            {'$count': 'one_count'}
        ]))
        self.assertEqual([{'one_count': 2}], actual)

    def test__aggregate_count_errors(self):
        self.db.a.insert_many([
            {'_id': i}
            for i in range(5)
        ])
        # Many cases for '$count' options that should raise an operation failure.
        cases = (None, 3, {}, [], '', '$one_count', 'one.count')
        for case in cases:
            with self.assertRaises(mongomock.OperationFailure):
                self.db.a.aggregate([{'$count': case}])

    def test__aggregate_project_array_size(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('a', {'$size': '$arr'})
            ])}
        ])
        self.assertEqual([{'a': 2}], list(actual))

    def test__aggregate_project_array_size_if_null(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        self.db.collection.insert_one({'_id': 2})
        self.db.collection.insert_one({'_id': 3, 'arr': None})
        actual = self.db.collection.aggregate([
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('a', {'$size': {'$ifNull': ['$arr', []]}})
            ])}
        ])
        self.assertEqual([{'a': 2}, {'a': 0}, {'a': 0}], list(actual))

    def test__aggregate_project_if_null(self):
        self.db.collection.insert_one({'_id': 1, 'elem_a': '<present_a>'})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('a', {'$ifNull': ['$elem_a', '<missing_a>']}),
                ('b', {'$ifNull': ['$elem_b', '<missing_b>']})
            ])}
        ])
        self.assertEqual([{'a': '<present_a>', 'b': '<missing_b>'}], list(actual))

    def test__aggregate_project_if_null_expression(self):
        self.db.collection.insert_many([
            {'_id': 1, 'description': 'Description 1', 'title': 'Title 1'},
            {'_id': 2, 'title': 'Title 2'},
            {'_id': 3, 'description': None, 'title': 'Title 3'},
        ])
        actual = self.db.collection.aggregate([{
            '$project': {
                'full_description': {'$ifNull': ['$description', '$title']},
            }
        }])
        self.assertEqual([
            {'_id': 1, 'full_description': 'Description 1'},
            {'_id': 2, 'full_description': 'Title 2'},
            {'_id': 3, 'full_description': 'Title 3'},
        ], list(actual))

    def test__aggregate_project_array_element_at(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('a', {'$arrayElemAt': ['$arr', 1]})
            ])}
        ])
        self.assertEqual([{'a': 3}], list(actual))

    def test__aggregate_project_rename__id(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('rename_id', '$_id')
            ])}
        ])
        self.assertEqual([{'rename_id': 1}], list(actual))

    def test__aggregate_project_rename_dot_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('rename_dot', '$arr.a')
            ])}
        ])
        self.assertEqual([{'rename_dot': 2}], list(actual))

    def test__aggregate_project_missing_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('_id', False),
                ('rename_dot', '$arr.c')
            ])}
        ])
        self.assertEqual([{}], list(actual))

    def test__aggregate_project_out(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        old_actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {'$project': collections.OrderedDict([
                ('rename_dot', '$arr.a')
            ])},
            {'$out': 'new_collection'}
        ])
        new_collection = self.db.get_collection('new_collection')
        new_actual = list(new_collection.find())
        expect = [{'_id': 1, 'rename_dot': 2}]

        self.assertEqual(expect, new_actual)
        self.assertEqual(expect, list(old_actual))

    def test__aggregate_project_include_in_exclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': collections.OrderedDict([
                    ('a', False),
                    ('b', True)
                ])}
            ])
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_exclude_in_inclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': collections.OrderedDict([
                    ('a', True),
                    ('b', False)
                ])}
            ])
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_computed_field_in_exclusion(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': {'a': 0, 'b': '$a'}},
            ])
        self.assertIn('Bad projection specification', str(err.exception))

    def test__aggregate_project_id_can_always_be_excluded(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate([
            {'$project': collections.OrderedDict([
                ('a', True),
                ('b', True),
                ('_id', False)
            ])}
        ])
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

    def test__aggregate_project_inclusion_with_only_id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate([
            {'$project': {'_id': True}}
        ])
        self.assertEqual([{'_id': 1}], list(actual))

    def test__aggregate_project_exclusion_with_only_id(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3})
        actual = self.db.collection.aggregate([
            {'$project': {'_id': False}}
        ])
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

        actual = self.db.collection.aggregate([
            {'$project': {'_id': 0}}
        ])
        self.assertEqual([{'a': 2, 'b': 3}], list(actual))

    def test__aggregate_project_subfield(self):
        self.db.collection.insert_many([
            {'_id': 1, 'a': {'b': 3}, 'other': 1},
            {'_id': 2, 'a': {'c': 3}},
            {'_id': 3, 'b': {'c': 3}},
            {'_id': 4, 'a': 5},
        ])
        self.assertEqual(
            [
                {'_id': 1, 'a': {'b': 3}},
                {'_id': 2, 'a': {}},
                {'_id': 3},
                {'_id': 4},
            ],
            list(self.db.collection.aggregate([
                {'$project': {'a.b': 1}},
            ])),
        )

    def test__aggregate_project_subfield_exclude(self):
        self.db.collection.insert_many([
            {'_id': 1, 'a': {'b': 3}, 'other': 1},
            {'_id': 2, 'a': {'c': 3}},
            {'_id': 3, 'b': {'c': 3}},
            {'_id': 4, 'a': 5},
        ])
        self.assertEqual(
            [
                {'_id': 1, 'a': {}, 'other': 1},
                {'_id': 2, 'a': {'c': 3}},
                {'_id': 3, 'b': {'c': 3}},
                {'_id': 4, 'a': 5},
            ],
            list(self.db.collection.aggregate([
                {'$project': {'a.b': 0}},
            ])),
        )

    def test__aggregate_project_subfield_conflict(self):
        self.db.collection.insert_many([
            {'_id': 1, 'a': {'b': 3}, 'other': 1},
            {'_id': 2, 'a': {'c': 3}},
            {'_id': 3, 'b': {'c': 3}},
        ])
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([
                {'$project': collections.OrderedDict([('a.b', 1), ('a', 1)])},
            ]))
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([
                {'$project': collections.OrderedDict([('a', 1), ('a.b', 1)])},
            ]))
        with self.assertRaises(mongomock.OperationFailure):
            list(self.db.collection.aggregate([
                {'$project': collections.OrderedDict([('d.e.f', 1), ('d.e.f.g', 1)])},
            ]))

    def test__aggregate_project_group_operations(self):
        self.db.collection.insert_one({'_id': 1, 'a': 2, 'b': 3, 'c': '$d'})
        actual = self.db.collection.aggregate([{'$project': {
            '_id': 1,
            'max': {'$max': [5, 9, '$a', None]},
            'min': {'$min': [8, '$a', None, '$b']},
            'avg': {'$avg': [4, '$a', '$b', 'a', 'b']},
            'sum': {'$sum': [4, '$a', None, '$b', 'a', 'b', {'$sum': [0, 1, '$b']}]},
            'maxString': {'$max': [{'$literal': '$b'}, '$c']},
        }}])
        self.assertEqual(
            [{'_id': 1, 'max': 9, 'min': 2, 'avg': 3, 'sum': 13, 'maxString': '$d'}],
            list(actual))

    def test__aggregate_arithmetic(self):
        self.db.collection.insert_one({
            'a': 1.5,
            'b': 2,
            'c': 2,
        })
        actual = self.db.collection.aggregate([{'$project': {
            'sum': {'$add': [15, '$a', '$b', '$c']},
            'prod': {'$multiply': [5, '$a', '$b', '$c']},
            'trunc': {'$trunc': '$a'},
        }}])
        self.assertEqual(
            [{'sum': 20.5, 'prod': 30, 'trunc': 1}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual])

    def test__aggregate_string_operations(self):
        self.db.collection.insert_one({
            'a': 'Hello',
            'b': 'World',
            'c': 3
        })
        actual = self.db.collection.aggregate([{'$project': {
            'concat': {'$concat': ['$a', ' Dear ', '$b']},
            'concat_none': {'$concat': ['$a', None, '$b']},
            'sub1': {'$substr': ['$a', 0, 4]},
            'sub2': {'$substr': ['$a', -1, 3]},
            'sub3': {'$substr': ['$a', 2, -1]},
            'lower': {'$toLower': '$a'},
            'lower_err': {'$toLower': None},
            'strcasecmp': {'$strcasecmp': ['$a', '$b']},
            'upper': {'$toUpper': '$a'},
            'upper_err': {'$toUpper': None},
        }}])
        self.assertEqual(
            [{'concat': 'Hello Dear World', 'concat_none': None, 'sub1': 'Hell', 'sub2': '',
              'sub3': 'llo', 'lower': 'hello', 'lower_err': '', 'strcasecmp': -1,
              'upper': 'HELLO', 'upper_err': ''}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual])

    def test__strcmp_not_enough_params(self):
        self.db.collection.insert_one({
            'a': 'Hello',
        })
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': {'cmp': {'$strcasecmp': ['s']}}}
            ])
        self.assertEqual(
            'strcasecmp must have 2 items',
            str(err.exception))

    def test__substr_not_enough_params(self):
        self.db.collection.insert_one({
            'a': 'Hello',
        })
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': {'sub': {'$substr': ['$a', 1]}}}
            ])
        self.assertEqual(
            'substr must have 3 items',
            str(err.exception))

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__aggregate_tostr_operation_objectid(self):
        self.db.collection.insert_one({
            'a': ObjectId('5abcfad1fbc93d00080cfe66')
        })
        actual = self.db.collection.aggregate([{'$project': {
            'toString': {'$toString': '$a'},
        }}])
        self.assertEqual(
            [{'toString': '5abcfad1fbc93d00080cfe66'}],
            [{k: v for k, v in doc.items() if k != '_id'} for doc in actual])

    def test__aggregate_unrecognized(self):
        self.db.collection.insert_one({})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.aggregate([
                {'$project': {'a': {'$notAValidOperation': True}}}
            ])

    def test__aggregate_not_implemented(self):
        self.db.collection.insert_one({})
        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$and': [True, False]}}}
            ])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$stdDevPop': 'scores'}}},
            ])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([{'$project': {
                'a': {'$let': {
                    'vars': {'a': 1},
                    'in': {'$multiply': ['$$a', 3]},
                }},
            }}])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$cmp': [1, 2]}}},
            ])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$dateToString': {'date': datetime.now()}}}},
            ])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$concatArrays': [[0, 1], [2, 3]]}}},
            ])

        with self.assertRaises(NotImplementedError):
            self.db.collection.aggregate([
                {'$project': {'a': {'$setEquals': [[2], [1, 2, 3]]}}},
            ])

    def test__aggregate_project_rotate(self):
        self.db.collection.insert_one({'_id': 1, 'a': 1, 'b': 2, 'c': 3})
        actual = self.db.collection.aggregate([
            {'$project': {'a': '$b', 'b': '$a', 'c': 1}},
        ])
        self.assertEqual([{'_id': 1, 'a': 2, 'b': 1, 'c': 3}], list(actual))

    def test__find_type_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        actual = self.db.collection.find(
            {'arr': {'$type': 'array'}})
        expect = [{'_id': 1, 'arr': [1, 2]}]

        self.assertEqual(expect, list(actual))

    def test__find_type_object(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        actual = self.db.collection.find({'arr': {'$type': 'object'}})
        expect = [{'_id': 2, 'arr': {'a': 4, 'b': 5}}]

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
        actual = self.db.collection.find(
            {'arr': {'$eq': None}},
            projection=['_id']
        )
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

    def test__unwind_no_prefix(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        with self.assertRaises(ValueError) as err:
            self.db.collection.aggregate([
                {'$unwind': 'arr'}
            ])
        self.assertEqual(
            "$unwind failed: exception: field path references must be prefixed with a '$' 'arr'",
            str(err.exception))

    def test__unwind_dict_options(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        actual = self.db.collection.aggregate([
            {'$unwind': {'path': '$arr'}}
        ])
        self.assertEqual(
            [
                {'_id': 1, 'arr': 1},
                {'_id': 1, 'arr': 2},
            ],
            list(actual))

    def test__unwind_not_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': 1})
        actual = self.db.collection.aggregate([{'$unwind': '$arr'}])
        self.assertEqual([{'_id': 1, 'arr': 1}], list(actual))

    def test__unwind_include_array_index(self):
        self.db.collection.insert_many([
            {'_id': 1, 'item': 'ABC', 'sizes': ['S', 'M', 'L']},
            {'_id': 2, 'item': 'EFG', 'sizes': []},
            {'_id': 3, 'item': 'IJK', 'sizes': 'M'},
            {'_id': 4, 'item': 'LMN'},
            {'_id': 5, 'item': 'XYZ', 'sizes': None},
        ])
        actual = self.db.collection.aggregate([
            {'$unwind': {'path': '$sizes', 'includeArrayIndex': 'arrayIndex'}}
        ])
        self.assertEqual(
            [
                {'_id': 1, 'item': 'ABC', 'sizes': 'S', 'arrayIndex': 0},
                {'_id': 1, 'item': 'ABC', 'sizes': 'M', 'arrayIndex': 1},
                {'_id': 1, 'item': 'ABC', 'sizes': 'L', 'arrayIndex': 2},
                {'_id': 3, 'item': 'IJK', 'sizes': 'M', 'arrayIndex': None},
            ],
            list(actual))

    def test__unwind_preserve_null_and_empty_arrays(self):
        self.db.collection.insert_many([
            {'_id': 1, 'item': 'ABC', 'sizes': ['S', 'M', 'L']},
            {'_id': 2, 'item': 'EFG', 'sizes': []},
            {'_id': 3, 'item': 'IJK', 'sizes': 'M'},
            {'_id': 4, 'item': 'LMN'},
            {'_id': 5, 'item': 'XYZ', 'sizes': None},
            {'_id': 6, 'item': 'abc', 'sizes': False},
        ])
        actual = self.db.collection.aggregate([
            {'$unwind': {'path': '$sizes', 'preserveNullAndEmptyArrays': True}},
        ])
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
            list(actual))

    def test__unwind_preserve_null_and_empty_arrays_on_nested(self):
        self.db.collection.insert_many([
            {'_id': 1, 'item': 'ABC', 'nest': {'sizes': ['S', 'M', 'L']}},
            {'_id': 2, 'item': 'EFG', 'nest': {'sizes': []}},
            {'_id': 3, 'item': 'IJK', 'nest': {'sizes': 'M'}},
            {'_id': 4, 'item': 'LMN', 'nest': {}},
            {'_id': 5, 'item': 'XYZ', 'nest': {'sizes': None}},
            {'_id': 6, 'item': 'abc', 'nest': {'sizes': False}},
            {'_id': 7, 'item': 'abc', 'nest': ['A', 'B', 'C']},
            {'_id': 8, 'item': 'abc', 'nest': [{'sizes': 'A'}, {'sizes': ['B', 'C']}]},
            {'_id': 9, 'item': 'def'},
        ])
        actual = self.db.collection.aggregate([
            {'$unwind': {'path': '$nest.sizes', 'preserveNullAndEmptyArrays': True}},
        ])
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
            list(actual))

    def test__array_size_non_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr0': [], 'arr3': [1, 2, 3]})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': {'size': {'$size': 'arr'}}}
            ])
        self.assertEqual(
            'The argument to $size must be an array, but was of type: %s' % type('arr'),
            str(err.exception))

    def test__array_size_argument_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        with self.assertRaises(mongomock.OperationFailure) as err:
            self.db.collection.aggregate([
                {'$project': {'size': {'$size': [1, 2, 3]}}}
            ])
        self.assertEqual(
            'Expression $size takes exactly 1 arguments. 3 were passed in.',
            str(err.exception))

    def test__array_size_valid_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr0': [], 'arr3': [1, 2, 3]})
        result1 = self.db.collection.aggregate([
            {'$project': {'size': {'$size': '$arr0'}}}
        ]).next()
        self.assertEqual(result1['size'], 0)

        result2 = self.db.collection.aggregate([
            {'$project': {'size': {'$size': '$arr3'}}}
        ]).next()
        self.assertEqual(result2['size'], 3)

    def test__array_size_valid_argument_array(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        result1 = self.db.collection.aggregate([
            {'$project': {'size': {'$size': [[1, 2]]}}}
        ]).next()
        self.assertEqual(result1['size'], 2)

        result2 = self.db.collection.aggregate([
            {'$project': {'size': {'$size': ['$arr']}}}
        ]).next()
        self.assertEqual(result2['size'], 3)

        result3 = self.db.collection.aggregate([
            {'$project': {'size': {'$size': [{'$literal': [1, 2, 3, 4, 5]}]}}}
        ]).next()
        self.assertEqual(result3['size'], 5)

    def test__array_size_valid_expression(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2, 3]})
        result = self.db.collection.aggregate([
            {'$project': {'size': {'$size': {'$literal': [1, 2, 3, 4]}}}}
        ]).next()
        self.assertEqual(result['size'], 4)

    def test__aggregate_project_out_replace(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        new_collection = self.db.get_collection('new_collection')
        new_collection.insert({'_id': 3})
        self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    'rename_dot': '$arr.a'
                }
            },
            {'$out': 'new_collection'}
        ])
        actual = list(new_collection.find())
        expect = [{'_id': 1, 'rename_dot': 2}]

        self.assertEqual(expect, actual)

    def test__all_elemmatch(self):
        self.db.collection.insert([
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
        ])
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

    def test__filter_eq_on_array(self):
        """$eq on array matches if one element of the array matches."""
        collection = self.db.collection
        collection.insert_many([
            {'_id': 1, 'shape': [{'color': 'red'}]},
            {'_id': 2, 'shape': [{'color': 'yellow'}]},
            {'_id': 3, 'shape': [{'color': 'red'}, {'color': 'yellow'}]},
            {'_id': 4, 'shape': [{'size': 3}]},
            {'_id': 5},
            {'_id': 6, 'shape': {'color': ['red', 'yellow']}},
        ])

        results = self.db.collection.find({'shape.color': {'$eq': 'red'}})
        self.assertEqual([1, 3, 6], [doc['_id'] for doc in results])

    def test__filter_ne_on_array(self):
        """$ne and $nin on array only matches if no element of the array matches."""
        collection = self.db.collection
        collection.insert_many([
            {'_id': 1, 'shape': [{'color': 'red'}]},
            {'_id': 2, 'shape': [{'color': 'yellow'}]},
            {'_id': 3, 'shape': [{'color': 'red'}, {'color': 'yellow'}]},
            {'_id': 4, 'shape': [{'size': 3}]},
            {'_id': 5},
            {'_id': 6, 'shape': {'color': ['red', 'yellow']}},
        ])

        # $ne
        results = self.db.collection.find({'shape.color': {'$ne': 'red'}})
        self.assertEqual([2, 4, 5], [doc['_id'] for doc in results])

        # $nin
        results = self.db.collection.find({'shape.color': {'$nin': ['blue', 'red']}})
        self.assertEqual([2, 4, 5], [doc['_id'] for doc in results])

    def test__filter_ne_multiple_keys(self):
        """Using $ne and another operator."""
        collection = self.db.collection
        collection.insert_many([
            {'_id': 1, 'cases': [{'total': 1}]},
            {'_id': 2, 'cases': [{'total': 2}]},
            {'_id': 3, 'cases': [{'total': 3}]},
            {'_id': 4, 'cases': []},
            {'_id': 5},
        ])

        # $ne
        results = self.db.collection.find({'cases.total': {'$gt': 1, '$ne': 3}})
        self.assertEqual([2], [doc['_id'] for doc in results])

        # $nin
        results = self.db.collection.find({'cases.total': {'$gt': 1, '$nin': [1, 3]}})
        self.assertEqual([2], [doc['_id'] for doc in results])

    def test__filter_objects_comparison(self):
        collection = self.db.collection
        query = {'counts': {'$gt': {'circles': 1}}}
        collection.insert_many([
            # Document kept: circles' value 3 is greater than 1.
            {'_id': 1, 'counts': {'circles': 3}},
            # Document kept: the first key, squares, is greater than circles.
            {'_id': 2, 'counts': {'squares': 0}},
            # Document dropped: the first key, arrows, is smaller than circles.
            {'_id': 3, 'counts': {'arrows': 15}},
            # Document dropped: the dicts are equal.
            {'_id': 4, 'counts': {'circles': 1}},
            # Document kept: the first item is equal, and there is an additional item.
            {'_id': 5, 'counts': collections.OrderedDict([
                ('circles', 1),
                ('arrows', 15),
            ])},
            # Document dropped: same as above, but order matters.
            {'_id': 6, 'counts': collections.OrderedDict([
                ('arrows', 15),
                ('circles', 1),
            ])},
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
        ])
        results = collection.find(query)
        self.assertEqual({1, 2, 5, 9, 11, 12}, {doc['_id'] for doc in results})

        query = {'counts': {'$gt': {'circles': re.compile('3')}}}
        self.assertFalse(list(collection.find(query)))

    def test__filter_objects_comparison_unknown_type(self):
        self.db.collection.insert_one({'counts': 3})
        with self.assertRaises(NotImplementedError):
            self.db.collection.find_one({'counts': {'$gt': str}})

    def test__filter_objects_nested_comparison(self):
        collection = self.db.collection
        query = {'counts': {'$gt': {'circles': {'blue': 1}}}}
        collection.insert_many([
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
        ])
        results = collection.find(query)
        self.assertEqual({1, 2, 5}, {doc['_id'] for doc in results})

    def test_filter_not_bad_value(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$not': 3}})

        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_one({'a': {'$not': {'b': 3}}})

    def test_filter_not_regex(self):
        self.db.collection.insert_many([
            {'_id': 1, 'a': 'b'},
            # Starts with a: should be excluded.
            {'_id': 2, 'a': 'a'},
            {'_id': 3, 'a': 'ba'},
            {'_id': 4}
        ])
        results = self.db.collection.find({'a': {'$not': {'$regex': '^a'}}})
        self.assertEqual({1, 3, 4}, {doc['_id'] for doc in results})

    def test_insert_many_bulk_write_error(self):
        collection = self.db.collection
        with self.assertRaises(mongomock.BulkWriteError) as cm:
            collection.insert_many([
                {'_id': 1},
                {'_id': 1}
            ])
        self.assertEqual(str(cm.exception), 'batch op errors occurred')

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_many_bulk_write_error_details(self):
        collection = self.db.collection
        with self.assertRaises(mongomock.BulkWriteError) as cm:
            collection.insert_many([
                {'_id': 1},
                {'_id': 1}
            ])
        self.assertEqual(65, cm.exception.code)
        write_errors = cm.exception.details['writeErrors']
        self.assertEqual([11000], [error.get('code') for error in write_errors])

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_bson_validation(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert({'a': {'b'}})
        if IS_PYPY or six.PY2:
            expect = "cannot encode object: set(['b']), of type: <type 'set'>"
        else:
            expect = "cannot encode object: {'b'}, of type: <class 'set'>"
        self.assertEqual(str(cm.exception), expect)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test_insert_bson_invalid_encode_type(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert({'$foo': 'bar'})
        self.assertEqual(str(cm.exception), "key '$foo' must not start with '$'")

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__update_invalid_encode_type(self):
        self.db.collection.insert_one({'_id': 1, 'foo': 'bar'})

        with self.assertRaises(InvalidDocument):
            self.db.collection.update_one({}, {'$set': {'foo': {'bar'}}})

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
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
                                {
                                    'b': '030502',
                                    'weight': 100.0
                                },
                                {
                                    'b': '030207',
                                    'weight': 100.0
                                }
                            ]
                        }
                    ],
                    'id': 'ooo',
                    'update_time': 1111
                },
                {
                    '_id': 22222,
                    'a': [
                        {
                            'class': '03',
                            'a': [
                                {
                                    'b': '030502',
                                    'weight': 99.0
                                },
                                {
                                    'b': '0302071',
                                    'weight': 100.0
                                }
                            ]
                        }
                    ],
                    'id': 'ooo',
                    'update_time': 1222
                }
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
                        'a': {'$first': '$a'}
                    }
                },
                {'$unwind': '$a'},
                {'$unwind': '$a.a'},
                {
                    '$group': {
                        '_id': '$_id',
                        'update_time': {'$first': '$update_time'},
                        'a': {
                            '$push': {
                                'b': '$a.a.b',
                                'weight': '$a.a.weight'
                            }
                        }
                    }
                },
                {'$out': 'ooo'}
            ],
            allowDiskUse=True)
        expect = [
            {
                'update_time': 1222,
                'a': [
                    {'weight': 99.0, 'b': '030502'},
                    {'weight': 100.0, 'b': '0302071'}],
                '_id': 'ooo'
            }]
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
        actual = collection.aggregate([
            {'$group': {'_id': '$a'}},
        ])
        assertCountEqual(self, [{'_id': 1}, {'_id': 2}], list(actual))

    def test__aggregate_group_missing_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 1},
                {},
                {'a': None},
            ]
        )
        actual = collection.aggregate([
            {'$group': {'_id': '$a'}},
        ])
        assertCountEqual(self, [{'_id': 1}, {'_id': None}], list(actual))

    def test__aggregate_group_dict_key(self):
        collection = self.db.collection
        collection.insert_many(
            [
                {'a': 2, 'b': 3, 'c': 4},
                {'a': 2, 'b': 3, 'c': 5},
                {'a': 1, 'b': 1, 'c': 1},
            ]
        )
        actual = collection.aggregate([
            {'$group': {'_id': {'a': '$a', 'b': '$b'}}},
        ])
        assertCountEqual(
            self,
            [{'_id': {'a': 1, 'b': 1}}, {'_id': {'a': 2, 'b': 3}}],
            list(actual)
        )

    def test_aggregate_group_sum(self):
        collection = self.db.collection
        collection.insert_many([
            {'group': 'one'},
            {'group': 'one'},
            {'group': 'one', 'data': None},
            {'group': 'one', 'data': 0},
            {'group': 'one', 'data': 2},
            {'group': 'one', 'data': {'a': 1}},
            {'group': 'one', 'data': [1, 2]},
            {'group': 'one', 'data': [3, 4]},
        ])
        actual = collection.aggregate([{'$group': {
            '_id': '$group',
            'count': {'$sum': 1},
            'countData': {'$sum': {'$cond': ['$data', 1, 0]}},
            'countDataExists': {'$sum': {'$cond': {
                'if': {'$gt': ['$data', None]},
                'then': 1,
                'else': 0,
            }}},
        }}])
        expect = [{
            '_id': 'one',
            'count': 8,
            'countData': 4,
            'countDataExists': 5,
        }]
        self.assertEqual(expect, list(actual))

    def test__aggregate_bucket(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many([
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
        ])

        actual = collection.aggregate([{'$bucket': {
            'groupBy': '$price',
            'boundaries': [0, 200, 400],
            'default': 'Other',
            'output': {
                'count': {'$sum': 1},
                'titles': {'$push': '$title'},
            },
        }}])
        expect = [
            {
                '_id': 0,
                'count': 4,
                'titles': [
                    'The Pillars of Society',
                    'Dancer',
                    'The Great Wave off Kanagawa',
                    'Blue Flower'
                ],
            },
            {
                '_id': 200,
                'count': 2,
                'titles': [
                    'Melancholy III',
                    'Composition VII'
                ],
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
        collection.insert_many([
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
        ])

        actual = collection.aggregate([{'$bucket': {
            'groupBy': '$price',
            'boundaries': [0, 200, 400, 600],
        }}])
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
            collection.aggregate([{'$bucket': {
                'groupBy': '$price',
                'boundaries': [0, 150],
            }}])

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
        self.db.collection.insert_one({
            'date': datetime(2014, 7, 4, 13, 0, 4, 20000),
        })
        actual = self.db.collection.aggregate([{'$project': {
            'since': {'$subtract': ['$date', datetime(2014, 7, 4, 13, 0, 0, 20)]},
        }}])
        self.assertEqual([4020], [d['since'] for d in actual])

    def test__aggregate_system_variables(self):
        self.db.collection.insert_many([
            {'_id': 1},
            {'_id': 2, 'parent_id': 1},
            {'_id': 3, 'parent_id': 1},
        ])
        actual = self.db.collection.aggregate([
            {'$match': {'parent_id': {'$in': [1]}}},
            {'$group': {'_id': 1, 'docs': {'$push': '$$ROOT'}}},
        ])
        self.assertEqual(
            [{'_id': 1, 'docs': [
                {'_id': 2, 'parent_id': 1},
                {'_id': 3, 'parent_id': 1},
            ]}],
            list(actual))

    def test__aggregate_select_nested(self):
        self.db.collection.insert_one({
            'base_value': 100,
            'values_list': [
                {'updated_value': 5},
                {'updated_value': 15},
            ],
            'nested_value': {
                'updated_value': 7,
            },
        })
        actual = list(self.db.collection.aggregate([
            {'$project': {
                'select_1': '$values_list.1.updated_value',
                'select_nested': '$nested_value.updated_value',
                'select_array': '$values_list.updated_value',
            }},
        ]))
        self.assertEqual(1, len(actual), msg=actual)
        actual[0].pop('_id')
        self.assertEqual({
            'select_1': 15,
            'select_nested': 7,
            'select_array': [5, 15],
        }, actual[0])

    def test__aggregate_filter(self):
        collection = self.db.collection
        collection.drop()
        collection.insert_many([
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
        ])

        actual = collection.aggregate([{'$project': {'filtered_items': {'$filter': {
            'input': '$items',
            'as': 'item',
            'cond': {'$gte': ['$$item.price', 100]},
        }}}}])
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
                    [{'$project': {'filtered_items': {'$filter': option}}}])

    def test__write_concern(self):
        self.assertEqual({}, self.db.collection.write_concern.document)
        self.assertTrue(self.db.collection.write_concern.is_server_default)
        self.assertTrue(self.db.collection.write_concern.acknowledged)

        collection = self.db.get_collection('a', write_concern=WriteConcern(
            w=2, wtimeout=100, j=True, fsync=False))
        self.assertEqual({
            'fsync': False,
            'j': True,
            'w': 2,
            'wtimeout': 100,
        }, collection.write_concern.document)

        # http://api.mongodb.com/python/current/api/pymongo/write_concern.html#pymongo.write_concern.WriteConcern.document
        collection.write_concern.document.pop('wtimeout')
        self.assertEqual({
            'fsync': False,
            'j': True,
            'w': 2,
            'wtimeout': 100,
        }, collection.write_concern.document, msg='Write concern is immutable')

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

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__read_preference(self):
        collection = self.db.get_collection('a', read_preference=ReadPreference.NEAREST)
        self.assertEqual('nearest', collection.read_preference.mongos_mode)

    def test__bulk_write_unordered(self):
        bulk = self.db.collection.initialize_unordered_bulk_op()
        bulk.insert({'_id': 1})
        bulk.insert({'_id': 2})
        bulk.insert({'_id': 1})
        bulk.insert({'_id': 3})
        bulk.insert({'_id': 1})

        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            bulk.execute()

        assertCountEqual(self, [1, 2, 3], [d['_id'] for d in self.db.collection.find()])
        self.assertEqual(3, err_context.exception.details['nInserted'])
        self.assertEqual([2, 4], [e['index'] for e in err_context.exception.details['writeErrors']])

    def test__bulk_write_ordered(self):
        bulk = self.db.collection.initialize_ordered_bulk_op()
        bulk.insert({'_id': 1})
        bulk.insert({'_id': 2})
        bulk.insert({'_id': 1})
        bulk.insert({'_id': 3})
        bulk.insert({'_id': 1})
        with self.assertRaises(mongomock.BulkWriteError) as err_context:
            bulk.execute()

        assertCountEqual(self, [1, 2], [d['_id'] for d in self.db.collection.find()])
        self.assertEqual(2, err_context.exception.details['nInserted'])
        self.assertEqual([2], [e['index'] for e in err_context.exception.details['writeErrors']])

    def test__set_union(self):
        collection = self.db.collection
        collection.insert_many([
            {'array': ['one', 'three']},
        ])
        actual = collection.aggregate([{'$project': {
            '_id': 0,
            'array': {'$setUnion': [['one', 'two'], '$array']},
            'distinct': {'$setUnion': [['one', 'two'], ['three'], ['four']]},
            'nested': {'$setUnion': [['one', 'two'], [['one', 'two']]]},
            'objects': {'$setUnion': [[{'a': 1}, {'b': 2}], [{'a': 1}, {'c': 3}]]},
        }}])
        expect = [{
            'array': ['one', 'two', 'three'],
            'distinct': ['one', 'two', 'three', 'four'],
            'nested': ['one', 'two', ['one', 'two']],
            'objects': [{'a': 1}, {'b': 2}, {'c': 3}],
        }]
        self.assertEqual(expect, list(actual))

    def test__add_to_set_missing_value(self):
        collection = self.db.collection
        collection.insert_many([
            {'key1': 'a', 'my_key': 1},
            {'key1': 'a'},
        ])
        actual = collection.aggregate([{'$group': {
            '_id': {'key1': '$key1'},
            'my_keys': {'$addToSet': '$my_key'},
        }}])
        expect = [{
            '_id': {'key1': 'a'},
            'my_keys': [1],
        }]
        self.assertEqual(expect, list(actual))
