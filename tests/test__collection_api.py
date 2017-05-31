from collections import OrderedDict
import copy
from datetime import datetime
import platform
import random
import six
from six import text_type
import time
from unittest import TestCase, skipIf
import warnings

import mongomock

try:
    from bson.errors import InvalidDocument
    import pymongo
    from pymongo import ReturnDocument
    _HAVE_PYMONGO = True
except ImportError:
    _HAVE_PYMONGO = False


warnings.simplefilter('ignore', DeprecationWarning)
IS_PYPY = platform.python_implementation() != 'CPython'


class CollectionAPITest(TestCase):

    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test__get_subcollections(self):
        self.db.a.b
        self.assertEqual(self.db.a.b.full_name, "somedb.a.b")
        self.assertEqual(self.db.a.b.name, "a.b")

        self.assertEqual(
            set(self.db.collection_names()),
            set(["a.b", "system.indexes", "a"]))

    def test__get_sibling_collection(self):
        self.db.a.database.b
        self.assertEqual(self.db.a.database.b.full_name, "somedb.b")
        self.assertEqual(self.db.a.database.b.name, "b")

    def test__get_collection_full_name(self):
        self.assertEqual(self.db.coll.name, "coll")
        self.assertEqual(self.db.coll.full_name, "somedb.coll")

    def test__get_collection_names(self):
        self.db.a
        self.db.b
        self.assertEqual(set(self.db.collection_names()), set(['a', 'b', 'system.indexes']))
        self.assertEqual(set(self.db.collection_names(True)), set(['a', 'b', 'system.indexes']))
        self.assertEqual(set(self.db.collection_names(False)), set(['a', 'b']))

        self.db.c.drop()
        self.assertEqual(set(self.db.collection_names(False)), set(['a', 'b']))

    def test__create_collection(self):
        coll = self.db.create_collection("c")
        self.assertIs(self.db.c, coll)
        self.assertRaises(mongomock.CollectionInvalid,
                          self.db.create_collection, 'c')

    def test__create_collection_bad_names(self):
        with self.assertRaises(mongomock.InvalidName):
            self.db.create_collection('')
        with self.assertRaises(mongomock.InvalidName):
            self.db.create_collection('...')

    def test__cursor_collection(self):
        self.assertIs(self.db.a.find().collection, self.db.a)

    def test__drop_collection(self):
        self.db.a
        self.db.b
        self.db.c
        self.db.drop_collection('b')
        self.db.drop_collection('b')
        self.db.drop_collection(self.db.c)
        self.assertEqual(set(self.db.collection_names()), set(['a', 'system.indexes']))

        col = self.db.a
        r = col.insert({"aa": "bb"})
        qr = col.find({"_id": r})
        self.assertEqual(qr.count(), 1)

        self.db.drop_collection("a")
        qr = col.find({"_id": r})
        self.assertEqual(qr.count(), 0)

        col = self.db.a
        r = col.insert({"aa": "bb"})
        qr = col.find({"_id": r})
        self.assertEqual(qr.count(), 1)

        self.assertTrue(isinstance(col._documents, OrderedDict))
        self.db.drop_collection(col)
        self.assertTrue(isinstance(col._documents, OrderedDict))
        qr = col.find({"_id": r})
        self.assertEqual(qr.count(), 0)

    def test__distinct_nested_field(self):
        self.db.collection.insert({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1.f2'), ['v'])

    def test__distinct_array_field(self):
        self.db.collection.insert(
            [{'f1': ['v1', 'v2', 'v1']}, {'f1': ['v2', 'v3']}])
        cursor = self.db.collection.find()
        self.assertEqual(set(cursor.distinct('f1')), set(['v1', 'v2', 'v3']))

    def test__distinct_document_field(self):
        self.db.collection.insert({'f1': {'f2': 'v2', 'f3': 'v3'}})
        cursor = self.db.collection.find()
        self.assertEqual(cursor.distinct('f1'), [{'f2': 'v2', 'f3': 'v3'}])

    def test__distinct_filter_field(self):
        self.db.collection.insert([{'f1': 'v1', 'k1': 'v1'}, {'f1': 'v2', 'k1': 'v1'},
                                   {'f1': 'v3', 'k1': 'v2'}])
        self.assertEqual(set(self.db.collection.distinct('f1', {'k1': 'v1'})), set(['v1', 'v2']))

    def test__cursor_clone(self):
        self.db.collection.insert([{"a": "b"}, {"b": "c"}, {"c": "d"}])
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

    def test_cursor_returns_document_copies(self):
        obj = {'a': 1, 'b': 2}
        self.db.collection.insert(obj)
        fetched_obj = self.db.collection.find_one({'a': 1})
        self.assertEqual(fetched_obj, obj)
        fetched_obj['b'] = 3
        refetched_obj = self.db.collection.find_one({'a': 1})
        self.assertNotEqual(fetched_obj, refetched_obj)

    def test__update_retval(self):
        self.db.col.save({"a": 1})
        retval = self.db.col.update({"a": 1}, {"b": 2})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[text_type("connectionId")], int)
        self.assertIsNone(retval[text_type("err")])
        self.assertEqual(retval[text_type("n")], 1)
        self.assertTrue(retval[text_type("updatedExisting")])
        self.assertEqual(retval["ok"], 1.0)

        self.assertEqual(self.db.col.update({"bla": 1}, {"bla": 2})["n"], 0)

    def test__remove_retval(self):
        self.db.col.save({"a": 1})
        retval = self.db.col.remove({"a": 1})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[text_type("connectionId")], int)
        self.assertIsNone(retval[text_type("err")])
        self.assertEqual(retval[text_type("n")], 1)
        self.assertEqual(retval[text_type("ok")], 1.0)

        self.assertEqual(self.db.col.remove({"bla": 1})["n"], 0)

    def test__remove_write_concern(self):
        self.db.col.remove({"a": 1}, w=None, wtimeout=None, j=None, fsync=None)

    def test__remove_bad_write_concern(self):
        with self.assertRaises(TypeError):
            self.db.col.remove({"a": 1}, bad_kwarg=1)

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
        self.assertIn("_id", doc)
        self.assertNotIn("collection", doc)

    def test__getting_collection_via_getitem(self):
        col1 = self.db['some_collection_here']
        col2 = self.db['some_collection_here']
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db.some_collection_here)
        self.assertIsInstance(col1, mongomock.Collection)

    def test__cannot_save_non_string_keys(self):
        for key in [2, 2.0, True, object()]:
            with self.assertRaises(ValueError):
                self.db.col1.save({key: "value"})

    def assert_document_count(self, count=1):
        self.assertEqual(len(self.db.collection._documents), count)

    def assert_document_stored(self, doc_id, expected=None):
        self.assertIn(doc_id, self.db.collection._documents)
        if expected is not None:
            expected = expected.copy()
            expected['_id'] = doc_id
            doc = self.db.collection._documents[doc_id]
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

    def test__insert_many_type_error(self):
        with self.assertRaises(TypeError):
            self.db.collection.insert_many({'a': 1})
        self.assert_document_count(0)

        with self.assertRaises(TypeError):
            self.db.collection.insert_many('a')
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

    def test__find_returns_cursors(self):
        collection = self.db.collection
        self.assertEqual(type(collection.find()).__name__, "Cursor")
        self.assertNotIsInstance(collection.find(), list)
        self.assertNotIsInstance(collection.find(), tuple)

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
            self.db.collection.find_and_modify({"a": 2}, {"a": 3}, remove=True)

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

        if _HAVE_PYMONGO:
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

        if _HAVE_PYMONGO:
            update = {'x': 4, 's': 1}
            doc = self.db.collection.find_one_and_update(
                {'s': 1}, {'$set': update},
                return_document=ReturnDocument.AFTER)
            doc.pop('_id')
            self.assertDictEqual(doc, update)

    def test__update_interns_lists_and_dicts(self):
        obj = {}
        obj_id = self.db.collection.save(obj)
        d = {}
        l = []
        self.db.collection.update({"_id": obj_id}, {"d": d, "l": l})
        d["a"] = "b"
        l.append(1)
        self.assertEqual(
            list(self.db.collection.find()),
            [{"_id": obj_id, "d": {}, "l": []}])

    def test__update_cannot_change__id(self):
        self.db.collection.insert({'_id': 1, 'a': 1})
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.update({'_id': 1}, {'_id': 2, 'b': 2})

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

    def test__update_one_unset_position(self):
        insert_result = self.db.collection.insert_one({'a': 1, 'b': [{'c': 2, 'd': 3}]})
        update_result = self.db.collection.update_one(
            filter={'a': 1, 'b': {'$elemMatch': {'c': 2, 'd': 3}}},
            update={'$unset': {'b.$.c': ''}}
        )
        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(update_result.matched_count, 1)
        self.assert_document_stored(insert_result.inserted_id, {'a': 1, 'b': [{'d': 3}]})

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
        self.assertEqual(count, 2)

    def test__cursor_getitem_negative_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(IndexError):
            cursor[-1]

    def test__cursor_getitem_bad_index(self):
        first = {'name': 'first'}
        second = {'name': 'second'}
        third = {'name': 'third'}
        self.db['coll_name'].insert([first, second, third])
        cursor = self.db['coll_name'].find()
        with self.assertRaises(TypeError):
            cursor['not_a_number']

    def test__find_with_skip_param(self):
        """Make sure that find() will take in account skip parameter"""

        u1 = {'name': 'first'}
        u2 = {'name': 'second'}
        self.db['users'].insert([u1, u2])
        self.assertEqual(
            self.db['users'].find(
                sort=[
                    ("name", 1)], skip=1).count(with_limit_and_skip=True), 1)
        self.assertEqual(
            self.db['users'].find(
                sort=[
                    ("name", 1)], skip=1)[0]['name'], 'second')

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

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__create_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.create_index([("value", pymongo.ASCENDING)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__create_uniq_idxs_with_descending_ordering(self):
        self.db.collection.create_index([("value", pymongo.DESCENDING)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__create_uniq_idxs_without_ordering(self):
        self.db.collection.create_index([("value", 1)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__ensure_uniq_idxs_with_ascending_ordering(self):
        self.db.collection.ensure_index([("value", pymongo.ASCENDING)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__ensure_uniq_idxs_with_descending_ordering(self):
        self.db.collection.ensure_index([("value", pymongo.DESCENDING)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__ensure_uniq_idxs_without_ordering(self):
        self.db.collection.ensure_index([("value", 1)], unique=True)

        self.db.collection.insert({"value": 1})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({"value": 1})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test__insert_empty_doc_uniq_idx(self):
        self.db.collection.ensure_index([("value", 1)], unique=True)

        self.db.collection.insert({"value": 1})
        self.db.collection.insert({})

        self.assertEqual(self.db.collection.find({}).count(), 2)

    def test__insert_empty_doc_twice_uniq_idx(self):
        self.db.collection.ensure_index([("value", 1)], unique=True)

        self.db.collection.insert({})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({})

        self.assertEqual(self.db.collection.find({}).count(), 1)

    def test_sparse_unique_index(self):
        self.db.collection.ensure_index([("value", 1)], unique=True, sparse=True)

        self.db.collection.insert({})
        self.db.collection.insert({})

        self.assertEqual(self.db.collection.find({}).count(), 2)

    def test_sparse_unique_index_dup(self):
        self.db.collection.ensure_index([("value", 1)], unique=True, sparse=True)

        self.db.collection.insert({})
        self.db.collection.insert({})
        self.db.collection.insert({'value': 'a'})
        with self.assertRaises(mongomock.DuplicateKeyError):
            self.db.collection.insert({'value': 'a'})

        self.assertEqual(self.db.collection.find({}).count(), 3)

    def test__set_with_positional_operator(self):
        """Real mongodb support positional operator $ for $set operation"""
        base_document = {"int_field": 1,
                         "list_field": [{"str_field": "a"},
                                        {"str_field": "b"},
                                        {"str_field": "c"}]}

        self.db.collection.insert(base_document)
        self.db.collection.update({"int_field": 1, "list_field.str_field": "b"},
                                  {"$set": {"list_field.$.marker": True}})

        expected_document = copy.deepcopy(base_document)
        expected_document["list_field"][1]["marker"] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

        self.db.collection.update({"int_field": 1, "list_field.str_field": "a"},
                                  {"$set": {"list_field.$.marker": True}})

        self.db.collection.update({"int_field": 1, "list_field.str_field": "c"},
                                  {"$set": {"list_field.$.marker": True}})

        expected_document["list_field"][0]["marker"] = True
        expected_document["list_field"][2]["marker"] = True
        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__set_replace_subdocument(self):
        base_document = {
            "int_field": 1,
            "list_field": [
                {"str_field": "a"},
                {"str_field": "b", "int_field": 1},
                {"str_field": "c"}
            ]}
        new_subdoc = {"str_field": "x"}
        self.db.collection.insert(base_document)
        self.db.collection.update(
            {"int_field": 1},
            {"$set": {"list_field.1": new_subdoc}})

        self.db.collection.update(
            {"int_field": 1, "list_field.2.str_field": "c"},
            {"$set": {"list_field.2": new_subdoc}})

        expected_document = copy.deepcopy(base_document)
        expected_document["list_field"][1] = new_subdoc
        expected_document["list_field"][2] = new_subdoc

        self.assertEqual(list(self.db.collection.find()), [expected_document])

    def test__set_replace_subdocument_positional_operator(self):
        base_document = {
            "int_field": 1,
            "list_field": [
                {"str_field": "a"},
                {"str_field": "b", "int_field": 1},
                {"str_field": "c"}
            ]}
        new_subdoc = {"str_field": "x"}
        self.db.collection.insert(base_document)
        self.db.collection.update(
            {"int_field": 1, "list_field.str_field": "b"},
            {"$set": {"list_field.$": new_subdoc}})

        expected_document = copy.deepcopy(base_document)
        expected_document["list_field"][1] = new_subdoc

        self.assertEqual(list(self.db.collection.find()), [expected_document])

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__find_and_modify_with_sort(self):
        self.db.collection.insert({"time_check": float(time.time())})
        self.db.collection.insert({"time_check": float(time.time())})
        self.db.collection.insert({"time_check": float(time.time())})

        start_check_time = float(time.time())
        self.db.collection.find_and_modify(
            {"time_check": {'$lt': start_check_time}},
            {"$set": {"time_check": float(time.time()), "checked": True}},
            sort=[("time_check", pymongo.ASCENDING)])
        sorted_records = sorted(list(self.db.collection.find()), key=lambda x: x["time_check"])
        self.assertEqual(sorted_records[-1]["checked"], True)

        self.db.collection.find_and_modify(
            {"time_check": {'$lt': start_check_time}},
            {"$set": {"time_check": float(time.time()), "checked": True}},
            sort=[("time_check", pymongo.ASCENDING)])

        self.db.collection.find_and_modify(
            {"time_check": {'$lt': start_check_time}},
            {"$set": {"time_check": float(time.time()), "checked": True}},
            sort=[("time_check", pymongo.ASCENDING)])

        expected = list(filter(lambda x: "checked" in x, list(self.db.collection.find())))
        self.assertEqual(self.db.collection.find().count(), len(expected))
        self.assertEqual(
            list(self.db.collection.find({"checked": True})), list(self.db.collection.find()))

    def test__avoid_change_data_after_set(self):
        test_data = {"test": ["test_data"]}
        self.db.collection.insert({"_id": 1})
        self.db.collection.update({"_id": 1}, {"$set": test_data})

        self.db.collection.update(
            {"_id": 1}, {"$addToSet": {"test": "another_one"}})
        data_in_db = self.db.collection.find_one({"_id": 1})
        self.assertNotEqual(data_in_db["test"], test_data["test"])
        self.assertEqual(len(test_data["test"]), 1)
        self.assertEqual(len(data_in_db["test"]), 2)

    def test__filter_with_ne(self):
        self.db.collection.insert({"_id": 1, "test_list": [{"data": "val"}]})
        data_in_db = self.db.collection.find(
            {"test_list.marker_field": {"$ne": True}})
        self.assertEqual(
            list(data_in_db), [{"_id": 1, "test_list": [{"data": "val"}]}])

    def test__find_and_project_3_level_deep_nested_field(self):
        self.db.collection.insert({"_id": 1, "a": {"b": {"c": 2}}})
        data_in_db = self.db.collection.find(projection=['a.b.c'])
        self.assertEqual(
            list(data_in_db), [{"_id": 1, "a": {"b": {"c": 2}}}])

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

        result = self.db.collection.find_one({'a': 1}, {'_id': 0, 'a': 0, 'b.c': 0, 'b.c.f': 0})
        self.assertEqual(result, {'b': [{'c': 2, 'd': 3, 'e': 4}, {'c': 5, 'd': 6, 'e': 7}]})

    def test__with_options(self):
        self.db.collection.with_options(read_preference=None)

    def test__update_current_date(self):
        for type_specification in [True, {'$type': 'date'}]:
            self.db.collection.update_one(
                {}, {'$currentDate': {'updated_at': type_specification}}, upsert=True)
            self.assertIsInstance(
                self.db.collection.find_one({})['updated_at'], datetime)

    # should be removed once Timestamp supported or implemented
    def test__current_date_timestamp_is_not_supported_yet(self):
        with self.assertRaises(NotImplementedError):
            self.db.collection.update_one(
                {}, {'$currentDate': {'updated_at': {'$type': 'timestamp'}}}, upsert=True)

    def test__rename_collection(self):
        self.db.collection.insert({"_id": 1, "test_list": [{"data": "val"}]})
        coll = self.db.collection

        coll.rename("other_name")

        self.assertEqual("other_name", coll.name)
        self.assertEqual(
            set(["other_name"]), set(self.db.collection_names(False)))
        self.assertEqual(coll, self.db.other_name)
        data_in_db = coll.find()
        self.assertEqual(
            [({"_id": 1, "test_list": [{"data": "val"}]})], list(data_in_db))

    def test__rename_collectiont_to_bad_names(self):
        coll = self.db.create_collection("a")
        self.assertRaises(TypeError, coll.rename, ["a"])
        self.assertRaises(mongomock.InvalidName, coll.rename, ".a")
        self.assertRaises(mongomock.InvalidName, coll.rename, "$a")

    def test__rename_collection_already_exists(self):
        coll = self.db.create_collection("a")
        self.db.create_collection("c")
        self.assertRaises(mongomock.OperationFailure, coll.rename, "c")

    def test__rename_collection_drop_target(self):
        coll = self.db.create_collection("a")
        self.db.create_collection("c")
        coll.rename("c", dropTarget=True)
        self.assertEqual(set(["c"]), set(self.db.collection_names(False)))

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

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
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

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__bulk_write_update_one(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1})
        operations = [pymongo.UpdateOne({'a': 1}, {"$set": {'a': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 2}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 1, 'nUpserted': 0, 'nMatched': 1,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 0})

        # Upsert == True
        operations = [pymongo.UpdateOne({'a': 1}, {"$set": {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 1, 'nMatched': 0,
            'writeErrors': [], 'writeConcernErrors': [],
            'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
            'nRemoved': 0, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test__bulk_write_update_many(self):
        # Upsert == False
        self.db.collection.insert_one({'a': 1, 'b': 1})
        self.db.collection.insert_one({'a': 1, 'b': 0})
        operations = [pymongo.UpdateMany({'a': 1}, {"$set": {'b': 2}})]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'b': 2}))
        self.assertEqual(len(docs), 2)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 2, 'nUpserted': 0, 'nMatched': 2,
            'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
            'nRemoved': 0, 'nInserted': 0})

        # Upsert == True
        operations = [pymongo.UpdateMany({'a': 2}, {"$set": {'a': 3}}, upsert=True)]
        result = self.db.collection.bulk_write(operations)

        docs = list(self.db.collection.find({'a': 3}))
        self.assertEqual(len(docs), 1)
        self.assertIsInstance(result, mongomock.results.BulkWriteResult)
        self.assertEqual(result.bulk_api_result, {
            'nModified': 0, 'nUpserted': 1, 'nMatched': 0,
            'writeErrors': [], 'writeConcernErrors': [],
            'upserted': [{'_id': docs[0]['_id'], 'index': 0}],
            'nRemoved': 0, 'nInserted': 0})

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
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

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
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

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
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

    def test_find_with_comment(self):
        self.db.collection.insert_one({'_id': 1})
        actual = list(self.db.collection.find({'_id': 1, '$comment': 'test'}))
        self.assertEqual([{'_id': 1}], actual)

    def test__aggregate_project_array_element_at(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    '_id': False,
                    'a': {
                        '$arrayElemAt': ['$arr', 1]
                    }
                }
            }
        ])
        self.assertEqual([{'a': 3}], list(actual))

    def test__aggregate_project_rename__id(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [2, 3]})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    '_id': False,
                    'rename_id': '$_id',
                    'a': {
                        '$arrayElemAt': ['$arr', 1]
                    }
                }
            }
        ])
        self.assertEqual([{'a': 3, 'rename_id': 1}],
                         list(actual))

    def test__aggregate_project_rename_dot_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    '_id': False,
                    'rename_dot': '$arr.a'
                }
            }
        ])
        self.assertEqual([{'rename_dot': 2}],
                         list(actual))

    def test__aggregate_project_missing_fields(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    '_id': False,
                    'rename_dot': '$arr.c'
                }
            }
        ])
        self.assertEqual([{}],
                         list(actual))

    def test__aggregate_project_out(self):
        self.db.collection.insert_one({'_id': 1, 'arr': {'a': 2, 'b': 3}})
        self.db.collection.insert_one({'_id': 2, 'arr': {'a': 4, 'b': 5}})
        old_actual = self.db.collection.aggregate([
            {'$match': {'_id': 1}},
            {
                '$project': {
                    'rename_dot': '$arr.a'
                }
            },
            {'$out': 'new_collection'}
        ])
        new_collection = self.db.get_collection('new_collection')
        new_actual = list(new_collection.find())
        expect = [{'_id': 1, 'rename_dot': 2}]

        self.assertEqual(expect, new_actual)
        self.assertEqual(expect, list(old_actual))

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
        actual = self.db.collection.find(
            {'arr': {'$type': 'object'}})
        expect = [{'_id': 2, 'arr': {'a': 4, 'b': 5}}]

        self.assertEqual(expect, list(actual))

    def test__find_eq_none(self):
        self.db.collection.insert_one({'_id': 1, 'arr': None})
        self.db.collection.insert_one({'_id': 2})
        actual = self.db.collection.find(
            {'arr': {'$eq': None}},
            projection=['_id']
        )
        expect = [{'_id': 1}, {'_id': 2}]

        self.assertEqual(expect, list(actual))

    def test__unwind_no_prefix(self):
        self.db.collection.insert_one({'_id': 1, 'arr': [1, 2]})
        with self.assertRaises(ValueError) as err:
            self.db.collection.aggregate([
                {'$unwind': 'arr'}
            ])
        self.assertEqual(
            "$unwind failed: exception: field path references must be prefixed with a '$' 'arr'",
            str(err.exception))

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
                "_id": 5,
                "code": "xyz",
                "tags": ["school", "book", "bag", "headphone", "appliance"],
                "qty": [
                    {"size": "S", "num": 10, "color": "blue"},
                    {"size": "M", "num": 45, "color": "blue"},
                    {"size": "L", "num": 100, "color": "green"},
                ],
            },
            {
                "_id": 6,
                "code": "abc",
                "tags": ["appliance", "school", "book"],
                "qty": [
                    {"size": "6", "num": 100, "color": "green"},
                    {"size": "6", "num": 50, "color": "blue"},
                    {"size": "8", "num": 100, "color": "brown"},
                ],
            },
            {
                "_id": 7,
                "code": "efg",
                "tags": ["school", "book"],
                "qty": [
                    {"size": "S", "num": 10, "color": "blue"},
                    {"size": "M", "num": 100, "color": "blue"},
                    {"size": "L", "num": 100, "color": "green"},
                ],
            },
            {
                "_id": 8,
                "code": "ijk",
                "tags": ["electronics", "school"],
                "qty": [
                    {"size": "M", "num": 100, "color": "green"},
                ],
            },
        ])
        filters = {
            "qty": {
                "$all": [
                    {"$elemMatch": {"size": "M", "num": {"$gt": 50}}},
                    {"$elemMatch": {"num": 100, "color": "green"}},
                ],
            },
        }
        results = self.db.collection.find(filters)
        self.assertEqual([doc["_id"] for doc in results], [7, 8])

    def test_insert_many_bulk_write_error(self):
        collection = self.db.collection
        with self.assertRaises(mongomock.BulkWriteError) as cm:
            collection.insert_many([
                {'_id': 1},
                {'_id': 1}
            ])
        self.assertEqual(str(cm.exception), 'batch op errors occurred')

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test_insert_bson_validation(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert({"a": {"b"}})
        if IS_PYPY:
            expect = "cannot convert value of type <type 'set'> to bson"
        elif six.PY2:
            expect = "Cannot encode object: set(['b'])"
        else:
            expect = "Cannot encode object: {'b'}"
        self.assertEqual(str(cm.exception), expect)

    @skipIf(not _HAVE_PYMONGO, "pymongo not installed")
    def test_insert_bson_invalid_encode_type(self):
        collection = self.db.collection
        with self.assertRaises(InvalidDocument) as cm:
            collection.insert({"$foo": "bar"})
        self.assertEqual(str(cm.exception), "key '$foo' must not start with '$'")

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
