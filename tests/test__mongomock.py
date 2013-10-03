import copy
import time
import itertools
import re
import platform
import sys

if platform.python_version() < '2.7':
    import unittest2 as unittest
else:
    import unittest

import mongomock
from mongomock import Database

try:
    import pymongo
    from pymongo import Connection as PymongoConnection
    from pymongo import MongoClient as PymongoClient
    from bson.objectid import ObjectId
    _HAVE_PYMONGO = True
except ImportError:
    from mongomock.object_id import ObjectId
    _HAVE_PYMONGO = False
try:
    import execjs
    from bson.code import Code
    from bson.son import SON
    _HAVE_MAP_REDUCE = True
except ImportError:
    _HAVE_MAP_REDUCE = False
from tests.multicollection import MultiCollection



class TestCase(unittest.TestCase):
    pass

class InterfaceTest(TestCase):
    def test__can_create_db_without_path(self):
        conn = mongomock.Connection()
        self.assertIsNotNone(conn)
    def test__can_create_db_without_path(self):
        conn = mongomock.Connection('mongodb://localhost')
        self.assertIsNotNone(conn)

class DatabaseGettingTest(TestCase):
    def setUp(self):
        super(DatabaseGettingTest, self).setUp()
        self.conn = mongomock.Connection()
    def test__getting_database_via_getattr(self):
        db1 = self.conn.some_database_here
        db2 = self.conn.some_database_here
        self.assertIs(db1, db2)
        self.assertIs(db1, self.conn['some_database_here'])
        self.assertIsInstance(db1, Database)
        self.assertIs(db1.connection, self.conn) # 'connection' is an attribute of pymongo Database
        self.assertIs(db2.connection, self.conn)
    def test__getting_database_via_getitem(self):
        db1 = self.conn['some_database_here']
        db2 = self.conn['some_database_here']
        self.assertIs(db1, db2)
        self.assertIs(db1, self.conn.some_database_here)
        self.assertIsInstance(db1, Database)

class CollectionAPITest(TestCase):
    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.conn = mongomock.Connection()
        self.db = self.conn['somedb']

    def test__get_collection_names(self):
        self.db.a
        self.db.b
        self.assertEquals(set(self.db.collection_names()), set(['a', 'b', 'system.indexes']))

    def test__drop_collection(self):
        self.db.a
        self.db.b
        self.db.c
        self.db.drop_collection('b')
        self.db.drop_collection('b')
        self.db.drop_collection(self.db.c)
        self.assertEquals(set(self.db.collection_names()), set(['a', 'system.indexes']))

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

    def test__find_returns_cursors(self):
        collection = self.db.collection
        self.assertEquals(type(collection.find()).__name__, "Cursor")
        self.assertNotIsInstance(collection.find(), list)
        self.assertNotIsInstance(collection.find(), tuple)

    def test__find_and_modify_cannot_remove_and_new(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.db.collection.find_and_modify({}, remove=True, new=True)

    def test__find_and_modify_cannot_remove_and_update(self):
        with self.assertRaises(ValueError): # this is also what pymongo raises
            self.db.collection.find_and_modify({"a": 2}, {"a": 3}, remove=True)

    def test__string_matching(self):
        """
        Make sure strings are not treated as collections on find
        """
        self.db['abc'].save({'name':'test1'})
        self.db['abc'].save({'name':'test2'})
        #now searching for 'name':'e' returns test1
        self.assertIsNone(self.db['abc'].find_one({'name':'e'}))

    def test__collection_is_indexable(self):
        self.db['def'].save({'name':'test1'})
        self.assertTrue(self.db['def'].find({'name':'test1'}).count() > 0)
        self.assertEquals(self.db['def'].find({'name':'test1'})[0]['name'], 'test1')


@unittest.skipIf(not _HAVE_PYMONGO,"pymongo not installed")
class _CollectionComparisonTest(TestCase):
    """Compares a fake collection with the real mongo collection implementation via cross-comparison."""

    def setUp(self):
        super(_CollectionComparisonTest, self).setUp()
        self.fake_conn = self._get_mongomock_connection_class()()
        self.mongo_conn = self._connect_to_local_mongodb()
        self.db_name = "mongomock___testing_db"
        self.collection_name = "mongomock___testing_collection"
        self.mongo_conn[self.db_name][self.collection_name].remove()
        self.cmp = MultiCollection({
            "fake" : self.fake_conn[self.db_name][self.collection_name],
            "real": self.mongo_conn[self.db_name][self.collection_name],
         })

    def _connect_to_local_mongodb(self, num_retries=60):
        "Performs retries on connection refused errors (for travis-ci builds)"
        connection_class = self._get_real_connection_class()
        for retry in range(num_retries):
            if retry > 0:
                time.sleep(0.5)
            try:
                return connection_class()
            except pymongo.errors.ConnectionFailure as e:
                if retry == num_retries - 1:
                    raise
                if "connection refused" not in e.message.lower():
                    raise

class _MongoClientMixin(object):

    def _get_real_connection_class(self):
        return PymongoClient

    def _get_mongomock_connection_class(self):
        return mongomock.MongoClient

class _PymongoConnectionMixin(object):

    def _get_real_connection_class(self):
        return PymongoConnection

    def _get_mongomock_connection_class(self):
        return mongomock.Connection

class _CollectionTest(_CollectionComparisonTest):
    def test__find_is_empty(self):
        self.cmp.do.remove()
        self.cmp.compare.find()
    def test__inserting(self):
        self.cmp.do.remove()
        data = {"a" : 1, "b" : 2, "c" : "data"}
        self.cmp.do.insert(data)
        self.cmp.compare.find() # single document, no need to ignore order
    def test__bulk_insert(self):
        objs = [{"a" : 2, "b" : {"c" : 3}}, {"c" : 5}, {"d" : 7}]
        results_dict = self.cmp.do.insert(objs)
        for results in results_dict.values():
            self.assertEquals(len(results), len(objs))
            self.assertEquals(len(set(results)), len(results), "Returned object ids not unique!")
        self.cmp.compare_ignore_order.find()


    def test__save(self):
        self.cmp.do.insert({"_id" : "b"}) #add an item with a non ObjectId _id first.
        self.cmp.do.save({"_id":ObjectId(), "someProp":1}, safe=True)
        self.cmp.compare_ignore_order.find()

    def test__count(self):
        self.cmp.compare.count()
        self.cmp.do.insert({"a" : 1})
        self.cmp.compare.count()

    def test__find_one(self):
        id1 = self.cmp.do.insert({"_id":"id1", "name" : "new"})
        self.cmp.compare.find_one({"_id" : "id1"})
        self.cmp.do.insert({"_id":"id2", "name" : "another new"})
        self.cmp.compare.find_one({"_id" : "id2"}, {"_id":1})
        self.cmp.compare.find_one("id2", {"_id":1})

    def test__find_by_attributes(self):
        id1 = ObjectId()
        self.cmp.do.insert({"_id":id1, "name" : "new"})
        self.cmp.do.insert({"name" : "another new"})
        self.cmp.compare_ignore_order.find()
        self.cmp.compare.find({"_id" : id1})

    def test__find_by_attributes_return_fields(self):
        id1 = ObjectId()
        id2 = ObjectId()
        self.cmp.do.insert({"_id":id1, "name" : "new", "someOtherProp":2})
        self.cmp.do.insert({"_id":id2, "name" : "another new"})

        self.cmp.compare_ignore_order.find({},{"_id":0}) #test exclusion of _id
        self.cmp.compare_ignore_order.find({},{"_id":1,"someOtherProp":1}) #test inclusion
        self.cmp.compare_ignore_order.find({},{"_id":0,"someOtherProp":0}) #test exclusion
        self.cmp.compare_ignore_order.find({},{"_id":0,"someOtherProp":1}) #test mixed _id:0
        self.cmp.compare_ignore_order.find({},{"someOtherProp":0}) #test no _id, otherProp:0
        self.cmp.compare_ignore_order.find({},{"someOtherProp":1}) #test no _id, otherProp:1

        self.cmp.compare.find({"_id" : id1},{"_id":0}) #test exclusion of _id
        self.cmp.compare.find({"_id" : id1},{"_id":1,"someOtherProp":1}) #test inclusion
        self.cmp.compare.find({"_id" : id1},{"_id":0,"someOtherProp":0}) #test exclusion
        self.cmp.compare.find({"_id" : id1},{"_id":0,"someOtherProp":1}) #test mixed _id:0
        self.cmp.compare.find({"_id" : id1},{"someOtherProp":0}) #test no _id, otherProp:0
        self.cmp.compare.find({"_id" : id1},{"someOtherProp":1}) #test no _id, otherProp:1

    def test__find_by_dotted_attributes(self):
        """Test seaching with dot notation."""
        green_bowler = {
                'name': 'bob',
                'hat': {
                    'color': 'green',
                    'type': 'bowler'}}
        red_bowler = {
                'name': 'sam',
                'hat': {
                    'color': 'red',
                    'type': 'bowler'}}
        self.cmp.do.insert(green_bowler)
        self.cmp.do.insert(red_bowler)
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({"name" : "sam"})
        self.cmp.compare_ignore_order.find({'hat.color': 'green'})
        self.cmp.compare_ignore_order.find({'hat.type': 'bowler'})
        self.cmp.compare.find({
            'hat.color': 'red',
            'hat.type': 'bowler'
        })
        self.cmp.compare.find({
            'name': 'bob',
            'hat.color': 'red',
            'hat.type': 'bowler'
        })
        self.cmp.compare.find({'hat': 'a hat'})
        self.cmp.compare.find({'hat.color.cat': 'red'})

    def test__find_by_regex(self):
        """Test searching with regular expression objects."""
        bob = {'name': 'bob'}
        sam = {'name': 'sam'}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        self.cmp.compare_ignore_order.find()
        regex = re.compile('bob|sam')
        self.cmp.compare_ignore_order.find({"name" : regex})
        regex = re.compile('bob|notsam')
        self.cmp.compare_ignore_order.find({"name" : regex})

    def test__find_by_elemMatch(self):
        self.cmp.do.insert({"field": [{"a": 1, "b": 2}, {"c": 3, "d": 4}]})
        self.cmp.do.insert({"field": [{"a": 1, "b": 4}, {"c": 3, "d": 8}]})
        self.cmp.do.insert({"field": "nonlist"})
        self.cmp.do.insert({"field": 2})

        self.cmp.compare.find({"field": {"$elemMatch": {"b": 1}}})
        self.cmp.compare_ignore_order.find({"field": {"$elemMatch": {"a": 1}}})
        self.cmp.compare.find({"field": {"$elemMatch": {"b": {"$gt": 3}}}})

    def test__find_in_array(self):
        self.cmp.do.insert({"field": [{"a": 1, "b": 2}, {"c": 3, "d": 4}]})

        self.cmp.compare.find({"field.0.a": 1})
        self.cmp.compare.find({"field.0.b": 2})
        self.cmp.compare.find({"field.1.c": 3})
        self.cmp.compare.find({"field.1.d": 4})

    def test__find_notequal(self):
        """Test searching with operators other than equality."""
        bob = {'_id': 1, 'name': 'bob'}
        sam = {'_id': 2, 'name': 'sam'}
        a_goat = {'_id': 3, 'goatness': 'very'}
        self.cmp.do.insert([bob, sam, a_goat])
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({'name': {'$ne': 'bob'}})
        self.cmp.compare_ignore_order.find({'goatness': {'$ne': 'very'}})
        self.cmp.compare_ignore_order.find({'goatness': {'$ne': 'not very'}})
        self.cmp.compare_ignore_order.find({'snakeness': {'$ne': 'very'}})

    def test__find_not(self):
        bob = {'_id': 1, 'name': 'bob'}
        sam = {'_id': 2, 'name': 'sam'}
        self.cmp.do.insert([bob, sam])
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'bob'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'sam'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'dan'}}})

    def test__find_compare(self):
        self.cmp.do.insert(dict(noise = "longhorn"))
        for x in range(10):
            self.cmp.do.insert(dict(num = x, sqrd = x * x))
        self.cmp.compare_ignore_order.find({'sqrd':{'$lte':4}})
        self.cmp.compare_ignore_order.find({'sqrd':{'$lt':4}})
        self.cmp.compare_ignore_order.find({'sqrd':{'$gte':64}})
        self.cmp.compare_ignore_order.find({'sqrd':{'$gte':25, '$lte':36}})

    def test__find_sets(self):
        single = 4
        even = [2, 4, 6, 8]
        prime = [2, 3, 5, 7]
        self.cmp.do.insert([
            dict(x = single),
            dict(x = even),
            dict(x = prime)])
        self.cmp.compare_ignore_order.find({'x':{'$in':[7, 8]}})
        self.cmp.compare_ignore_order.find({'x':{'$in':[4, 5]}})
        self.cmp.compare_ignore_order.find({'x':{'$nin':[2, 5]}})
        self.cmp.compare_ignore_order.find({'x':{'$all':[2, 5]}})
        self.cmp.compare_ignore_order.find({'x':{'$all':[7, 8]}})
        self.cmp.compare_ignore_order.find({'x':2})
        self.cmp.compare_ignore_order.find({'x':4})
        self.cmp.compare_ignore_order.find({'$or':[{'x':4}, {'x':2}]})
        self.cmp.compare_ignore_order.find({'$or':[{'x':4}, {'x':7}]})
        self.cmp.compare_ignore_order.find({'$and':[{'x':2}, {'x':7}]})

    def test__find_and_modify_remove(self):
        self.cmp.do.insert([{"a": x} for x in range(10)])
        self.cmp.do.find_and_modify({"a": 2}, remove=True)
        self.cmp.compare_ignore_order.find()

    def test__find_sort_list(self):
        self.cmp.do.remove()
        for data in ({"a" : 1, "b" : 3, "c" : "data1"},
                     {"a" : 2, "b" : 2, "c" : "data3"},
                     {"a" : 3, "b" : 1, "c" : "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(sort = [("a", 1), ("b", -1)])
        self.cmp.compare.find(sort = [("b", 1), ("a", -1)])
        self.cmp.compare.find(sort = [("b", 1), ("a", -1), ("c", 1)])

    def test__find_limit(self):
        self.cmp.do.remove()
        for data in ({"a" : 1, "b" : 3, "c" : "data1"},
                     {"a" : 2, "b" : 2, "c" : "data3"},
                     {"a" : 3, "b" : 1, "c" : "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(limit=2, sort = [("a", 1), ("b", -1)])
        self.cmp.compare.find(limit=0, sort = [("a", 1), ("b", -1)]) #pymongo limit defaults to 0, returning everything

    def test__as_class(self):
        class MyDict(dict): pass

        self.cmp.do.remove()
        self.cmp.do.insert({"a": 1, "b": {"ba": 3, "bb": 4, "bc": [ {"bca": 5 } ] }})
        self.cmp.compare.find({}, as_class=MyDict)
        self.cmp.compare.find({"a": 1}, as_class=MyDict)

    def test__return_only_selected_fields(self):
        self.cmp.do.insert({'name':'Chucky', 'type':'doll', 'model':'v6'})
        self.cmp.compare_ignore_order.find({'name':'Chucky'}, fields = ['type'])

    def test__default_fields_to_id_if_empty(self):
        self.cmp.do.insert({'name':'Chucky', 'type':'doll', 'model':'v6'})
        self.cmp.compare_ignore_order.find({'name':'Chucky'}, fields = [])

    def test__remove(self):
        """Test the remove method."""
        self.cmp.do.insert({"value" : 1})
        self.cmp.compare_ignore_order.find()
        self.cmp.do.remove()
        self.cmp.compare.find()
        self.cmp.do.insert([
            {'name': 'bob'},
            {'name': 'sam'},
        ])
        self.cmp.compare_ignore_order.find()
        self.cmp.do.remove({'name': 'bob'})
        self.cmp.compare_ignore_order.find()
        self.cmp.do.remove({'name': 'notsam'})
        self.cmp.compare.find()
        self.cmp.do.remove({'name': 'sam'})
        self.cmp.compare.find

    def test__update(self):
        doc = {"a" : 1}
        self.cmp.do.insert(doc)
        new_document = {"new_attr" : 2}
        self.cmp.do.update({"a" : 1}, new_document)
        self.cmp.compare_ignore_order.find()

    def test__set(self):
        """Tests calling update with $set members."""
        self.cmp.do.update({'_id':42}, {'$set': {'some': 'thing'}}, upsert=True)
        self.cmp.compare.find({'_id' : 42})
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'hat': 'green'}})
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'hat': 'red'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__unset(self):
        """Tests calling update with $set members."""
        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': 0}})
        self.cmp.compare.find({'name' : 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': 1}})
        self.cmp.compare.find({'name' : 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': ""}})
        self.cmp.compare.find({'name' : 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': True}})
        self.cmp.compare.find({'name' : 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name' : 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': False}})
        self.cmp.compare.find({'name' : 'bob'})

    def test__set_upsert(self):
        self.cmp.do.remove()
        self.cmp.do.update({"name": "bob"}, {"$set": {}}, True)
        self.cmp.compare.find()
        self.cmp.do.update({"name": "alice"}, {"$set": {"age": 1}}, True)
        self.cmp.compare_ignore_order.find()

    def test__set_subdocuments(self):
        """Tests using $set for setting subdocument fields"""
        if isinstance(self, _MongoClientMixin):
            self.skipTest("MongoClient does not allow setting subdocuments on existing non-documents")
        self.cmp.do.insert({'name': 'bob', 'data1': 1, 'subdocument': {'a': {'b': {'c': 20}}}})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'data1.field1': 11}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$set': {'data2.field1': 21}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$set': {'subdocument.a.b': 21}})
        self.cmp.compare.find()

    def test__inc(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$inc': {'count':1}})
            self.cmp.compare.find({'name': 'bob'})

    def test__inc_upsert(self):
        self.cmp.do.remove()
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$inc': {'count':1}}, True)
            self.cmp.compare.find({'name': 'bob'})

    def test__inc_subdocument(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': {'age': 0}})
        self.cmp.do.update({'name':'bob'}, {'$inc': {'data.age': 1}})
        self.cmp.compare.find()
        self.cmp.do.update({'name':'bob'}, {'$inc': {'data.age2': 1}})
        self.cmp.compare.find()

    def test__inc_subdocument_positional(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': [{'age': 0}, {'age': 1}]})
        self.cmp.do.update({'name': 'bob', 'data': {'$elemMatch': {'age': 0}}},
            {'$inc': {'data.$.age': 1}})
        self.cmp.compare.find()

    def test__addToSet(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$addToSet': {'hat':'green'}})
            self.cmp.compare.find({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$addToSet': {'hat':'tall'}})
            self.cmp.compare.find({'name': 'bob'})

    def test__pull(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update({'name': 'bob'}, {'$pull': {'hat': 'green'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pull_nested_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': [{'size': 'L', 'quantity': 3}, {'size': 'XL', 'quantity': 4}], 'colors': ['green', 'blue']}, {'name': 'cap', 'sizes': [{'size': 'S', 'quantity': 10}, {'size': 'L', 'quantity': 5}], 'colors': ['blue']}]})
        self.cmp.do.update({'hat': {'$elemMatch': {'name': 'derby'}}}, {'$pull': {'hat.$.sizes': {'size': 'L'}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pull_nested_list(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': ['L', 'XL']}, {'name': 'cap', 'sizes': ['S', 'L']}]})
        self.cmp.do.update({'hat': {'$elemMatch': {'name': 'derby'}}}, {'$pull': {'hat.$.sizes': 'XL'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat': 'wide'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': ['L', 'XL']}]})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat': {'name': 'cap', 'sizes': ['S', 'L']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat': {'$each': ['wide', 'blue']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': [{'size': 'L', 'quantity': 3}, {'size': 'XL', 'quantity': 4}], 'colors': ['green', 'blue']}, {'name': 'cap', 'sizes': [{'size': 'S', 'quantity': 10}, {'size': 'L', 'quantity': 5}], 'colors': ['blue']}]})
        self.cmp.do.update({'hat': {'$elemMatch': {'name': 'derby'}}}, {'$push': {'hat.$.sizes': {'size': 'M', 'quantity': 6}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_dict_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': [{'size': 'L', 'quantity': 3}, {'size': 'XL', 'quantity': 4}], 'colors': ['green', 'blue']}, {'name': 'cap', 'sizes': [{'size': 'S', 'quantity': 10}, {'size': 'L', 'quantity': 5}], 'colors': ['blue']}]})
        self.cmp.do.update({'hat': {'$elemMatch': {'name': 'derby'}}}, {'$push': {'hat.$.sizes': {'$each': [{'size': 'M', 'quantity': 6}, {'size': 'S', 'quantity': 1}]}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_list_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'name': 'derby', 'sizes': ['L', 'XL'], 'colors': ['green', 'blue']}, {'name': 'cap', 'sizes': ['S', 'L'], 'colors': ['blue']}]})
        self.cmp.do.update({'hat': {'$elemMatch': {'name': 'derby'}}}, {'$push': {'hat.$.sizes': {'$each': ['M', 'S']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__drop(self):
        data = {'a': 1}
        self.cmp.do.insert({"name" : "another new"})
        self.cmp.do.drop()
        self.cmp.compare.find({})

    def test__ensure_index(self):
        # Does nothing - just make sure it exists and takes the right args
        self.cmp.do.ensure_index("name")
        self.cmp.do.ensure_index("hat", cache_for = 100)
        self.cmp.do.ensure_index([("name", 1), ("hat", -1)])

class MongoClientCollectionTest(_CollectionTest, _MongoClientMixin):
    pass

class PymongoCollectionTest(_CollectionTest, _PymongoConnectionMixin):
    pass

@unittest.skipIf(not _HAVE_PYMONGO,"pymongo not installed")
@unittest.skipIf(not _HAVE_MAP_REDUCE,"execjs not installed")
class CollectionMapReduceTest(TestCase):
    def setUp(self):
        self.db = mongomock.Connection().map_reduce_test
        self.data = [{"x": 1, "tags": ["dog", "cat"]},
                     {"x": 2, "tags": ["cat"]},
                     {"x": 3, "tags": ["mouse", "cat", "dog"]},
                     {"x": 4, "tags": []}]
        for item in self.data:
            self.db.things.insert(item)
        self.map_func = Code("""
                function() {
                    this.tags.forEach(function(z) {
                        emit(z, 1);
                    });
                }""")
        self.reduce_func = Code("""
                function(key, values) {
                    var total = 0;
                    for(var i = 0; i<values.length; i++) {
                        total += values[i];
                    }
                    return total;
                }""")
        self.expected_results = [{'_id': 'mouse', 'value': 1},
                                 {'_id': 'dog', 'value': 2},
                                 {'_id': 'cat', 'value': 3}]

    def test__map_reduce(self):
        result = self.db.things.map_reduce(self.map_func, self.reduce_func, 'myresults')
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 3)
        for doc in result.find():
            self.assertIn(doc, self.expected_results)

    def test__map_reduce_son(self):
        result = self.db.things.map_reduce(self.map_func, self.reduce_func, out=SON([('replace', 'results'), ('db', 'map_reduce_son_test')]))
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'results')
        self.assertEqual(result._Collection__database.name, 'map_reduce_son_test')
        self.assertEqual(result.count(), 3)
        for doc in result.find():
            self.assertIn(doc, self.expected_results)

    def test__map_reduce_full_response(self):
        expected_full_response = {'counts': {'input': 4, 'reduce': 2, 'emit': 6, 'output': 3}, 'timeMillis': 5, 'ok': 1.0, 'result': 'myresults'}
        result = self.db.things.map_reduce(self.map_func, self.reduce_func, 'myresults', full_response=True)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result['counts'], expected_full_response['counts'])
        self.assertEqual(result['result'], expected_full_response['result'])
        for doc in getattr(self.db, result['result']).find():
            self.assertIn(doc, self.expected_results)

    def test__map_reduct_with_query(self):
        expected_results = [{'_id': 'mouse', 'value': 1},
                            {'_id': 'dog', 'value': 2},
                            {'_id': 'cat', 'value': 2}]
        result = self.db.things.map_reduce(self.map_func, self.reduce_func, 'myresults', query={'tags': 'dog'})
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 3)
        for doc in result.find():
            self.assertIn(doc, expected_results)

    def test__map_reduce_with_limit(self):
        result = self.db.things.map_reduce(self.map_func, self.reduce_func, 'myresults', limit=2)
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 2)

    def test__inline_map_reduce(self):
        result = self.db.things.inline_map_reduce(self.map_func, self.reduce_func)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 3)
        for doc in result:
            self.assertIn(doc, self.expected_results)

    def test__inline_map_reduce_full_response(self):
        expected_full_response = {'counts': {'input': 4, 'reduce': 2, 'emit': 6, 'output': 3}, 'timeMillis': 5, 'ok': 1.0, 'result': [{'_id': 'cat', 'value': 3}, {'_id': 'dog', 'value': 2}, {'_id': 'mouse', 'value': 1}]}
        result = self.db.things.inline_map_reduce(self.map_func, self.reduce_func, full_response=True)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result['counts'], expected_full_response['counts'])
        for doc in result['result']:
            self.assertIn(doc, self.expected_results)


def _LIMIT(*args):
    return lambda cursor: cursor.limit(*args)

def _SORT(*args):
    return lambda cursor: cursor.sort(*args)

def _SKIP(*args):
    return lambda cursor: cursor.skip(*args)

class _SortSkipLimitTest(_CollectionComparisonTest):
    def setUp(self):
        super(_SortSkipLimitTest, self).setUp()
        self.cmp.do.insert([{"_id":i, "index" : i} for i in range(30)])
    def test__skip(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10)).find()
    def test__limit(self):
        self.cmp.compare(_SORT("index", 1), _LIMIT(10)).find()
    def test__skip_and_limit(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10), _LIMIT(10)).find()

    def test__sort_name(self):
        self.cmp.do.remove()
        for data in ({"a" : 1, "b" : 3, "c" : "data1"},
                     {"a" : 2, "b" : 2, "c" : "data3"},
                     {"a" : 3, "b" : 1, "c" : "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT("a")).find()
        self.cmp.compare(_SORT("b")).find()

    def test__sort_list(self):
        self.cmp.do.remove()
        for data in ({"a" : 1, "b" : 3, "c" : "data1"},
                     {"a" : 2, "b" : 2, "c" : "data3"},
                     {"a" : 3, "b" : 1, "c" : "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT([("a", 1), ("b", -1)])).find()
        self.cmp.compare(_SORT([("b", 1), ("a", -1)])).find()
        self.cmp.compare(_SORT([("b", 1), ("a", -1), ("c", 1)])).find()

    def test__close(self):
        # Does nothing - just make sure it exists and takes the right args
        self.cmp.do(lambda cursor: cursor.close()).find()

class MongoClientSortSkipLimitTest(_SortSkipLimitTest, _MongoClientMixin):
    pass

class PymongoConnectionSortSkipLimitTest(_SortSkipLimitTest, _PymongoConnectionMixin):
    pass

class InsertedDocumentTest(TestCase):
    def setUp(self):
        super(InsertedDocumentTest, self).setUp()
        self.collection = mongomock.Connection().db.collection
        self.data = {"a" : 1, "b" : [1, 2, 3], "c" : {"d" : 4}}
        self.orig_data = copy.deepcopy(self.data)
        self.object_id = self.collection.insert(self.data)
    def test__object_is_consistent(self):
        [object] = self.collection.find()
        self.assertEquals(object["_id"], self.object_id)
    def test__find_by_id(self):
        [object] = self.collection.find({"_id" : self.object_id})
        self.assertEquals(object, self.data)
    def test__remove_by_id(self):
        self.collection.remove(self.object_id)
        self.assertEqual(0, self.collection.count())
    def test__inserting_changes_argument(self):
        #Like pymongo, we should fill the _id in the inserted dict (odd behavior, but we need to stick to it)
        self.assertEquals(self.data, dict(self.orig_data, _id=self.object_id))
    def test__data_is_copied(self):
        [object] = self.collection.find()
        self.assertEquals(dict(self.orig_data, _id=self.object_id), object)
        self.data.pop("a")
        self.data["b"].append(5)
        self.assertEquals(dict(self.orig_data, _id=self.object_id), object)
        [object] = self.collection.find()
        self.assertEquals(dict(self.orig_data, _id=self.object_id), object)
    def test__find_returns_copied_object(self):
        [object1] = self.collection.find()
        [object2] = self.collection.find()
        self.assertEquals(object1, object2)
        self.assertIsNot(object1, object2)
        object1["b"].append("bla")
        self.assertNotEquals(object1, object2)

class ObjectIdTest(TestCase):
    def test__equal_with_same_id(self):
        obj1 = ObjectId()
        obj2 = ObjectId(str(obj1))
        self.assertEqual(obj1, obj2)
