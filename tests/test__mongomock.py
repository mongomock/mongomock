import copy
import datetime
import re
import time
from unittest import TestCase, skipIf


import mongomock
from mongomock import ConfigurationError
from mongomock import Database
from mongomock import InvalidURI
from mongomock import OperationFailure

from .utils import DBRef

try:
    from bson.objectid import ObjectId
    import pymongo
    from pymongo import MongoClient as PymongoClient
    _HAVE_PYMONGO = True
except ImportError:
    from mongomock.object_id import ObjectId
    _HAVE_PYMONGO = False
try:
    from bson.code import Code
    from bson.son import SON
    import execjs  # noqa
    _HAVE_MAP_REDUCE = True
except ImportError:
    _HAVE_MAP_REDUCE = False
from nose.tools import assert_raises
from tests.multicollection import MultiCollection


class InterfaceTest(TestCase):

    def test__can_create_db_without_path(self):
        self.assertIsNotNone(mongomock.MongoClient())

    def test__can_create_db_with_path(self):
        self.assertIsNotNone(mongomock.MongoClient('mongodb://localhost'))

    def test__repr(self):
        self.assertEqual(repr(mongomock.MongoClient()),
                         "mongomock.MongoClient('localhost', 27017)")

    def test__bad_uri_raises(self):
        with assert_raises(InvalidURI):
            mongomock.MongoClient("http://host1")

        with assert_raises(InvalidURI):
            mongomock.MongoClient("://host1")

    def test__none_uri_host(self):
        self.assertIsNotNone(mongomock.MongoClient('host1'))
        self.assertIsNotNone(mongomock.MongoClient('//host2'))
        self.assertIsNotNone(mongomock.MongoClient('mongodb:host2'))


class DatabaseGettingTest(TestCase):

    def setUp(self):
        super(DatabaseGettingTest, self).setUp()
        self.client = mongomock.MongoClient()

    def test__getting_database_via_getattr(self):
        db1 = self.client.some_database_here
        db2 = self.client.some_database_here
        self.assertIs(db1, db2)
        self.assertIs(db1, self.client['some_database_here'])
        self.assertIsInstance(db1, Database)
        self.assertIs(db1.client, self.client)
        self.assertIs(db2.client, self.client)

    def test__getting_database_via_getitem(self):
        db1 = self.client['some_database_here']
        db2 = self.client['some_database_here']
        self.assertIs(db1, db2)
        self.assertIs(db1, self.client.some_database_here)
        self.assertIsInstance(db1, Database)

    def test__drop_database(self):
        db = self.client.a
        collection = db.a
        doc_id = collection.insert({"aa": "bb"})
        result = collection.find({"_id": doc_id})
        self.assertEqual(result.count(), 1)

        self.client.drop_database("a")
        result = collection.find({"_id": doc_id})
        self.assertEqual(result.count(), 0)

        db = self.client.a
        collection = db.a

        doc_id = collection.insert({"aa": "bb"})
        result = collection.find({"_id": doc_id})
        self.assertEqual(result.count(), 1)

        self.client.drop_database(db)
        result = collection.find({"_id": doc_id})
        self.assertEqual(result.count(), 0)

    def test__alive(self):
        self.assertTrue(self.client.alive())

    def test__dereference(self):
        db = self.client.a
        collection = db.a
        to_insert = {"_id": "a", "aa": "bb"}
        collection.insert(to_insert)

        a = db.dereference(DBRef("a", "a", db.name))
        self.assertEqual(to_insert, a)

    def test__getting_default_database_valid(self):
        def gddb(uri):
            client = mongomock.MongoClient(uri)
            return client, client.get_default_database()

        c, db = gddb("mongodb://host1/foo")
        self.assertIsNotNone(db)
        self.assertIsInstance(db, Database)
        self.assertIs(db.client, c)
        self.assertIs(db, c['foo'])

        c, db = gddb("mongodb://host1/bar")
        self.assertIs(db, c['bar'])

        c, db = gddb(r"mongodb://a%00lice:f%00oo@127.0.0.1/t%00est")
        self.assertIs(db, c["t\x00est"])

        c, db = gddb("mongodb://bob:bar@[::1]:27018/admin")
        self.assertIs(db, c['admin'])

        c, db = gddb("mongodb://%24am:f%3Azzb%40zz@127.0.0.1/"
                     "admin%3F?authMechanism=MONGODB-CR")
        self.assertIs(db, c['admin?'])

    def test__getting_default_database_invalid(self):
        def client(uri):
            return mongomock.MongoClient(uri)

        c = client("mongodb://host1")
        with assert_raises(ConfigurationError):
            c.get_default_database()

        c = client("host1")
        with assert_raises(ConfigurationError):
            c.get_default_database()

        c = client("")
        with assert_raises(ConfigurationError):
            c.get_default_database()

        c = client("mongodb://host1/")
        with assert_raises(ConfigurationError):
            c.get_default_database()


@skipIf(not _HAVE_PYMONGO, "pymongo not installed")
class _CollectionComparisonTest(TestCase):
    """Compares a fake collection with the real mongo collection implementation

       This is done via cross-comparison of the results.
    """

    def setUp(self):
        super(_CollectionComparisonTest, self).setUp()
        self.fake_conn = mongomock.MongoClient()
        self.mongo_conn = self._connect_to_local_mongodb()
        self.db_name = "mongomock___testing_db"
        self.collection_name = "mongomock___testing_collection"
        self.mongo_conn[self.db_name][self.collection_name].remove()
        self.mongo_collection = self.mongo_conn[self.db_name][self.collection_name]
        self.fake_collection = self.fake_conn[self.db_name][self.collection_name]
        self.cmp = MultiCollection({
            "fake": self.fake_collection,
            "real": self.mongo_collection,
        })

    def _connect_to_local_mongodb(self, num_retries=60):
        """Performs retries on connection refused errors (for travis-ci builds)"""
        for retry in range(num_retries):
            if retry > 0:
                time.sleep(0.5)
            try:
                return PymongoClient()
            except pymongo.errors.ConnectionFailure as e:
                if retry == num_retries - 1:
                    raise
                if "connection refused" not in e.message.lower():
                    raise


class MongoClientCollectionTest(_CollectionComparisonTest):

    def test__find_is_empty(self):
        self.cmp.do.remove()
        self.cmp.compare.find()

    def test__inserting(self):
        self.cmp.do.remove()
        data = {"a": 1, "b": 2, "c": "data"}
        self.cmp.do.insert(data)
        self.cmp.compare.find()  # single document, no need to ignore order

    def test__bulk_insert(self):
        objs = [{"a": 2, "b": {"c": 3}}, {"c": 5}, {"d": 7}]
        results_dict = self.cmp.do.insert(objs)
        for results in results_dict.values():
            self.assertEqual(len(results), len(objs))
            self.assertEqual(
                len(set(results)), len(results),
                "Returned object ids not unique!")
        self.cmp.compare_ignore_order.find()

    def test__insert_one(self):
        self.cmp.do.insert_one({'a': 1})
        self.cmp.compare.find()

    def test__insert_many(self):
        self.cmp.do.insert_many([{'a': 1}, {'a': 2}])
        self.cmp.compare.find()

    def test__save(self):
        # add an item with a non ObjectId _id first.
        self.cmp.do.insert({"_id": "b"})
        self.cmp.do.save({"_id": ObjectId(), "someProp": 1})
        self.cmp.compare_ignore_order.find()

    def test__insert_object_id_as_dict(self):
        self.cmp.do.remove()

        doc_ids = [
            # simple top-level dictionary
            {'A': 1},
            # dict with value as list
            {'A': [1, 2, 3]},
            # dict with value as dict
            {'A': {'sub': {'subsub': 3}}}
        ]
        for doc_id in doc_ids:
            _id = self.cmp.do.insert({'_id': doc_id, 'a': 1})

            self.assertEqual(_id['fake'], _id['real'])
            self.assertEqual(_id['fake'], doc_id)
            self.assertEqual(_id['real'], doc_id)
            self.assertEqual(type(_id['fake']), type(_id['real']))

            self.cmp.compare.find({'_id': doc_id})

            docs = self.cmp.compare.find_one({'_id': doc_id})
            self.assertEqual(docs['fake']['_id'], doc_id)
            self.assertEqual(docs['real']['_id'], doc_id)

            self.cmp.do.remove({'_id': doc_id})

    def test__count(self):
        self.cmp.compare.count()
        self.cmp.do.insert({"a": 1})
        self.cmp.compare.count()
        self.cmp.do.insert({"a": 0})
        self.cmp.compare.count()
        self.cmp.compare.count({"a": 1})

    def test__find_one(self):
        self.cmp.do.insert({"_id": "id1", "name": "new"})
        self.cmp.compare.find_one({"_id": "id1"})
        self.cmp.do.insert({"_id": "id2", "name": "another new"})
        self.cmp.compare.find_one({"_id": "id2"}, {"_id": 1})
        self.cmp.compare.find_one("id2", {"_id": 1})

    def test__find_one_no_args(self):
        self.cmp.do.insert({"_id": "new_obj", "field": "value"})
        self.cmp.compare.find_one()

    def test__find_by_attributes(self):
        id1 = ObjectId()
        self.cmp.do.insert({"_id": id1, "name": "new"})
        self.cmp.do.insert({"name": "another new"})
        self.cmp.compare_ignore_order.find()
        self.cmp.compare.find({"_id": id1})

    def test__find_by_document(self):
        self.cmp.do.insert({"name": "new", "doc": {"key": "val"}})
        self.cmp.do.insert({"name": "another new"})
        self.cmp.compare_ignore_order.find()
        self.cmp.compare.find({"doc": {"key": "val"}})
        self.cmp.compare.find({"doc": {"key": {'$eq': 'val'}}})

    def test__find_by_attributes_return_fields(self):
        id1 = ObjectId()
        id2 = ObjectId()
        self.cmp.do.insert({"_id": id1, "name": "new", "someOtherProp": 2})
        self.cmp.do.insert({"_id": id2, "name": "another new"})

        self.cmp.compare_ignore_order.find({}, {"_id": 0})  # test exclusion of _id
        self.cmp.compare_ignore_order.find({}, {"_id": 1, "someOtherProp": 1})  # test inclusion
        self.cmp.compare_ignore_order.find({}, {"_id": 0, "someOtherProp": 0})  # test exclusion
        self.cmp.compare_ignore_order.find({}, {"_id": 0, "someOtherProp": 1})  # test mixed _id:0
        self.cmp.compare_ignore_order.find({}, {"someOtherProp": 0})  # test no _id, otherProp:0
        self.cmp.compare_ignore_order.find({}, {"someOtherProp": 1})  # test no _id, otherProp:1

        self.cmp.compare.find({"_id": id1}, {"_id": 0})  # test exclusion of _id
        self.cmp.compare.find({"_id": id1}, {"_id": 1, "someOtherProp": 1})  # test inclusion
        self.cmp.compare.find({"_id": id1}, {"_id": 0, "someOtherProp": 0})  # test exclusion
        # test mixed _id:0
        self.cmp.compare.find({"_id": id1}, {"_id": 0, "someOtherProp": 1})
        # test no _id, otherProp:0
        self.cmp.compare.find({"_id": id1}, {"someOtherProp": 0})
        # test no _id, otherProp:1
        self.cmp.compare.find({"_id": id1}, {"someOtherProp": 1})

    def test__find_by_attributes_return_fields_elemMatch(self):
        id = ObjectId()
        self.cmp.do.insert({
            '_id': id,
            'owns': [
                {'type': 'hat', 'color': 'black'},
                {'type': 'hat', 'color': 'green'},
                {'type': 't-shirt', 'color': 'black', 'size': 'small'},
                {'type': 't-shirt', 'color': 'black'},
                {'type': 't-shirt', 'color': 'white'}
            ],
            'hat': 'red'
        })
        elem = {'$elemMatch': {'type': 't-shirt', 'color': 'black'}}
        # test filtering on array field only
        self.cmp.compare.find({'_id': id}, {'owns': elem})
        # test filtering on array field with inclusion
        self.cmp.compare.find({'_id': id}, {'owns': elem, 'hat': 1})
        # test filtering on array field with exclusion
        self.cmp.compare.find({'_id': id}, {'owns': elem, 'hat': 0})
        # test filtering on non array field
        self.cmp.compare.find({'_id': id}, {'hat': elem})
        # test no match
        self.cmp.compare.find({'_id': id}, {'owns': {'$elemMatch': {'type': 'cap'}}})

    def test__size(self):
        id = ObjectId()
        self.cmp.do.insert({
            '_id': id,
            'l_string': 1,
            'l_tuple': ['a', 'b']
        })
        self.cmp.compare.find({'_id': id})
        self.cmp.compare.find({'_id': id, 'l_string': {'$not': {'$size': 0}}})
        self.cmp.compare.find({'_id': id, 'l_tuple': {'$size': 2}})

    def test__regex_match_non_string(self):
        id = ObjectId()
        self.cmp.do.insert({
            '_id': id,
            'test': 1
        })
        self.cmp.compare.find({'_id': id, 'test': {'$regex': '1'}})

    def test__find_by_dotted_attributes(self):
        """Test seaching with dot notation."""
        green_bowler = {
            'name': 'bob',
            'hat': {'color': 'green', 'type': 'bowler'}}
        red_bowler = {
            'name': 'sam',
            'hat': {'color': 'red', 'type': 'bowler'}}
        self.cmp.do.insert(green_bowler)
        self.cmp.do.insert(red_bowler)
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({"name": "sam"})
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

    def test__find_empty_array_field(self):
        # See #90
        self.cmp.do.insert({'array_field': []})
        self.cmp.compare.find({'array_field': []})

    def test__find_non_empty_array_field(self):
        # See #90
        self.cmp.do.insert({'array_field': [['abc']]})
        self.cmp.do.insert({'array_field': ['def']})
        self.cmp.compare.find({'array_field': ['abc']})
        self.cmp.compare.find({'array_field': [['abc']]})
        self.cmp.compare.find({'array_field': 'def'})
        self.cmp.compare.find({'array_field': ['def']})

    def test__find_by_objectid_in_list(self):
        # See #79
        self.cmp.do.insert(
            {'_id': 'x', 'rel_id': [ObjectId('52d669dcad547f059424f783')]})
        self.cmp.compare.find({'rel_id': ObjectId('52d669dcad547f059424f783')})

    def test__find_subselect_in_list(self):
        # See #78
        self.cmp.do.insert({'_id': 'some_id', 'a': [{'b': 1, 'c': 2}]})
        self.cmp.compare.find_one({'a.b': 1})

    def test__find_by_regex_object(self):
        """Test searching with regular expression objects."""
        bob = {'name': 'bob'}
        sam = {'name': 'sam'}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        self.cmp.compare_ignore_order.find()
        regex = re.compile('bob|sam')
        self.cmp.compare_ignore_order.find({"name": regex})
        regex = re.compile('bob|notsam')
        self.cmp.compare_ignore_order.find({"name": regex})

    def test__find_by_regex_string(self):
        """Test searching with regular expression string."""
        bob = {'name': 'bob'}
        sam = {'name': 'sam'}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({"name": {'$regex': 'bob|sam'}})
        self.cmp.compare_ignore_order.find({'name': {'$regex': 'bob|notsam'}})

    def test__find_in_array_by_regex_object(self):
        """Test searching inside array with regular expression object."""
        bob = {'name': 'bob', 'text': ['abcd', 'cde']}
        sam = {'name': 'sam', 'text': ['bde']}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        regex = re.compile('^a')
        self.cmp.compare_ignore_order.find({"text": regex})
        regex = re.compile('e$')
        self.cmp.compare_ignore_order.find({"text": regex})
        regex = re.compile('bde|cde')
        self.cmp.compare_ignore_order.find({"text": regex})

    def test__find_in_array_by_regex_string(self):
        """Test searching inside array with regular expression string"""
        bob = {'name': 'bob', 'text': ['abcd', 'cde']}
        sam = {'name': 'sam', 'text': ['bde']}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        self.cmp.compare_ignore_order.find({"text": {'$regex': '^a'}})
        self.cmp.compare_ignore_order.find({"text": {'$regex': 'e$'}})
        self.cmp.compare_ignore_order.find({"text": {'$regex': 'bcd|cde'}})

    def test__find_by_regex_string_on_absent_field_dont_break(self):
        """Test searching on absent field with regular expression string dont break"""
        bob = {'name': 'bob'}
        sam = {'name': 'sam'}
        self.cmp.do.insert(bob)
        self.cmp.do.insert(sam)
        self.cmp.compare_ignore_order.find({"text": {'$regex': 'bob|sam'}})

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
        self.cmp.compare.find({"field.0": {"$exists": True}})
        self.cmp.compare.find({"field.0": {"$exists": False}})
        self.cmp.compare.find({"field.0.a": {"$exists": True}})
        self.cmp.compare.find({"field.0.a": {"$exists": False}})
        self.cmp.compare.find({"field.1.a": {"$exists": True}})
        self.cmp.compare.find({"field.1.a": {"$exists": False}})
        self.cmp.compare.find(
            {"field.0.a": {"$exists": True}, "field.1.a": {"$exists": False}})

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

    def test__find_notequal_by_value(self):
        """Test searching for None."""
        bob = {'_id': 1, 'name': 'bob', 'sheepness': {'sometimes': True}}
        sam = {'_id': 2, 'name': 'sam', 'sheepness': {'sometimes': True}}
        a_goat = {'_id': 3, 'goatness': 'very', 'sheepness': {}}
        self.cmp.do.insert([bob, sam, a_goat])
        self.cmp.compare_ignore_order.find({'goatness': None})
        self.cmp.compare_ignore_order.find({'sheepness.sometimes': None})

    def test__find_not(self):
        bob = {'_id': 1, 'name': 'bob'}
        sam = {'_id': 2, 'name': 'sam'}
        self.cmp.do.insert([bob, sam])
        self.cmp.compare_ignore_order.find()
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'bob'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'sam'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$ne': 'dan'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$eq': 'bob'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$eq': 'sam'}}})
        self.cmp.compare_ignore_order.find({'name': {'$not': {'$eq': 'dan'}}})

        self.cmp.compare_ignore_order.find({'name': {'$not': re.compile('dan')}})

    def test__find_not_exceptions(self):
        self.cmp.do.insert(dict(noise="longhorn"))
        with assert_raises(OperationFailure):
            self.mongo_collection.find({'name': {'$not': True}}).count()
        with assert_raises(OperationFailure):
            self.fake_collection.find({'name': {'$not': True}}).count()

        with assert_raises(OperationFailure):
            self.mongo_collection.find({'name': {'$not': {'$regex': ''}}}).count()
        with assert_raises(OperationFailure):
            self.fake_collection.find({'name': {'$not': {'$regex': ''}}}).count()

        with assert_raises(OperationFailure):
            self.mongo_collection.find({'name': {'$not': []}}).count()
        with assert_raises(OperationFailure):
            self.fake_collection.find({'name': {'$not': []}}).count()

        with assert_raises(OperationFailure):
            self.mongo_collection.find({'name': {'$not': ''}}).count()
        with assert_raises(OperationFailure):
            self.fake_collection.find({'name': {'$not': ''}}).count()

    def test__find_compare(self):
        self.cmp.do.insert(dict(noise="longhorn"))
        for x in range(10):
            self.cmp.do.insert(dict(num=x, sqrd=x * x))
        self.cmp.compare_ignore_order.find({'sqrd': {'$lte': 4}})
        self.cmp.compare_ignore_order.find({'sqrd': {'$lt': 4}})
        self.cmp.compare_ignore_order.find({'sqrd': {'$gte': 64}})
        self.cmp.compare_ignore_order.find({'sqrd': {'$gte': 25, '$lte': 36}})

    def test__find_sets(self):
        single = 4
        even = [2, 4, 6, 8]
        prime = [2, 3, 5, 7]
        self.cmp.do.insert([
            dict(x=single),
            dict(x=even),
            dict(x=prime)])
        self.cmp.compare_ignore_order.find({'x': {'$in': [7, 8]}})
        self.cmp.compare_ignore_order.find({'x': {'$in': [4, 5]}})
        self.cmp.compare_ignore_order.find({'x': {'$nin': [2, 5]}})
        self.cmp.compare_ignore_order.find({'x': {'$all': [2, 5]}})
        self.cmp.compare_ignore_order.find({'x': {'$all': [7, 8]}})
        self.cmp.compare_ignore_order.find({'x': 2})
        self.cmp.compare_ignore_order.find({'x': 4})
        self.cmp.compare_ignore_order.find({'$or': [{'x': 4}, {'x': 2}]})
        self.cmp.compare_ignore_order.find({'$or': [{'x': 4}, {'x': 7}]})
        self.cmp.compare_ignore_order.find({'$and': [{'x': 2}, {'x': 7}]})
        self.cmp.compare_ignore_order.find({'$nor': [{'x': 3}]})
        self.cmp.compare_ignore_order.find({'$nor': [{'x': 4}, {'x': 2}]})

    def test__find_and_modify_remove(self):
        self.cmp.do.insert([{"a": x} for x in range(10)])
        self.cmp.do.find_and_modify({"a": 2}, remove=True)
        self.cmp.compare_ignore_order.find()

    def test__find_one_and_delete(self):
        self.cmp.do.insert_many([{'a': i} for i in range(10)])
        self.cmp.compare.find_one_and_delete({'a': 5}, {'_id': False})
        self.cmp.compare.find()

    def test__find_one_and_replace(self):
        self.cmp.do.insert_many([{'a': i} for i in range(10)])
        self.cmp.compare.find_one_and_replace(
            {'a': 5}, {'a': 11}, projection={'_id': False})
        self.cmp.compare.find()

    def test__find_one_and_update(self):
        self.cmp.do.insert_many([{'a': i} for i in range(10)])
        self.cmp.compare.find_one_and_update(
            {'a': 5}, {'$set': {'a': 11}}, projection={'_id': False})
        self.cmp.compare.find()

    def test__find_sort_list(self):
        self.cmp.do.remove()
        for data in ({"a": 1, "b": 3, "c": "data1"},
                     {"a": 2, "b": 2, "c": "data3"},
                     {"a": 3, "b": 1, "c": "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(sort=[("a", 1), ("b", -1)])
        self.cmp.compare.find(sort=[("b", 1), ("a", -1)])
        self.cmp.compare.find(sort=[("b", 1), ("a", -1), ("c", 1)])

    def test__find_sort_list_empty_order(self):
        self.cmp.do.remove()
        for data in ({"a": 1},
                     {"a": 2, "b": -2},
                     {"a": 3, "b": 4}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(sort=[("b", 1)])
        self.cmp.compare.find(sort=[("b", -1)])

    def test__find_sort_list_nested_doc(self):
        self.cmp.do.remove()
        for data in ({"root": {"a": 1, "b": 3, "c": "data1"}},
                     {"root": {"a": 2, "b": 2, "c": "data3"}},
                     {"root": {"a": 3, "b": 1, "c": "data2"}}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(sort=[("root.a", 1), ("root.b", -1)])
        self.cmp.compare.find(sort=[("root.b", 1), ("root.a", -1)])
        self.cmp.compare.find(
            sort=[
                ("root.b", 1), ("root.a", -1), ("root.c", 1)])

    def test__find_sort_list_nested_list(self):
        self.cmp.do.remove()
        for data in ({"root": [{"a": 1, "b": 3, "c": "data1"}]},
                     {"root": [{"a": 2, "b": 2, "c": "data3"}]},
                     {"root": [{"a": 3, "b": 1, "c": "data2"}]}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(sort=[("root.0.a", 1), ("root.0.b", -1)])
        self.cmp.compare.find(sort=[("root.0.b", 1), ("root.0.a", -1)])
        self.cmp.compare.find(
            sort=[
                ("root.0.b", 1), ("root.0.a", -1), ("root.0.c", 1)])

    def test__find_limit(self):
        self.cmp.do.remove()
        for data in ({"a": 1, "b": 3, "c": "data1"},
                     {"a": 2, "b": 2, "c": "data3"},
                     {"a": 3, "b": 1, "c": "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare.find(limit=2, sort=[("a", 1), ("b", -1)])
        # pymongo limit defaults to 0, returning everything
        self.cmp.compare.find(limit=0, sort=[("a", 1), ("b", -1)])

    def test__find_projection_subdocument_lists(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'a': 1, 'b': [{'c': 3, 'd': 4}, {'c': 5, 'd': 6}]})
        for project in ({'_id': 0, 'a': 1, 'b': 1},
                        {'_id': 0, 'a': 1, 'b.c': 1},
                        {'_id': 0, 'a': 0, 'b.c': 0},
                        {'_id': 0, 'a': 1, 'b.c.e': 1},
                        {'_id': 0, 'a': 0, 'b.c': 0, 'b.c.e': 0}):
            self.cmp.compare.find_one({'a': 1}, project)

    # def test__as_class(self):
    #     class MyDict(dict):
    #         pass
    #
    #     self.cmp.do.remove()
    #     self.cmp.do.insert(
    #         {"a": 1, "b": {"ba": 3, "bb": 4, "bc": [{"bca": 5}]}})
    #     self.cmp.compare.find({}, as_class=MyDict)
    #     self.cmp.compare.find({"a": 1}, as_class=MyDict)

    def test__return_only_selected_fields(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll', 'model': 'v6'})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection=['type'])

    def test__return_only_selected_fields_no_id(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll', 'model': 'v6'})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'type': 1, '_id': 0})

    def test__return_only_selected_fields_nested_field_found(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection=['properties.type'])

    def test__return_only_selected_fields_nested_field_not_found(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection=['properties.color'])

    def test__return_only_selected_fields_nested_field_found_no_id(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'properties.type': 1, '_id': 0})

    def test__return_only_selected_fields_nested_field_not_found_no_id(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'properties.color': 1, '_id': 0})

    def test__exclude_selected_fields(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll', 'model': 'v6'})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'type': 0})

    def test__exclude_selected_fields_including_id(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll', 'model': 'v6'})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'type': 0, '_id': 0})

    def test__exclude_all_fields_including_id(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll'})
        self.cmp.compare.find(
            {'name': 'Chucky'}, projection={'type': 0, '_id': 0, 'name': 0})

    def test__exclude_selected_nested_fields(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'properties.type': 0})

    def test__exclude_all_selected_nested_fields(self):
        self.cmp.do.insert(
            {'name': 'Chucky', 'properties': {'type': 'doll', 'model': 'v6'}})
        self.cmp.compare_ignore_order.find(
            {'name': 'Chucky'}, projection={'properties.type': 0, 'properties.model': 0})

    def test__default_fields_to_id_if_empty(self):
        self.cmp.do.insert({'name': 'Chucky', 'type': 'doll', 'model': 'v6'})
        self.cmp.compare_ignore_order.find({'name': 'Chucky'}, projection=[])

    def test__remove(self):
        """Test the remove method."""
        self.cmp.do.insert({"value": 1})
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
        self.cmp.compare.find()

    def test__delete_one(self):
        self.cmp.do.insert_many([{'a': i} for i in range(10)])
        self.cmp.compare.find()

        self.cmp.do.delete_one({'a': 5})
        self.cmp.compare.find()

    def test__delete_many(self):
        self.cmp.do.insert_many([{'a': i} for i in range(10)])
        self.cmp.compare.find()

        self.cmp.do.delete_many({'a': {'$gt': 5}})
        self.cmp.compare.find()

    def test__update(self):
        doc = {"a": 1}
        self.cmp.do.insert(doc)
        new_document = {"new_attr": 2}
        self.cmp.do.update({"a": 1}, new_document)
        self.cmp.compare_ignore_order.find()

    def test__update_upsert_with_id(self):
        self.cmp.do.update(
            {'a': 1}, {'_id': ObjectId('52d669dcad547f059424f783'), 'a': 1}, upsert=True)
        self.cmp.compare.find()

    def test__update_upsert_with_dots(self):
        self.cmp.do.update(
            {'a.b': 1}, {'$set': {'c': 2}}, upsert=True)
        self.cmp.compare.find()

    def test__update_with_empty_document_comes(self):
        """Tests calling update with just '{}' for replacing whole document"""
        self.cmp.do.insert({'name': 'bob', 'hat': 'wide'})
        self.cmp.do.update({'name': 'bob'}, {})
        self.cmp.compare.find()

    def test__update_one(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 0},
                                 {'a': 2, 'b': 0}])
        self.cmp.compare.find()

        self.cmp.do.update_one({'a': 2}, {'$set': {'b': 1}})
        self.cmp.compare.find()

        self.cmp.do.update_one({'a': 3}, {'$set': {'a': 3, 'b': 0}})
        self.cmp.compare.find()

        self.cmp.do.update_one({'a': 3}, {'$set': {'a': 3, 'b': 0}},
                               upsert=True)
        self.cmp.compare.find()

    def test__update_many(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 0},
                                 {'a': 2, 'b': 0}])
        self.cmp.compare.find()

        self.cmp.do.update_many({'b': 1}, {'$set': {'b': 1}})
        self.cmp.compare.find()

        self.cmp.do.update_many({'b': 0}, {'$set': {'b': 1}})
        self.cmp.compare.find()

    def test__replace_one(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 0},
                                 {'a': 2, 'b': 0}])
        self.cmp.compare.find()

        self.cmp.do.replace_one({'a': 2}, {'a': 3, 'b': 0})
        self.cmp.compare.find()

        self.cmp.do.replace_one({'a': 4}, {'a': 4, 'b': 0})
        self.cmp.compare.find()

        self.cmp.do.replace_one({'a': 4}, {'a': 4, 'b': 0}, upsert=True)
        self.cmp.compare.find()

    def test__set(self):
        """Tests calling update with $set members."""
        self.cmp.do.update({'_id': 42},
                           {'$set': {'some': 'thing'}},
                           upsert=True)
        self.cmp.compare.find({'_id': 42})
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'hat': 'green'}})
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'hat': 'red'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__unset(self):
        """Tests calling update with $unset members."""
        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': 0}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': 1}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': ""}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': True}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.update({'name': 'bob'}, {'a': 'aaa'}, upsert=True)
        self.cmp.compare.find({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$unset': {'a': False}})
        self.cmp.compare.find({'name': 'bob'})

    def test__unset_nested(self):
        self.cmp.do.update({'_id': 1}, {'$set': {'a': {'b': 1, 'c': 2}}}, upsert=True)
        self.cmp.do.update({'_id': 1}, {'$unset': {'a.b': True}})
        self.cmp.compare.find()

        self.cmp.do.update({'_id': 1}, {'$set': {'a': {'b': 1, 'c': 2}}}, upsert=True)
        self.cmp.do.update({'_id': 1}, {'$unset': {'a.b': False}})
        self.cmp.compare.find()

        self.cmp.do.update({'_id': 1}, {'$set': {'a': {'b': 1}}}, upsert=True)
        self.cmp.do.update({'_id': 1}, {'$unset': {'a.b': True}})
        self.cmp.compare.find()

        self.cmp.do.update({'_id': 1}, {'$set': {'a': {'b': 1}}}, upsert=True)
        self.cmp.do.update({'_id': 1}, {'$unset': {'a.b': False}})
        self.cmp.compare.find()

    def test__unset_positional(self):
        self.cmp.do.insert({'a': 1, 'b': [{'c': 2, 'd': 3}]})
        self.cmp.do.update(
            {'a': 1, 'b': {'$elemMatch': {'c': 2, 'd': 3}}},
            {'$unset': {'b.$.c': ''}}
        )
        self.cmp.compare.find()

    def test__set_upsert(self):
        self.cmp.do.remove()
        self.cmp.do.update({"name": "bob"}, {"$set": {"age": 1}}, True)
        self.cmp.compare.find()
        self.cmp.do.update({"name": "alice"}, {"$set": {"age": 1}}, True)
        self.cmp.compare_ignore_order.find()

    def test__set_subdocuments(self):
        """Tests using $set for setting subdocument fields"""
        self.skipTest(
            "MongoClient does not allow setting subdocuments on existing non-documents")
        self.cmp.do.insert(
            {'name': 'bob', 'data1': 1, 'subdocument': {'a': {'b': {'c': 20}}}})
        self.cmp.do.update({'name': 'bob'}, {'$set': {'data1.field1': 11}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$set': {'data2.field1': 21}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$set': {'subdocument.a.b': 21}})
        self.cmp.compare.find()

    def test__set_subdocuments_positional(self):
        self.cmp.do.insert({'name': 'bob', 'subdocs': [
            {'id': 1, 'name': 'foo'},
            {'id': 2, 'name': 'bar'}
        ]})
        self.cmp.do.update({'name': 'bob', 'subdocs.id': 2},
                           {'$set': {'subdocs.$': {'id': 3, 'name': 'baz'}}})
        self.cmp.compare.find()

    def test__inc(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$inc': {'count': 1}})
            self.cmp.compare.find({'name': 'bob'})

    def test__max(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$max': {'count': i}})
            self.cmp.compare.find({'name': 'bob'})

    def test__min(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$min': {'count': i}})
            self.cmp.compare.find({'name': 'bob'})

    def test__inc_upsert(self):
        self.cmp.do.remove()
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$inc': {'count': 1}}, True)
            self.cmp.compare.find({'name': 'bob'})

    def test__inc_subdocument(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': {'age': 0}})
        self.cmp.do.update({'name': 'bob'}, {'$inc': {'data.age': 1}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$inc': {'data.age2': 1}})
        self.cmp.compare.find()

    def test__inc_subdocument_positional(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': [{'age': 0}, {'age': 1}]})
        self.cmp.do.update({'name': 'bob', 'data': {'$elemMatch': {'age': 0}}},
                           {'$inc': {'data.$.age': 1}})
        self.cmp.compare.find()

    def test__setOnInsert(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$setOnInsert': {'age': 1}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'ann'}, {'$setOnInsert': {'age': 1}})
        self.cmp.compare.find()

    def test__setOnInsert_upsert(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$setOnInsert': {'age': 1}}, True)
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'ann'}, {'$setOnInsert': {'age': 1}}, True)
        self.cmp.compare.find()

    def test__setOnInsert_subdocument(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': {'age': 0}})
        self.cmp.do.update({'name': 'bob'}, {'$setOnInsert': {'data.age': 1}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'bob'}, {'$setOnInsert': {'data.age1': 1}})
        self.cmp.compare.find()
        self.cmp.do.update({'name': 'ann'}, {'$setOnInsert': {'data.age': 1}})
        self.cmp.compare.find()

    def test__setOnInsert_subdocument_upsert(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': {'age': 0}})
        self.cmp.do.update(
            {'name': 'bob'}, {'$setOnInsert': {'data.age': 1}}, True)
        self.cmp.compare.find()
        self.cmp.do.update(
            {'name': 'bob'}, {'$setOnInsert': {'data.age1': 1}}, True)
        self.cmp.compare.find()
        self.cmp.do.update(
            {'name': 'ann'}, {'$setOnInsert': {'data.age': 1}}, True)
        self.cmp.compare.find()

    def test__setOnInsert_subdocument_elemMatch(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': [{'age': 0}, {'age': 1}]})
        self.cmp.do.update({'name': 'bob', 'data': {'$elemMatch': {'age': 0}}},
                           {'$setOnInsert': {'data.$.age': 1}})
        self.cmp.compare.find()

    def test__inc_subdocument_positional_upsert(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'data': [{'age': 0}, {'age': 1}]})
        self.cmp.do.update({'name': 'bob', 'data': {'$elemMatch': {'age': 0}}},
                           {'$setOnInsert': {'data.$.age': 1}}, True)
        self.cmp.compare.find()

    def test__addToSet(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$addToSet': {'hat': 'green'}})
            self.cmp.compare.find({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name': 'bob'}, {'$addToSet': {'hat': 'tall'}})
            self.cmp.compare.find({'name': 'bob'})

    def test__addToSet_nested(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update(
                {'name': 'bob'}, {'$addToSet': {'hat.color': 'green'}})
            self.cmp.compare.find({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update(
                {'name': 'bob'}, {'$addToSet': {'hat.color': 'tall'}})
            self.cmp.compare.find({'name': 'bob'})

    def test__addToSet_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update(
                {'name': 'bob'},
                {'$addToSet': {'hat': {'$each': ['green', 'yellow']}}})
            self.cmp.compare.find({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update(
                {'name': 'bob'},
                {'$addToSet': {'shirt.color': {'$each': ['green', 'yellow']}}})
            self.cmp.compare.find({'name': 'bob'})

    def test__pull(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$pull': {'hat': 'green'}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update({'name': 'bob'}, {'$pull': {'hat': 'green'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pull_query(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': [{'size': 5}, {'size': 10}]})
        self.cmp.do.update(
            {'name': 'bob'}, {'$pull': {'hat': {'size': {'$gt': 6}}}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': {'sizes': [{'size': 5}, {'size': 10}]}})
        self.cmp.do.update(
            {'name': 'bob'}, {'$pull': {'hat.sizes': {'size': {'$gt': 6}}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pull_nested_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert({
            'name': 'bob',
            'hat': [
                {'name': 'derby',
                 'sizes': [{'size': 'L', 'quantity': 3},
                           {'size': 'XL', 'quantity': 4}],
                 'colors': ['green', 'blue']},
                {'name': 'cap',
                 'sizes': [{'size': 'S', 'quantity': 10},
                           {'size': 'L', 'quantity': 5}],
                 'colors': ['blue']}]})
        self.cmp.do.update(
            {'hat': {'$elemMatch': {'name': 'derby'}}},
            {'$pull': {'hat.$.sizes': {'size': 'L'}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pull_nested_list(self):
        self.cmp.do.remove()
        self.cmp.do.insert(
            {'name': 'bob', 'hat':
             [{'name': 'derby', 'sizes': ['L', 'XL']},
              {'name': 'cap', 'sizes': ['S', 'L']}]})
        self.cmp.do.update(
            {'hat': {'$elemMatch': {'name': 'derby'}}},
            {'$pull': {'hat.$.sizes': 'XL'}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': {'nested': ['element1', 'element2', 'element1']}})
        self.cmp.do.update({'name': 'bob'}, {'$pull': {'hat.nested': 'element1'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pullAll(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$pullAll': {'hat': ['green']}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update(
            {'name': 'bob'}, {'$pullAll': {'hat': ['green', 'blue']}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall', 'blue']})
        self.cmp.do.update({'name': 'bob'}, {'$pullAll': {'hat': ['green']}})
        self.cmp.compare.find({'name': 'bob'})

    def test__pullAll_nested_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert(
            {'name': 'bob', 'hat': {'properties': {'sizes': ['M', 'L', 'XL']}}})
        self.cmp.do.update(
            {'name': 'bob'}, {'$pullAll': {'hat.properties.sizes': ['M']}})
        self.cmp.compare.find({'name': 'bob'})

        self.cmp.do.remove()
        self.cmp.do.insert(
            {'name': 'bob', 'hat': {'properties': {'sizes': ['M', 'L', 'XL']}}})
        self.cmp.do.update(
            {'name': 'bob'},
            {'$pullAll': {'hat.properties.sizes': ['M', 'L']}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat': 'wide'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert(
            {'name': 'bob', 'hat': [{'name': 'derby', 'sizes': ['L', 'XL']}]})
        self.cmp.do.update(
            {'name': 'bob'},
            {'$push': {'hat': {'name': 'cap', 'sizes': ['S', 'L']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': ['green', 'tall']})
        self.cmp.do.update(
            {'name': 'bob'}, {'$push': {'hat': {'$each': ['wide', 'blue']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_dict(self):
        self.cmp.do.remove()
        self.cmp.do.insert({
            'name': 'bob',
            'hat': [
                {'name': 'derby',
                 'sizes': [{'size': 'L', 'quantity': 3},
                           {'size': 'XL', 'quantity': 4}],
                 'colors': ['green', 'blue']},
                {'name': 'cap',
                 'sizes': [{'size': 'S', 'quantity': 10},
                           {'size': 'L', 'quantity': 5}],
                 'colors': ['blue']}]})
        self.cmp.do.update(
            {'hat': {'$elemMatch': {'name': 'derby'}}},
            {'$push': {'hat.$.sizes': {'size': 'M', 'quantity': 6}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_dict_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({
            'name': 'bob',
            'hat': [
                {'name': 'derby',
                 'sizes': [{'size': 'L', 'quantity': 3},
                           {'size': 'XL', 'quantity': 4}],
                 'colors': ['green', 'blue']},
                {'name': 'cap',
                 'sizes': [{'size': 'S', 'quantity': 10},
                           {'size': 'L', 'quantity': 5}],
                 'colors': ['blue']}]})
        self.cmp.do.update(
            {'hat': {'$elemMatch': {'name': 'derby'}}},
            {'$push':
             {'hat.$.sizes':
              {'$each':
               [{'size': 'M', 'quantity': 6}, {'size': 'S', 'quantity': 1}]}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_list_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({
            'name': 'bob',
            'hat': [
                {'name': 'derby',
                 'sizes': ['L', 'XL'],
                 'colors': ['green', 'blue']},
                {'name': 'cap', 'sizes': ['S', 'L'],
                 'colors': ['blue']}
            ]
        })
        self.cmp.do.update(
            {'hat': {'$elemMatch': {'name': 'derby'}}},
            {'$push': {'hat.$.sizes': {'$each': ['M', 'S']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_attribute(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': {'data': {'sizes': ['XL']}}})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat.data.sizes': 'L'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_nested_attribute_each(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat': {}})
        self.cmp.do.update(
            {'name': 'bob'}, {'$push': {'hat.first': {'$each': ['a', 'b']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_to_absent_nested_attribute(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat.data.sizes': 'L'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_to_absent_field(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update({'name': 'bob'}, {'$push': {'hat': 'wide'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__push_each_to_absent_field(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        self.cmp.do.update(
            {'name': 'bob'}, {'$push': {'hat': {'$each': ['wide', 'blue']}}})
        self.cmp.compare.find({'name': 'bob'})

    def test__drop(self):
        self.cmp.do.insert({"name": "another new"})
        self.cmp.do.drop()
        self.cmp.compare.find({})

    def test__ensure_index(self):
        # Does nothing - just make sure it exists and takes the right args
        self.cmp.do.ensure_index("name")
        self.cmp.do.ensure_index("hat", cache_for=100)
        self.cmp.do.ensure_index([("name", 1), ("hat", -1)])

    def test__drop_index(self):
        # Does nothing - just make sure it exists and takes the right args
        self.cmp.do.drop_index("name")

    def test__index_information(self):
        # Does nothing - just make sure it exists
        self.cmp.do.index_information()

    def test__empty_logical_operators(self):
        for operator in mongomock.filtering.LOGICAL_OPERATOR_MAP:
            try:
                self.cmp.compare_ignore_order.find({operator: []})
            except Exception as e:
                assert isinstance(e, mongomock.OperationFailure)


@skipIf(not _HAVE_PYMONGO, "pymongo not installed")
@skipIf(not _HAVE_MAP_REDUCE, "execjs not installed")
class CollectionMapReduceTest(TestCase):

    def setUp(self):
        self.db = mongomock.MongoClient().map_reduce_test
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
        self._check_map_reduce(self.db.things, self.expected_results)

    def test__map_reduce_clean_res_colc(self):
        # Checks that the result collection is cleaned between calls
        self._check_map_reduce(self.db.things, self.expected_results)

        more_data = [{"x": 1, "tags": []},
                     {"x": 2, "tags": []},
                     {"x": 3, "tags": []},
                     {"x": 4, "tags": []}]
        for item in more_data:
            self.db.more_things.insert(item)
        expected_results = []

        self._check_map_reduce(self.db.more_things, expected_results)

    def _check_map_reduce(self, colc, expected_results):
        result = colc.map_reduce(self.map_func, self.reduce_func, 'myresults')
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), len(expected_results))
        for doc in result.find():
            self.assertIn(doc, expected_results)

    def test__map_reduce_son(self):
        result = self.db.things.map_reduce(
            self.map_func, self.reduce_func,
            out=SON([('replace', 'results'), ('db', 'map_reduce_son_test')]))
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'results')
        self.assertEqual(result.database.name, 'map_reduce_son_test')
        self.assertEqual(result.count(), 3)
        for doc in result.find():
            self.assertIn(doc, self.expected_results)

    def test__map_reduce_full_response(self):
        expected_full_response = {
            'counts': {
                'input': 4,
                'reduce': 2,
                'emit': 6,
                'output': 3
            },
            'timeMillis': 5,
            'ok': 1.0,
            'result': 'myresults'
        }
        result = self.db.things.map_reduce(
            self.map_func, self.reduce_func,
            'myresults', full_response=True)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result['counts'], expected_full_response['counts'])
        self.assertEqual(result['result'], expected_full_response['result'])
        for doc in getattr(self.db, result['result']).find():
            self.assertIn(doc, self.expected_results)

    def test__map_reduce_with_query(self):
        expected_results = [{'_id': 'mouse', 'value': 1},
                            {'_id': 'dog', 'value': 2},
                            {'_id': 'cat', 'value': 2}]
        result = self.db.things.map_reduce(
            self.map_func, self.reduce_func,
            'myresults', query={'tags': 'dog'})
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 3)
        for doc in result.find():
            self.assertIn(doc, expected_results)

    def test__map_reduce_with_limit(self):
        result = self.db.things.map_reduce(
            self.map_func, self.reduce_func, 'myresults', limit=2)
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 2)

    def test__inline_map_reduce(self):
        result = self.db.things.inline_map_reduce(
            self.map_func, self.reduce_func)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 3)
        for doc in result:
            self.assertIn(doc, self.expected_results)

    def test__inline_map_reduce_full_response(self):
        expected_full_response = {
            'counts': {
                'input': 4,
                'reduce': 2,
                'emit': 6,
                'output': 3
            },
            'timeMillis': 5,
            'ok': 1.0,
            'result': [
                {'_id': 'cat', 'value': 3},
                {'_id': 'dog', 'value': 2},
                {'_id': 'mouse', 'value': 1}]
        }
        result = self.db.things.inline_map_reduce(
            self.map_func, self.reduce_func, full_response=True)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result['counts'], expected_full_response['counts'])
        for doc in result['result']:
            self.assertIn(doc, self.expected_results)

    def test__map_reduce_with_object_id(self):
        obj1 = ObjectId()
        obj2 = ObjectId()
        data = [{"x": 1, "tags": [obj1, obj2]},
                {"x": 2, "tags": [obj1]}]
        for item in data:
            self.db.things_with_obj.insert(item)
        expected_results = [{'_id': obj1, 'value': 2},
                            {'_id': obj2, 'value': 1}]
        result = self.db.things_with_obj.map_reduce(
            self.map_func, self.reduce_func, 'myresults')
        self.assertTrue(isinstance(result, mongomock.Collection))
        self.assertEqual(result.name, 'myresults')
        self.assertEqual(result.count(), 2)
        for doc in result.find():
            self.assertIn(doc, expected_results)

    def test_mongomock_map_reduce(self):
        # Arrange
        fake_etap = mongomock.MongoClient().db
        fake_statuses_collection = fake_etap.create_collection('statuses')
        fake_config_id = "this_is_config_id"
        test_name = "this_is_test_name"
        fake_statuses_objects = [
            {
                "testID": test_name,
                "kind": "Test",
                "duration": 8392,
                "configID": fake_config_id
            },
            {
                "testID": test_name,
                "kind": "Test",
                "duration": 8393,
                "configID": fake_config_id
            },
            {
                "testID": test_name,
                "kind": "Test",
                "duration": 8394,
                "configID": fake_config_id
            }
        ]
        fake_statuses_collection.insert_many(fake_statuses_objects)

        map_function = Code("function(){emit(this._id, this.duration);}")
        reduce_function = Code("function() {}")
        search_query = {'configID': fake_config_id, 'kind': 'Test', 'testID': test_name}

        # Act
        result = fake_etap.statuses.map_reduce(
            map_function, reduce_function, "my_collection", query=search_query)

        # Assert
        self.assertEqual(result.count(), 3)


@skipIf(not _HAVE_PYMONGO, "pymongo not installed")
@skipIf(not _HAVE_MAP_REDUCE, "execjs not installed")
class _GroupTest(_CollectionComparisonTest):

    def setUp(self):
        _CollectionComparisonTest.setUp(self)
        self._id1 = ObjectId()
        self.data = [
            {"a": 1, "count": 4},
            {"a": 1, "count": 2},
            {"a": 1, "count": 4},
            {"a": 2, "count": 3},
            {"a": 2, "count": 1},
            {"a": 1, "count": 5},
            {"a": 4, "count": 4},
            {"b": 4, "foo": 4},
            {"b": 2, "foo": 3, "name": "theone"},
            {"b": 1, "foo": 2},
            {"b": 1, "foo": self._id1},
        ]
        for item in self.data:
            self.cmp.do.insert(item)

    def test__group1(self):
        key = ["a"]
        initial = {"count": 0}
        condition = {"a": {"$lt": 3}}
        reduce_func = Code("""
                function(cur, result) { result.count += cur.count }
                """)
        self.cmp.compare.group(key, condition, initial, reduce_func)

    def test__group2(self):
        reduce_func = Code("""
                function(cur, result) { result.count += 1 }
                """)
        self.cmp.compare.group(
            key=["b"],
            condition={"foo": {"$in": [3, 4]}, "name": "theone"},
            initial={"count": 0},
            reduce=reduce_func)

    def test__group3(self):
        reducer = Code("""
            function(obj, result) {result.count+=1 }
            """)
        conditions = {'foo': {'$in': [self._id1]}}
        self.cmp.compare.group(
            key=['foo'],
            condition=conditions,
            initial={"count": 0},
            reduce=reducer)


@skipIf(not _HAVE_PYMONGO, "pymongo not installed")
@skipIf(not _HAVE_MAP_REDUCE, "execjs not installed")
class MongoClientAggregateTest(_CollectionComparisonTest):

    def setUp(self):
        super(MongoClientAggregateTest, self).setUp()
        self.data = [
            {"_id": ObjectId(), "a": 1, "b": 1, "count": 4, "swallows": ['European swallow'],
             "date": datetime.datetime(2015, 10, 1, 10, 0)},
            {"_id": ObjectId(), "a": 1, "b": 1, "count": 2, "swallows": ['African swallow'],
             "date": datetime.datetime(2015, 12, 1, 12, 0)},
            {"_id": ObjectId(), "a": 1, "b": 2, "count": 4, "swallows": ['European swallow'],
             "date": datetime.datetime(2014, 10, 2, 12, 0)},
            {"_id": ObjectId(), "a": 2, "b": 2, "count": 3, "swallows": ['African swallow',
                                                                         'European swallow'],
             "date": datetime.datetime(2015, 1, 2, 10, 0)},
            {"_id": ObjectId(), "a": 2, "b": 3, "count": 1, "swallows": [],
             "date": datetime.datetime(2013, 1, 3, 12, 0)},
            {"_id": ObjectId(), "a": 1, "b": 4, "count": 5, "swallows": ['African swallow',
                                                                         'European swallow'],
             "date": datetime.datetime(2015, 8, 4, 12, 0)},
            {"_id": ObjectId(), "a": 4, "b": 4, "count": 4, "swallows": ['unladen swallow'],
             "date": datetime.datetime(2014, 7, 4, 13, 0)}]

        for item in self.data:
            self.cmp.do.insert(item)

    def test__aggregate1(self):
        pipeline = [
            {'$match': {'a': {'$lt': 3}}},
            {'$sort': {'_id': -1}},
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate2(self):
        pipeline = [
            {'$group': {'_id': '$a', 'count': {'$sum': '$count'}}},
            {'$match': {'a': {'$lt': 3}}},
            {'$sort': {'_id': -1, 'count': 1}},
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate3(self):
        pipeline = [
            {'$group': {'_id': 'a', 'count': {'$sum': '$count'}}},
            {'$match': {'a': {'$lt': 3}}},
            {'$sort': {'_id': -1, 'count': 1}},
            {'$skip': 1},
            {'$limit': 2}]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate4(self):
        pipeline = [
            {'$unwind': '$swallows'},
            {'$sort': {'count': -1, 'swallows': -1}}]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate5(self):
        pipeline = [
            {'$group': {'_id': {'id_a': '$a'}, 'total': {'$sum': '$count'},
                        'avg': {'$avg': '$count'}}},
            {'$sort': {'_id.a': 1, 'total': 1, 'avg': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate6(self):
        pipeline = [
            {'$group': {'_id': {'id_a': '$a', 'id_b': '$b'}, 'total': {'$sum': '$count'},
                        'avg': {'$avg': '$count'}}},
            {'$sort': {'_id.id_a': 1, '_id.id_b': 1, 'total': 1, 'avg': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate7(self):
        pipeline = [
            {'$group': {'_id': {'id_a': '$a', 'id_b': {'$year': '$date'}},
                        'total': {'$sum': '$count'}, 'avg': {'$avg': '$count'}}},
            {'$sort': {'_id.id_a': 1, '_id.id_b': 1, 'total': 1, 'avg': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate8(self):
        pipeline = [
            {'$group': {'_id': None, 'counts': {'$sum': '$count'}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate9(self):
        pipeline = [
            {'$group': {'_id': {'id_a': '$a'}, 'total': {'$sum': '$count'},
                        'avg': {'$avg': '$count'}}},
            {'$group': {'_id': None, 'counts': {'$sum': '$total'}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate10(self):     # group on compound index
        self.cmp.do.remove()

        data = [
            {"_id": ObjectId(),
             "key_1": {"sub_key_1": "value_1"}, "nb": 1},
            {"_id": ObjectId(),
             "key_1": {"sub_key_1": "value_2"}, "nb": 1},
            {"_id": ObjectId(),
             "key_1": {"sub_key_1": "value_1"}, "nb": 2}
        ]
        for item in data:
            self.cmp.do.insert(item)

        pipeline = [
            {'$group': {"_id": "$key_1.sub_key_1", "nb": {"$sum": "$nb"}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate11(self):
        pipeline = [
            {'$group': {'_id': None, 'max_count': {'$max': '$count'},
                        'min_count': {'$min': '$count'}}},
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate12(self):
        pipeline = [
            {'$group': {'_id': '$a', 'max_count': {'$max': '$count'},
                        'min_count': {'$min': '$count'}}},
            {'$sort': {'_id': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate13(self):
        pipeline = [
            {'$sort': {'date': 1}},
            {'$group': {'_id': None, 'last_date': {'$last': '$date'},
                        'first_date': {'$first': '$date'}}},
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate14(self):
        pipeline = [
            {'$sort': {'date': 1}},
            {'$group': {'_id': '$a', 'last_date': {'$last': '$date'},
                        'first_date': {'$first': '$date'}}},
            {'$sort': {'_id': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate15(self):
        pipeline = [
            {'$project': {'_id': 1, 'a': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate16(self):
        pipeline = [
            {'$project': {'_id': 0, 'a': 1}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate17(self):
        pipeline = [
            {'$project': {'_id': 0, 'created': {'$subtract': [{'$min': ['$a', '$b']}, '$count']}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate18(self):
        pipeline = [
            {'$project': {'_id': 0, 'created': {'$subtract': ['$a', '$b']}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate19(self):
        pipeline = [
            {'$project': {'_id': 0, 'created': {'$subtract': ['$a', 1]}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate20(self):
        pipeline = [
            {'$project': {'_id': 0, 'abs': {'$abs': '$b'}, 'ceil': {'$ceil': 8.35},
                          'div': {'$divide': ['$a', 1]}, 'exp': {'$exp': 2},
                          'floor': {'$floor': 4.65}, 'ln': {'$ln': 100},
                          'log10': {'$log10': 1000}, 'mod': {'$mod': [46, 9]},
                          'pow': {'$pow': [4, 2]}, 'sqrt': {'$sqrt': 100}}}
        ]
        self.cmp.compare.aggregate(pipeline)

    def test__aggregate21(self):
        pipeline = [
            {'$group': {'_id': '$a', 'count': {'$sum': 1}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate22(self):
        pipeline = [
            {"$group": {"_id": {"$gte": ["$a", 2]}, "total": {"$sum": "$count"}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate23(self):
        # make sure we aggregate compound keys correctly
        pipeline = [
            {"$group": {"_id": {"id_a": "$a", "id_b": "$b"}, "total": {"$sum": "$count"}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate24(self):
        # make sure we aggregate zero rows correctly
        pipeline = [
            {"$match": {"_id": "123456"}},
            {"$group": {"_id": {"$eq": ["$a", 1]}, 'total': {"$sum": "$count"}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate25(self):
        pipeline = [
            {"$group": {"_id": {"$eq": [{'$year': '$date'}, 2015]}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)

    def test__aggregate26(self):
        pipeline = [
            {"$group": {"_id": {"$eq": [{'$year': '$date'}, 2015]}, "total": {"$sum": "$count"}}},
        ]
        self.cmp.compare_ignore_order.aggregate(pipeline)


def _LIMIT(*args):
    return lambda cursor: cursor.limit(*args)


def _SORT(*args):
    return lambda cursor: cursor.sort(*args)


def _SKIP(*args):
    return lambda cursor: cursor.skip(*args)


class MongoClientSortSkipLimitTest(_CollectionComparisonTest):

    def setUp(self):
        super(MongoClientSortSkipLimitTest, self).setUp()
        self.cmp.do.insert([{"_id": i, "index": i} for i in range(30)])

    def test__skip(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10)).find()

    def test__skipped_find(self):
        self.cmp.compare(_SORT("index", 1)).find(skip=10)

    def test__limit(self):
        self.cmp.compare(_SORT("index", 1), _LIMIT(10)).find()

    def test__skip_and_limit(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10), _LIMIT(10)).find()

    def test__sort_name(self):
        self.cmp.do.remove()
        for data in ({"a": 1, "b": 3, "c": "data1"},
                     {"a": 2, "b": 2, "c": "data3"},
                     {"a": 3, "b": 1, "c": "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT("a")).find()
        self.cmp.compare(_SORT("b")).find()

    def test__sort_name_nested_doc(self):
        self.cmp.do.remove()
        for data in ({"root": {"a": 1, "b": 3, "c": "data1"}},
                     {"root": {"a": 2, "b": 2, "c": "data3"}},
                     {"root": {"a": 3, "b": 1, "c": "data2"}}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT("root.a")).find()
        self.cmp.compare(_SORT("root.b")).find()

    def test__sort_name_nested_list(self):
        self.cmp.do.remove()
        for data in ({"root": [{"a": 1, "b": 3, "c": "data1"}]},
                     {"root": [{"a": 2, "b": 2, "c": "data3"}]},
                     {"root": [{"a": 3, "b": 1, "c": "data2"}]}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT("root.0.a")).find()
        self.cmp.compare(_SORT("root.0.b")).find()

    def test__sort_list(self):
        self.cmp.do.remove()
        for data in ({"a": 1, "b": 3, "c": "data1"},
                     {"a": 2, "b": 2, "c": "data3"},
                     {"a": 3, "b": 1, "c": "data2"}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT([("a", 1), ("b", -1)])).find()
        self.cmp.compare(_SORT([("b", 1), ("a", -1)])).find()
        self.cmp.compare(_SORT([("b", 1), ("a", -1), ("c", 1)])).find()

    def test__sort_list_nested_doc(self):
        self.cmp.do.remove()
        for data in ({"root": {"a": 1, "b": 3, "c": "data1"}},
                     {"root": {"a": 2, "b": 2, "c": "data3"}},
                     {"root": {"a": 3, "b": 1, "c": "data2"}}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT([("root.a", 1), ("root.b", -1)])).find()
        self.cmp.compare(_SORT([("root.b", 1), ("root.a", -1)])).find()
        self.cmp.compare(
            _SORT([("root.b", 1), ("root.a", -1), ("root.c", 1)])).find()

    def test__sort_list_nested_list(self):
        self.cmp.do.remove()
        for data in ({"root": [{"a": 1, "b": 3, "c": "data1"}]},
                     {"root": [{"a": 2, "b": 2, "c": "data3"}]},
                     {"root": [{"a": 3, "b": 1, "c": "data2"}]}):
            self.cmp.do.insert(data)
        self.cmp.compare(_SORT([("root.0.a", 1), ("root.0.b", -1)])).find()
        self.cmp.compare(_SORT([("root.0.b", 1), ("root.0.a", -1)])).find()
        self.cmp.compare(
            _SORT(
                [("root.0.b", 1), ("root.0.a", -1),
                 ("root.0.c", 1)])).find()

    def test__close(self):
        # Does nothing - just make sure it exists and takes the right args
        self.cmp.do(lambda cursor: cursor.close()).find()


class InsertedDocumentTest(TestCase):

    def setUp(self):
        super(InsertedDocumentTest, self).setUp()
        self.collection = mongomock.MongoClient().db.collection
        self.data = {"a": 1, "b": [1, 2, 3], "c": {"d": 4}}
        self.orig_data = copy.deepcopy(self.data)
        self.object_id = self.collection.insert(self.data)

    def test__object_is_consistent(self):
        [object] = self.collection.find()
        self.assertEqual(object["_id"], self.object_id)

    def test__find_by_id(self):
        [object] = self.collection.find({"_id": self.object_id})
        self.assertEqual(object, self.data)

    def test__remove_by_id(self):
        self.collection.remove(self.object_id)
        self.assertEqual(0, self.collection.count())

    def test__inserting_changes_argument(self):
        # Like pymongo, we should fill the _id in the inserted dict
        # (odd behavior, but we need to stick to it)
        self.assertEqual(self.data, dict(self.orig_data, _id=self.object_id))

    def test__data_is_copied(self):
        [object] = self.collection.find()
        self.assertEqual(dict(self.orig_data, _id=self.object_id), object)
        self.data.pop("a")
        self.data["b"].append(5)
        self.assertEqual(dict(self.orig_data, _id=self.object_id), object)
        [object] = self.collection.find()
        self.assertEqual(dict(self.orig_data, _id=self.object_id), object)

    def test__find_returns_copied_object(self):
        [object1] = self.collection.find()
        [object2] = self.collection.find()
        self.assertEqual(object1, object2)
        self.assertIsNot(object1, object2)
        object1["b"].append("bla")
        self.assertNotEqual(object1, object2)


class ObjectIdTest(TestCase):

    def test__equal_with_same_id(self):
        obj1 = ObjectId()
        obj2 = ObjectId(str(obj1))
        self.assertEqual(obj1, obj2)


class DatabasesNamesTest(TestCase):

    def setUp(self):
        super(DatabasesNamesTest, self).setUp()
        self.client = mongomock.MongoClient()

    def test__database_names(self):
        self.client.unit.tests.insert({'foo': 'bar'})
        self.client.foo.bar.insert({'unit': 'test'})
        names = self.client.database_names()
        self.assertIsInstance(names, list)
        self.assertEqual(sorted(['foo', 'unit']), sorted(names))
