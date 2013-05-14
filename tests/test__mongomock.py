import copy
import itertools
import re
import platform
if platform.python_version() < '2.7':
    import unittest2 as unittest
else:
    import unittest
from mongomock import Database, ObjectId, Collection
from mongomock import Connection as MongoMockConnection
from pymongo import Connection as PymongoConnection
from .multicollection import MultiCollection

class TestCase(unittest.TestCase):
    pass

class InterfaceTest(TestCase):
    def test__can_create_db_without_path(self):
        conn = MongoMockConnection()
        self.assertIsNotNone(conn)
    def test__can_create_db_without_path(self):
        conn = MongoMockConnection('mongodb://localhost')
        self.assertIsNotNone(conn)

class DatabaseGettingTest(TestCase):
    def setUp(self):
        super(DatabaseGettingTest, self).setUp()
        self.conn = MongoMockConnection()
    def test__getting_database_via_getattr(self):
        db1 = self.conn.some_database_here
        db2 = self.conn.some_database_here
        self.assertIs(db1, db2)
        self.assertIs(db1, self.conn['some_database_here'])
        self.assertIsInstance(db1, Database)
    def test__getting_database_via_getitem(self):
        db1 = self.conn['some_database_here']
        db2 = self.conn['some_database_here']
        self.assertIs(db1, db2)
        self.assertIs(db1, self.conn.some_database_here)
        self.assertIsInstance(db1, Database)

class CollectionAPITest(TestCase):
    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.conn = MongoMockConnection()
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
        self.assertIsInstance(col1, Collection)
    def test__getting_collection_via_getitem(self):
        col1 = self.db['some_collection_here']
        col2 = self.db['some_collection_here']
        self.assertIs(col1, col2)
        self.assertIs(col1, self.db.some_collection_here)
        self.assertIsInstance(col1, Collection)
    def test__find_returns_cursors(self):
        collection = self.db.collection
        self.assertEquals(type(collection.find()).__name__, "Cursor")
        self.assertNotIsInstance(collection.find(), list)
        self.assertNotIsInstance(collection.find(), tuple)


class CollectionComparisonTest(TestCase):
    """Compares a fake collection with the real mongo collection implementation via cross-comparison"""
    def setUp(self):
        super(CollectionComparisonTest, self).setUp()
        self.fake_conn = MongoMockConnection()
        self.mongo_conn = PymongoConnection()
        self.db_name = "mongomock___testing_db"
        self.collection_name = "mongomock___testing_collection"
        self.mongo_conn[self.db_name][self.collection_name].remove()
        self.cmp = MultiCollection({
            "fake" : self.fake_conn[self.db_name][self.collection_name],
            "real" : self.mongo_conn[self.db_name][self.collection_name],
         })

class CollectionTest(CollectionComparisonTest):
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
    def test__count(self):
        self.cmp.compare.count()
        self.cmp.do.insert({"a" : 1})
        self.cmp.compare.count()
    def test__find_by_attributes(self):
        id1 = self.cmp.do.insert({"name" : "new"})
        self.cmp.do.insert({"name" : "another new"})
        self.cmp.compare_ignore_order.find()
        self.cmp.compare.find({"_id" : id1})
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

    def test__inc(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$inc': {'count':1}})
            self.cmp.compare.find({'name': 'bob'})

    def test__addToSet(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$addToSet': {'hat':'green'}})
            self.cmp.compare.find({'name': 'bob'})
        for i in range(3):
            self.cmp.do.update({'name':'bob'}, {'$addToSet': {'hat':'tall'}})
            self.cmp.compare.find({'name': 'bob'})

    def test__pull(self):
        self.cmp.do.remove()
        self.cmp.do.insert({'name': 'bob', 'hat':['green', 'tall']})
        self.cmp.do.update({'name':'bob'}, {'$pull': {'hat':'green'}})
        self.cmp.compare.find({'name': 'bob'})

    def test__drop(self):
        data = {'a': 1}
        self.cmp.do.insert({"name" : "another new"})
        self.cmp.do.drop()
        self.cmp.compare.find({})
        


def _LIMIT(*args):
    return lambda cursor: cursor.limit(*args)

def _SORT(*args):
    return lambda cursor: cursor.sort(*args)

def _SKIP(*args):
    return lambda cursor: cursor.skip(*args)

def _NEXT(*args):
    return lambda cursor: cursor.next(*args)

class SortSkipLimitNextTest(CollectionComparisonTest):
    def setUp(self):
        super(SortSkipLimitNextTest, self).setUp()
        self.cmp.do.insert([{"_id":i, "index" : i} for i in range(30)])
    def test__sort_next(self):
        self.cmp.compare(_SORT("index", 1), _NEXT()).find()
    def test__skip(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10)).find()
    def test__limit(self):
        self.cmp.compare(_SORT("index", 1), _LIMIT(10)).find()
    def test__skip_and_limit(self):
        self.cmp.compare(_SORT("index", 1), _SKIP(10), _LIMIT(10)).find()
    

class InsertedDocumentTest(TestCase):
    def setUp(self):
        super(InsertedDocumentTest, self).setUp()
        self.collection = MongoMockConnection().db.collection
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
