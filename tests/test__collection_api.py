import mongomock
from .utils import TestCase

class CollectionAPITest(TestCase):
    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.conn = mongomock.Connection()
        self.db = self.conn['somedb']

    def test__get_subcollections(self):
        self.db.a.b
        self.assertEquals(self.db.a.b.full_name, "somedb.a.b")
        self.assertEquals(self.db.a.b.name, "a.b")

        self.assertEquals(
            set(self.db.collection_names()),
            set(["a.b", "system.indexes", "a"]))

    def test__get_collection_full_name(self):
        self.assertEquals(self.db.coll.name, "coll")
        self.assertEquals(self.db.coll.full_name, "somedb.coll")

    def test__get_collection_names(self):
        self.db.a
        self.db.b
        self.assertEquals(set(self.db.collection_names()), set(['a', 'b', 'system.indexes']))

    def test__cursor_collection(self):
        self.assertIs(self.db.a.find().collection, self.db.a)

    def test__drop_collection(self):
        self.db.a
        self.db.b
        self.db.c
        self.db.drop_collection('b')
        self.db.drop_collection('b')
        self.db.drop_collection(self.db.c)
        self.assertEquals(set(self.db.collection_names()), set(['a', 'system.indexes']))

    def test__distinct_nested_field(self):
        self.db.collection.insert({'f1': {'f2': 'v'}})
        cursor = self.db.collection.find()
        self.assertEquals(cursor.distinct('f1.f2'), ['v'])

    def test__cursor_clone(self):
        self.db.collection.insert([{"a": "b"}, {"b": "c"}, {"c": "d"}])
        cursor1 = self.db.collection.find()
        iterator1 = iter(cursor1)
        first_item = next(iterator1)
        cursor2 = cursor1.clone()
        iterator2 = iter(cursor2)
        self.assertEquals(next(iterator2), first_item)
        for item in iterator1:
            self.assertEquals(item, next(iterator2))

        with self.assertRaises(StopIteration):
            next(iterator2)

    def test__update_retval(self):
        self.db.col.save({"a": 1})
        retval = self.db.col.update({"a": 1}, {"b": 2})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[u"connectionId"], int)
        self.assertIsNone(retval[u"err"])
        self.assertEquals(retval[u"n"], 1)
        self.assertTrue(retval[u"updatedExisting"])
        self.assertEquals(retval["ok"], 1.0)

        self.assertEquals(self.db.col.update({"bla": 1}, {"bla": 2})["n"], 0)

    def test__remove_retval(self):
        self.db.col.save({"a": 1})
        retval = self.db.col.remove({"a": 1})
        self.assertIsInstance(retval, dict)
        self.assertIsInstance(retval[u"connectionId"], int)
        self.assertIsNone(retval[u"err"])
        self.assertEquals(retval[u"n"], 1)
        self.assertEquals(retval[u"ok"], 1.0)

        self.assertEquals(self.db.col.remove({"bla": 1})["n"], 0)

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

    def test__cursor_distinct(self):
        larry_bob = {'name':'larry'}
        larry = {'name':'larry'}
        gary = {'name':'gary'}
        self.db['coll_name'].insert([larry_bob, larry, gary])
        ret_val = self.db['coll_name'].find().distinct('name')
        self.assertTrue(isinstance(ret_val,list))
        self.assertTrue(set(ret_val) == set(['larry','gary']))
