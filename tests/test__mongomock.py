import copy
import re
from unittest import TestCase
from mongomock import Connection, Database, Collection, ObjectId

class FakePymongoConnectionTest(TestCase):
    def setUp(self):
        super(FakePymongoConnectionTest, self).setUp()
        self.conn = Connection()

class DatabaseGettingTest(FakePymongoConnectionTest):
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

class FakePymongoDatabaseTest(FakePymongoConnectionTest):
    def setUp(self):
        super(FakePymongoDatabaseTest, self).setUp()
        self.db = self.conn['somedb']

class FakePymongoDatabaseAPITest(FakePymongoDatabaseTest):
    def test__get_collection_names(self):
        self.db.a
        self.db.b
        self.assertItemsEqual(self.db.collection_names(), ['a', 'b', 'system.indexes'])

class CollectionGettingTest(FakePymongoDatabaseTest):
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

class CollectionTest(FakePymongoDatabaseTest):
    def setUp(self):
        super(CollectionTest, self).setUp()
        self.collection = self.db.collection
    def test__inserting(self):
        data = dict(a=1, b=2, c="data")
        object_id = self.collection.insert(data)
        self.assertIsInstance(object_id, ObjectId)
    def test__inserted_document(self):
        data = dict(a=1, b=2)
        data_before_insertion = data.copy()
        object_id = self.collection.insert(data)
        retrieved = self.collection.find_one(dict(_id=object_id))
        self.assertEquals(retrieved, dict(data, _id=object_id))
        self.assertIsNot(data, retrieved)
        self.assertEquals(data, data_before_insertion)
    def test__bulk_insert(self):
        objects = [dict(a=2), dict(a=3, b=5), dict(name="bla")]
        original_objects = copy.deepcopy(objects)
        ids = self.collection.insert(objects)
        expected_objects = [dict(obj, _id=id) for id, obj in zip(ids, original_objects)]
        self.assertEquals(sorted(self.collection.find()), sorted(expected_objects))
        # make sure objects were not changed in-place
        self.assertEquals(objects, original_objects)

class DocumentTest(FakePymongoDatabaseTest):
    def setUp(self):
        super(DocumentTest, self).setUp()
        self.collection = self.db.collection
        data = dict(a=1, b=2, c="blap")
        self.document_id = self.collection.insert(dict(a=1, b=2, c="blap"))
        self.document = dict(data, _id=self.document_id)

class FindTest(DocumentTest):
    def test__find_returns_cursors(self):
        self.assertNotIsInstance(self.collection.find(), list)
        self.assertNotIsInstance(self.collection.find(), tuple)
    def test__find_single_document(self):
        self.assertEquals(
            [self.document],
            list(self.collection.find()),
            )
    def test__find_returns_new_object(self):
        list(self.collection.find())[0]['new_field'] = 20
        self.test__find_single_document()
    def test__find_by_attributes(self):
        self.collection.insert(dict(name="new"))
        self.collection.insert(dict(name="another new"))
        self.assertEquals(len(list(self.collection.find())), 3)
        self.assertEquals(
            list(self.collection.find(dict(_id=self.document['_id']))),
            [self.document]
            )
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

        self.collection.insert(green_bowler)
        self.collection.insert(red_bowler)
        self.assertEquals(len(list(self.collection.find())), 3)

        docs = list(self.collection.find({'name': 'sam'}))
        assert len(docs) == 1
        assert docs[0]['name'] == 'sam'

        docs = list(self.collection.find({'hat.color': 'green'}))
        assert len(docs) == 1
        assert docs[0]['name'] == 'bob'

        docs = list(self.collection.find({'hat.type': 'bowler'}))
        assert len(docs) == 2

        docs = list(self.collection.find({
            'hat.color': 'red',
            'hat.type': 'bowler'}))
        assert len(docs) == 1
        assert docs[0]['name'] == 'sam'

        docs = list(self.collection.find({
            'name': 'bob',
            'hat.color': 'red',
            'hat.type': 'bowler'}))
        assert len(docs) == 0

        docs = list(self.collection.find({'hat': 'a hat'}))
        assert len(docs) == 0

        docs = list(self.collection.find({'hat.color.cat': 'red'}))
        assert len(docs) == 0

    def test__find_by_regex(self):
        """Test searching with regular expression objects."""
        bob = {'name': 'bob'}
        sam = {'name': 'sam'}

        self.collection.insert(bob)
        self.collection.insert(sam)
        self.assertEquals(len(list(self.collection.find())), 3)

        regex = re.compile('bob|sam')
        docs = list(self.collection.find({'name': regex}))
        assert len(docs) == 2
        assert docs[0]['name'] in ('bob', 'sam')
        assert docs[1]['name'] in ('bob', 'sam')

        regex = re.compile('bob|notsam')
        docs = list(self.collection.find({'name': regex}))
        assert len(docs) == 1
        assert docs[0]['name'] == 'bob'

class UpdateTest(DocumentTest):
    def test__update(self):
        new_document = dict(new_attr=2)
        self.collection.update(dict(a=self.document['a']), new_document)
        expected_new_document = dict(new_document, _id=self.document_id)
        self.assertEquals(list(self.collection.find()), [expected_new_document])
