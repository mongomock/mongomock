import unittest

import mongomock_ng as mongomock


class NotImplementedTests(unittest.TestCase):
    def tearDown(self):
        mongomock.warn_on_feature('comment')

    def test_raises(self):
        collection = mongomock.MongoClient().db.collection
        with self.assertRaises(NotImplementedError):
            collection.insert_one({}, comment='test')

    def test_ignores(self):
        mongomock.ignore_feature('comment')

        collection = mongomock.MongoClient().db.collection
        collection.insert_one({}, comment='test')

    def test_on_and_off(self):
        collection = mongomock.MongoClient().db.collection

        with self.assertRaises(NotImplementedError):
            collection.insert_one({'_id': 1}, comment='test')

        mongomock.ignore_feature('comment')

        collection.insert_one({'_id': 2}, comment='test')

        mongomock.warn_on_feature('comment')

        with self.assertRaises(NotImplementedError):
            collection.insert_one({'_id': 3}, comment='test')

        self.assertEqual({2}, {doc['_id'] for doc in collection.find()})

    def test_wrong_key(self):
        with self.assertRaises(KeyError):
            mongomock.ignore_feature('sessions')
