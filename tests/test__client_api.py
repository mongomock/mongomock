import unittest

import mongomock

try:
    from pymongo.read_preferences import ReadPreference
    _HAVE_PYMONGO = True
except ImportError:
    _HAVE_PYMONGO = False


class MongoClientApiTest(unittest.TestCase):

    def test__read_preference(self):
        client = mongomock.MongoClient()
        self.assertEqual('Primary', client.read_preference.name)
        self.assertEqual(client.read_preference, client.db.read_preference)
        self.assertEqual(client.read_preference, client.db.coll.read_preference)

        client2 = mongomock.MongoClient(read_preference=client.read_preference)
        self.assertEqual(client2.read_preference, client.read_preference)

        with self.assertRaises(TypeError):
            mongomock.MongoClient(read_preference=0)

    @unittest.skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__different_read_preference(self):
        client = mongomock.MongoClient(read_preference=ReadPreference.NEAREST)
        self.assertEqual(ReadPreference.NEAREST, client.db.read_preference)
        self.assertEqual(ReadPreference.NEAREST, client.db.coll.read_preference)

    def test__parse_url(self):
        client = mongomock.MongoClient('mongodb://localhost:27017/')
        self.assertEqual(('localhost', 27017), client.address)

        client = mongomock.MongoClient('mongodb://localhost:1234,example.com/')
        self.assertEqual(('localhost', 1234), client.address)

        client = mongomock.MongoClient('mongodb://example.com,localhost:1234/')
        self.assertEqual(('example.com', 27017), client.address)

        client = mongomock.MongoClient('mongodb://[::1]:1234/')
        self.assertEqual(('::1', 1234), client.address)

        with self.assertRaises(ValueError):
            mongomock.MongoClient('mongodb://localhost:1234:456/')

        with self.assertRaises(ValueError):
            mongomock.MongoClient('mongodb://localhost:123456/')

        with self.assertRaises(ValueError):
            mongomock.MongoClient('mongodb://localhost:mongoport/')

    def test__parse_hosts(self):
        client = mongomock.MongoClient('localhost')
        self.assertEqual(('localhost', 27017), client.address)

        client = mongomock.MongoClient('localhost:1234,example.com')
        self.assertEqual(('localhost', 1234), client.address)

        client = mongomock.MongoClient('example.com,localhost:1234')
        self.assertEqual(('example.com', 27017), client.address)

        client = mongomock.MongoClient('[::1]:1234')
        self.assertEqual(('::1', 1234), client.address)

        client = mongomock.MongoClient('/var/socket/mongo.sock')
        self.assertEqual(('/var/socket/mongo.sock', None), client.address)

        with self.assertRaises(ValueError):
            mongomock.MongoClient('localhost:1234:456')

        with self.assertRaises(ValueError):
            mongomock.MongoClient('localhost:123456')

        with self.assertRaises(ValueError):
            mongomock.MongoClient('localhost:mongoport')
