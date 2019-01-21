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
