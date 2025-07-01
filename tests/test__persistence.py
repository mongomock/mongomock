"""test store persistence"""
import os
import unittest
from tempfile import NamedTemporaryFile
from mongomock import MongoClient
from mongomock.store import ServerStore


class ServerStorePersistenceTest(unittest.TestCase):
    """test server store persistence"""

    ref_str_1 = '{"test_db": {"test_coll": {"name": "test_coll", "documents": [[{"$oid": "'
    ref_str_2 = '"}, {"test": true, "_id": {"$oid": "'

    def setUp(self):
        with NamedTemporaryFile(mode='w', prefix='mongodb-', suffix='.json',
                                encoding='utf-8', delete=False) as fh:
            fh.write('{}')
            self.filename = fh.name
        os.environ['MONGOMOCK_SERVERSTORE_FILE'] = self.filename

    def tearDown(self):
        os.unlink(self.filename)
        del os.environ['MONGOMOCK_SERVERSTORE_FILE']

    def test_kwargs_method(self):
        """test by using custom ServerStore with kwargs filename"""
        store = ServerStore(filename=self.filename)
        client = MongoClient(_store=store)
        client.test_db.test_coll.insert_one({'test': True})
        finalizer = getattr(store, '_finalizer')
        assert finalizer.alive
        finalizer()
        with open(self.filename, 'r', encoding='utf-8') as fh:
            contents = fh.read()
        assert self.ref_str_1 in contents
        assert self.ref_str_2 in contents

    def test_environ_method(self):
        """test by using an environment variable"""
        client = MongoClient()
        client.test_db.test_coll.insert_one({'test': True})
        finalizer = getattr(getattr(client, '_store'), '_finalizer')
        assert finalizer.alive
        finalizer()
        with open(self.filename, 'r', encoding='utf-8') as fh:
            contents = fh.read()
        assert self.ref_str_1 == contents[:73]
        assert self.ref_str_2 == contents[97:133]
