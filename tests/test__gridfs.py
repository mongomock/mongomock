import os
import time
import unittest
from unittest import mock
from unittest import skipIf
from unittest import skipUnless
from unittest import TestCase

import mongomock_ng as mongomock
import mongomock_ng.gridfs
from mongomock_ng import helpers


try:
    import gridfs
    from gridfs import errors

    _HAVE_GRIDFS = True
except ImportError:
    _HAVE_GRIDFS = False


try:
    import pymongo
    from bson.objectid import ObjectId
    from pymongo import MongoClient as PymongoClient
except ImportError:
    ...


@skipUnless(helpers.HAVE_PYMONGO, 'pymongo not installed')
@skipUnless(_HAVE_GRIDFS and hasattr(gridfs.__builtins__, 'copy'), 'gridfs not installed')
@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class GridFsTest(TestCase):
    @classmethod
    def setUpClass(cls):
        mongomock.gridfs.enable_gridfs_integration()
        try:
            conn = PymongoClient(
                host=os.environ.get('TEST_MONGO_HOST', 'localhost'),
                serverSelectionTimeoutMS=2000,
            )
            conn.admin.command('ping')
            conn.close()
        except Exception:
            raise unittest.SkipTest('No local MongoDB server available') from None

    def setUp(self):
        super().setUp()
        self.fake_conn = mongomock.MongoClient()
        self.mongo_conn = self._connect_to_local_mongodb()
        self.db_name = 'mongomock_ng___testing_db'

        self.mongo_conn[self.db_name]['fs']['files'].drop()
        self.mongo_conn[self.db_name]['fs']['chunks'].drop()

        self.real_gridfs = gridfs.GridFS(self.mongo_conn[self.db_name])
        self.fake_gridfs = gridfs.GridFS(self.fake_conn[self.db_name])

    def tearDown(self):
        super().setUp()
        self.mongo_conn.close()
        self.fake_conn.close()

    def test__put_get_small(self):
        before = time.time()
        fid = self.fake_gridfs.put(GenFile(50))
        rid = self.real_gridfs.put(GenFile(50))
        after = time.time()
        ffile = self.fake_gridfs.get(fid)
        rfile = self.real_gridfs.get(rid)
        self.assertEqual(ffile.read(), rfile.read())
        fake_doc = self.get_fake_file(fid)
        mongo_doc = self.get_mongo_file(rid)
        self.assertSameFile(mongo_doc, fake_doc, max_delta_seconds=after - before + 1)

    def test__put_get_big(self):
        # 500k files are bigger than doc size limit
        before = time.time()
        fid = self.fake_gridfs.put(GenFile(500000, 10))
        rid = self.real_gridfs.put(GenFile(500000, 10))
        after = time.time()
        ffile = self.fake_gridfs.get(fid)
        rfile = self.real_gridfs.get(rid)
        self.assertEqual(ffile.read(), rfile.read())
        fake_doc = self.get_fake_file(fid)
        mongo_doc = self.get_mongo_file(rid)
        self.assertSameFile(mongo_doc, fake_doc, max_delta_seconds=after - before + 1)

    def test__delete_exists_small(self):
        fid = self.fake_gridfs.put(GenFile(50))
        self.assertTrue(self.get_fake_file(fid) is not None)
        self.assertTrue(self.fake_gridfs.exists(fid))
        self.fake_gridfs.delete(fid)
        self.assertFalse(self.fake_gridfs.exists(fid))
        self.assertFalse(self.get_fake_file(fid) is not None)
        # All the chunks got removed
        self.assertEqual(0, self.fake_conn[self.db_name].fs.chunks.count_documents({}))

    def test__delete_exists_big(self):
        fid = self.fake_gridfs.put(GenFile(500000))
        self.assertTrue(self.get_fake_file(fid) is not None)
        self.assertTrue(self.fake_gridfs.exists(fid))
        self.fake_gridfs.delete(fid)
        self.assertFalse(self.fake_gridfs.exists(fid))
        self.assertFalse(self.get_fake_file(fid) is not None)
        # All the chunks got removed
        self.assertEqual(0, self.fake_conn[self.db_name].fs.chunks.count_documents({}))

    def test__delete_no_file(self):
        # Just making sure we don't crash
        self.fake_gridfs.delete(ObjectId())

    def test__list_files(self):
        fids = [
            self.fake_gridfs.put(GenFile(50, 9), filename='one'),
            self.fake_gridfs.put(GenFile(62, 5), filename='two'),
            self.fake_gridfs.put(GenFile(654, 1), filename='three'),
            self.fake_gridfs.put(GenFile(5), filename='four'),
        ]
        names = ['one', 'two', 'three', 'four']
        names_no_two = [x for x in names if x != 'two']
        for x in self.fake_gridfs.list():
            self.assertIn(x, names)

        self.fake_gridfs.delete(fids[1])

        for x in self.fake_gridfs.list():
            self.assertIn(x, names_no_two)

        three_file = self.get_fake_file(fids[2])
        self.assertEqual('three', three_file['filename'])
        self.assertEqual(654, three_file['length'])
        self.fake_gridfs.delete(fids[0])
        self.fake_gridfs.delete(fids[2])
        self.fake_gridfs.delete(fids[3])
        self.assertEqual(0, len(self.fake_gridfs.list()))

    def test__find_files(self):
        file_ids = []
        for name, data in [
            ('a', GenFile(50, 9)),
            ('b', GenFile(62, 5)),
            ('b', GenFile(654, 1)),
            ('a', GenFile(5)),
        ]:
            time.sleep(0.001)
            file_ids.append(self.fake_gridfs.put(data, filename=name))

        c = self.fake_gridfs.find({'filename': 'a'}).sort('uploadDate', -1)
        file3 = c.next()
        file0 = c.next()
        self.assertFalse(c.alive)
        self.assertNotEqual(file3.uploadDate, file0.uploadDate)

        self.assertEqual(file_ids[3], file3._id)
        self.assertEqual(file_ids[0], file0._id)

    def test__put_exists(self):
        self.fake_gridfs.put(GenFile(1), _id='12345')
        with self.assertRaises(errors.FileExists):
            self.fake_gridfs.put(GenFile(2, 3), _id='12345')

    def assertSameFile(self, real, fake, max_delta_seconds=1):
        self.assertEqual(real['length'], fake['length'])
        self.assertEqual(real['chunkSize'], fake['chunkSize'])
        self.assertLessEqual(
            abs(real['uploadDate'] - fake['uploadDate']).seconds,
            max_delta_seconds,
            msg='real: {}, fake: {}'.format(real['uploadDate'], fake['uploadDate']),
        )

    def get_mongo_file(self, i):
        return self.mongo_conn[self.db_name]['fs']['files'].find_one({'_id': i})

    def get_fake_file(self, i):
        return self.fake_conn[self.db_name]['fs']['files'].find_one({'_id': i})

    def _connect_to_local_mongodb(self, num_retries=60):
        """Performs retries on connection errors (for travis-ci builds)"""
        for retry in range(num_retries):
            if retry > 0:
                time.sleep(0.5)
            try:
                return PymongoClient(
                    host=os.environ.get('TEST_MONGO_HOST', 'localhost'), maxPoolSize=1
                )
            except pymongo.errors.ConnectionFailure:
                if retry == num_retries - 1:
                    raise


class GenFile:
    def __init__(self, length, value=0, do_encode=True):
        self.gen = self._gen_data(length, value)
        self.do_encode = do_encode

    def _gen_data(self, length, value):
        while length:
            length -= 1
            yield value

    def _maybe_encode(self, s):
        if self.do_encode and isinstance(s, str):
            return s.encode('UTF-8')
        return s

    def read(self, num_bytes=-1):
        s = ''
        bytes_left = -1 if num_bytes <= 0 else num_bytes
        while True:
            n = next(self.gen, None)
            if n is None:
                return self._maybe_encode(s)
            s += chr(n)
            bytes_left -= 1
            if bytes_left == 0:
                return self._maybe_encode(s)


class TestMongoMockGridOutCursor(unittest.TestCase):
    """Tests for _MongoMockGridOutCursor: add_option, remove_option, _clone_base."""

    def setUp(self):
        self.client = mongomock.MongoClient()
        self.db = self.client['test_db']
        self.collection = self.db['fs']

    def _make_cursor(self):
        from mongomock_ng.gridfs import _MongoMockGridOutCursor

        return _MongoMockGridOutCursor(self.collection)

    def test_add_option_raises_not_implemented(self):
        cursor = self._make_cursor()
        with self.assertRaises(NotImplementedError):
            cursor.add_option(64)

    def test_remove_option_raises_not_implemented(self):
        cursor = self._make_cursor()
        with self.assertRaises(NotImplementedError):
            cursor.remove_option(64)

    def test_clone_base_returns_grid_out_cursor(self):
        from mongomock_ng.gridfs import _MongoMockGridOutCursor

        cursor = self._make_cursor()
        cloned = cursor._clone_base(session=None)
        self.assertIsInstance(cloned, _MongoMockGridOutCursor)

    def test_clone_base_creates_new_instance(self):
        cursor = self._make_cursor()
        cloned = cursor._clone_base(session=None)
        self.assertIsNot(cloned, cursor)

    def test_clone_base_carries_session(self):
        cursor = self._make_cursor()
        cloned = cursor._clone_base(session='my_session')
        self.assertEqual(cloned.session, 'my_session')


class TestCreateGridOutCursor(unittest.TestCase):
    """Tests for _create_grid_out_cursor dispatch."""

    def test_with_mongomock_collection(self):
        from mongomock_ng.gridfs import _create_grid_out_cursor
        from mongomock_ng.gridfs import _MongoMockGridOutCursor

        client = mongomock.MongoClient()
        db = client['test_db']
        result = _create_grid_out_cursor(db['fs'])
        self.assertIsInstance(result, _MongoMockGridOutCursor)

    @skipUnless(_HAVE_GRIDFS, 'gridfs not installed')
    def test_with_pymongo_collection(self):
        from gridfs.grid_file import GridOutCursor as PyMongoGridOutCursor
        from pymongo.collection import Collection as PyMongoCollection

        from mongomock_ng.gridfs import _create_grid_out_cursor

        mock_collection = mock.MagicMock(spec=PyMongoCollection)
        mock_collection.files = mock.MagicMock()
        result = _create_grid_out_cursor(mock_collection)
        self.assertIsInstance(result, PyMongoGridOutCursor)


@skipUnless(_HAVE_GRIDFS, 'gridfs not installed')
class TestEnableGridFSIntegration(unittest.TestCase):
    """Tests for enable_gridfs_integration."""

    def test_enable_without_pymongo_raises_error(self):
        with (
            mock.patch.object(mongomock.gridfs, '_HAVE_PYMONGO', False),
            self.assertRaises(NotImplementedError),
        ):
            mongomock.gridfs.enable_gridfs_integration()

    def test_enable_runs_without_error(self):
        mongomock.gridfs.enable_gridfs_integration()

    def test_can_create_gridfs_after_enable(self):
        import gridfs

        mongomock.gridfs.enable_gridfs_integration()
        client = mongomock.MongoClient()
        gfs = gridfs.GridFS(client['test_db'])
        self.assertIsNotNone(gfs)

    def test_put_and_get_after_enable(self):
        import gridfs

        mongomock.gridfs.enable_gridfs_integration()
        client = mongomock.MongoClient()
        gfs = gridfs.GridFS(client['test_db'])
        data = b'test data for enable_gridfs_integration'
        fid = gfs.put(data, filename='test.txt')
        gout = gfs.get(fid)
        self.assertEqual(gout.read(), data)


if __name__ == '__main__':
    unittest.main()
