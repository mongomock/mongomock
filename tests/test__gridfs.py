import gridfs
import datetime
import mongomock
from nose.tools import assert_raises
from mongomock.gridfs import MockGridFS
from unittest import TestCase, skipIf


try:
    from bson.objectid import ObjectId
    import pymongo
    from pymongo import MongoClient as PymongoClient
    _HAVE_PYMONGO = True
except ImportError:
    from mongomock.object_id import ObjectId
    _HAVE_PYMONGO = False

@skipIf(not _HAVE_PYMONGO, "pymongo not installed")
class GridFsTest(TestCase):

    def setUp(self):
        super(GridFsTest, self).setUp()
        self.fake_conn = mongomock.MongoClient()
        self.mongo_conn = self._connect_to_local_mongodb()
        self.db_name = "mongomock___testing_db"
        
        self.mongo_conn[self.db_name]["fs"]["files"].remove()
        self.mongo_conn[self.db_name]["fs"]["chunks"].remove()

        self.real_gridfs = gridfs.GridFS(self.mongo_conn[self.db_name])
        self.fake_gridfs = MockGridFS(self.fake_conn[self.db_name])

    def tearDown(self):
        super(GridFsTest, self).setUp()
        self.mongo_conn.close()
        self.fake_conn.close()

    def test__put_get_small(self):
        fid = self.fake_gridfs.put(GenFile(50))
        rid = self.real_gridfs.put(GenFile(50))
        ffile = self.fake_gridfs.get(fid)
        rfile = self.real_gridfs.get(rid)
        self.assertEquals(ffile.read(), rfile.read())
        fake_doc = self.get_fake_file(fid)
        mongo_doc = self.get_mongo_file(rid)
        self.assertSameFile(mongo_doc, fake_doc)
        
    def test__put_get_big(self):
        # 500k files are bigger than doc size limit
        fid = self.fake_gridfs.put(GenFile(500000, 10))
        rid = self.real_gridfs.put(GenFile(500000, 10))
        ffile = self.fake_gridfs.get(fid)
        rfile = self.real_gridfs.get(rid)
        self.assertEquals(ffile.read(), rfile.read())
        fake_doc = self.get_fake_file(fid)
        mongo_doc = self.get_mongo_file(rid)
        self.assertSameFile(mongo_doc, fake_doc)

    def test__delete_exists_small(self):
        fid = self.fake_gridfs.put(GenFile(50))
        self.assertTrue(self.get_fake_file(fid) is not None)
        self.assertTrue(self.fake_gridfs.exists(fid))
        self.fake_gridfs.delete(fid)
        self.assertFalse(self.fake_gridfs.exists(fid))
        self.assertFalse(self.get_fake_file(fid) is not None)
        # All the chunks got removed
        self.assertEquals(0, self.fake_conn[self.db_name].fs.chunks.find({}).count())

    def test__delete_exists_big(self):
        fid = self.fake_gridfs.put(GenFile(500000))
        self.assertTrue(self.get_fake_file(fid) is not None)
        self.assertTrue(self.fake_gridfs.exists(fid))
        self.fake_gridfs.delete(fid)
        self.assertFalse(self.fake_gridfs.exists(fid))
        self.assertFalse(self.get_fake_file(fid) is not None)
        # All the chunks got removed
        self.assertEquals(0, self.fake_conn[self.db_name].fs.chunks.find({}).count())

    def test__delete_no_file(self):
        # Just making sure we don't crash
        self.fake_gridfs.delete(ObjectId("AAAAAAAAAAAA"))


    def test__list_files(self):
        fids = [self.fake_gridfs.put(GenFile(50,9), filename="one"),
                self.fake_gridfs.put(GenFile(62,5), filename="two"),
                self.fake_gridfs.put(GenFile(654,1), filename="three"),
                self.fake_gridfs.put(GenFile(5), filename="four")]
        names = ["one", "two", "three", "four"]
        names_no_two = [x for x in names if x != "two"]
        for x in self.fake_gridfs.list():
            self.assertTrue(x in names)
            
        self.fake_gridfs.delete(fids[1])

        for x in self.fake_gridfs.list():
            self.assertTrue(x in names_no_two)

        three_file = self.get_fake_file(fids[2])
        self.assertEquals("three", three_file["filename"])
        self.assertEquals(654, three_file["length"])
        self.fake_gridfs.delete(fids[0])
        self.fake_gridfs.delete(fids[2])
        self.fake_gridfs.delete(fids[3])
        self.assertEquals(0, len(self.fake_gridfs.list()))
        
    def test__find_files(self):
        fids = [self.fake_gridfs.put(GenFile(50,9), filename="a"),
                self.fake_gridfs.put(GenFile(62,5), filename="b"),
                self.fake_gridfs.put(GenFile(654,1), filename="b"),
                self.fake_gridfs.put(GenFile(5), filename="a")]
        c = self.fake_gridfs.find({"filename":"a"}).sort("uploadDate", -1)
        should_be_fid3 = c.next()
        should_be_fid0 = c.next()
        self.assertEquals(0, c.count())

        self.assertEquals(fids[3], should_be_fid3._id)
        self.assertEquals(fids[0], should_be_fid0._id)

    def test__put_exists(self):
        self.fake_gridfs.put(GenFile(1), _id="12345")
        with assert_raises(gridfs.errors.FileExists):
            self.fake_gridfs.put(GenFile(2, 3), _id="12345")

        
    def assertSameFile(self, real, fake):
        self.assertEquals(real["md5"], fake["md5"])
        self.assertEquals(real["length"], fake["length"])
        self.assertEquals(real["chunkSize"], fake["chunkSize"])
        self.assertTrue(abs(real["uploadDate"] - fake["uploadDate"]).seconds <= 1)

    def get_mongo_file(self, i):
        return self.mongo_conn[self.db_name]["fs"]["files"].find_one({"_id":i})

    def get_fake_file(self, i):
        return self.fake_conn[self.db_name]["fs"]["files"].find_one({"_id":i})
    
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

class GenFile:
    def __init__(self, length, value=0):
        self.gen = self._gen_data(length, value)
        
    def _gen_data(self, length, value):
        while (length != 0):
            length-=1
            yield value

    def read(self, num_bytes=-1):
        s = ""
        if num_bytes <= 0:
            bytes_left = -1
        else:
            bytes_left = num_bytes
        while True:
            n = next(self.gen, None)
            if n is None:
                return s
            s+=chr(n)
            bytes_left -= 1
            if bytes_left == 0:
                return s
            
