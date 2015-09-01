import mongomock
import mock

try:
    import pymongo
    from pymongo import ReturnDocument
    _HAVE_PYMONGO = True
except ImportError:
    _HAVE_PYMONGO = False

from .utils import TestCase

class BulkOperationsTest(TestCase):

    def setUp(self):
        super(BulkOperationsTest, self).setUp()
        self.client = mongomock.MongoClient()
        #self.client = pymongo.MongoClient()
        self.db = self.client['somedb']
        self.db.collection.drop()
        for _i in "abx":
            self.db.collection.create_index(_i, unique=False, name="idx" + _i, sparse=True, background=True)
        self.bulk_op = self.db.collection.initialize_ordered_bulk_op()


    def __check_document(self, doc, count=1):
        found_num = self.db.collection.find(doc).count()
        if found_num != count:
            all = list(self.db.collection.find())
            self.fail("Document %s count()=%s BUT expected count=%s! All documents: %s" % (doc, found_num, count, all))


    def __check_result(self, result, **expecting_values):
        for key in ('nModified', 'nUpserted','nMatched', 'writeErrors', 'upserted', 'writeConcernErrors', 'nRemoved',
                    'nInserted'):
            exp_val = expecting_values.get(key)
            has_val = result.get(key)
            self.assertFalse(has_val is None, "Missed key '%s' in result: %s" % (key, result))
            if exp_val:
                self.assertEqual(exp_val, has_val,
                             "Invalid result %s=%s (but expected value=%s)" % (key, has_val, exp_val))
            else:
                self.assertFalse(bool(has_val), "Received unexpected value %s = %s" % (key, has_val))

    def __execute_and_check_result(self, write_concern=None, **expecting_result):
        result = self.bulk_op.execute(write_concern=write_concern)
        self.__check_result(result, **expecting_result)

    def __check_number_of_elements(self, count):
        has_count = self.db.collection.count()
        self.assertEqual(has_count, count, "There is %s documents but there should be %s" % (has_count, count))


    def test__insert(self):
        self.bulk_op.insert({"a": 1, "b": 2})
        self.bulk_op.insert({"a": 2, "b": 4})
        self.bulk_op.insert({"a": 2, "b": 6})

        self.__check_number_of_elements(0)
        self.__execute_and_check_result(nInserted=3)
        self.__check_document({"a": 1, "b": 2})
        self.__check_document({"a": 2, "b": 4})
        self.__check_document({"a": 2, "b": 6})


    def test__bulk_update_must_raise_error_if_missed_operator(self):
        self.assertRaises(ValueError, self.bulk_op.find({"a": 1}).update, {"b": 20})


    def test_update(self):
        self.bulk_op.find({"a": 1}).update({"$set": {"b": 20}})
        self.__execute_and_check_result()
        self.__check_number_of_elements(0)


    def test__update_must_update_all_documents(self):
        self.db.collection.insert({"a": 1, "b": 2})
        self.db.collection.insert({"a": 2, "b": 4})
        self.db.collection.insert({"a": 2, "b": 8})

        self.bulk_op.find({"a": 1}).update({"$set": {"b": 20}})
        self.bulk_op.find({"a": 2}).update({"$set": {"b": 40}})

        self.__check_document({"a": 1, "b": 2})
        self.__check_document({"a": 2, "b": 4})
        self.__check_document({"a": 2, "b": 8})

        self.__execute_and_check_result(nMatched=3, nModified=3)
        self.__check_document({"a": 1, "b": 20})
        self.__check_document({"a": 2, "b": 40}, 2)


    def test__ordered_insert_and_update(self):
        self.bulk_op.insert({"a": 1, "b": 2})
        self.bulk_op.find({"a": 1}).update({"$set": {"b": 3}})
        self.__execute_and_check_result(nInserted=1, nMatched=1, nModified=1)
        self.__check_document({"a": 1, "b": 3})


    def test__update_one(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 2})

        self.bulk_op.find({"a": 2}).update_one({"$set": {"b": 3}})
        self.__execute_and_check_result(nMatched=1, nModified=1)
        self.__check_document({"a": 2}, count=2)
        self.__check_number_of_elements(2)


    def test__remove(self):
        self.bulk_op.find({"a": 2}).remove()
        self.__execute_and_check_result()
        self.__check_number_of_elements(0)


    def test__remove(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 2})

        self.bulk_op.find({"a": 2}).remove()

        self.__execute_and_check_result(nRemoved=2)
        self.__check_number_of_elements(0)


    def test__remove_one(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 2})

        self.bulk_op.find({"a": 2}).remove_one()

        self.__execute_and_check_result(nRemoved=1)
        self.__check_document({"a": 2}, 1)
        self.__check_number_of_elements(1)


    def test_upsert_replace_one_on_empty_set(self):
        self.bulk_op.find({}).upsert().replace_one({"x": 1})
        self.__execute_and_check_result(nUpserted=1, upserted=[{"index": 0, "_id": mock.ANY}])


    def test_upsert_replace_one(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 2})
        self.bulk_op.find({"a": 2}).replace_one({"x": 1})
        self.__execute_and_check_result(nModified=1, nMatched=1)
        self.__check_document({"a": 2}, 1)
        self.__check_document({"x": 1}, 1)
        self.__check_number_of_elements(2)


    def test_upsert_update_on_empty_set(self):
        self.bulk_op.find({}).upsert().update({"$set": {"a": 1, "b": 2}})
        self.__execute_and_check_result(nUpserted=1, upserted=[{"index": 0, "_id": mock.ANY}])
        self.__check_document({"a": 1, "b": 2})
        self.__check_number_of_elements(1)

    def test_upsert_update(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 2})
        self.bulk_op.find({"a": 2}).upsert().update({"$set": {"b": 3}})
        self.__execute_and_check_result(nMatched=2, nModified=2)
        self.__check_document({"a": 2, "b": 3}, 2)
        self.__check_number_of_elements(2)


    def test_upsert_update_one(self):
        self.db.collection.insert({"a": 2, "b": 1})
        self.db.collection.insert({"a": 2, "b": 1})
        self.bulk_op.find({"a": 2}).upsert().update_one({"$inc": {"b": 1, "x": 1}})
        self.__execute_and_check_result(nModified=1, nMatched=1)
        self.__check_document({"a": 2, "b": 1}, 1)
        self.__check_document({"a": 2, "b": 2, "x": 1}, 1)
        self.__check_number_of_elements(2)
