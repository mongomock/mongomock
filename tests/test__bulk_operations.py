import os
import warnings
from unittest import mock

import mongomock_ng as mongomock
from mongomock_ng import helpers


try:
    import pymongo
except ImportError:
    pymongo = None

from unittest import skipIf
from unittest import TestCase

from packaging import version

from tests.multicollection import MultiCollection


class BulkOperationsTest(TestCase):
    def setUp(self):
        super().setUp()
        warnings.filterwarnings('ignore', category=DeprecationWarning)
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']
        self.db.collection.drop()
        for _i in 'abx':
            self.db.collection.create_index(
                _i, unique=False, name='idx' + _i, sparse=True, background=True
            )
        self.bulk_op = self.db.collection.initialize_ordered_bulk_op()

    def __check_document(self, doc, count=1):
        found_num = self.db.collection.count_documents(doc)
        if found_num != count:
            all = list(self.db.collection.find())
            self.fail(
                f'Document {doc} count()={found_num} BUT expected count={count}! All'
                f' documents: {all}'
            )

    def __check_result(self, result, **expecting_values):
        for key in (
            'nModified',
            'nUpserted',
            'nMatched',
            'writeErrors',
            'upserted',
            'writeConcernErrors',
            'nRemoved',
            'nInserted',
        ):
            exp_val = expecting_values.get(key)
            has_val = result.get(key)
            self.assertFalse(has_val is None, f"Missed key '{key}' in result: {result}")
            if exp_val:
                self.assertEqual(
                    exp_val,
                    has_val,
                    f'Invalid result {key}={has_val} (but expected value={exp_val})',
                )
            else:
                self.assertFalse(bool(has_val), f'Received unexpected value {key} = {has_val}')

    def __execute_and_check_result(self, write_concern=None, **expecting_result):
        result = self.bulk_op.execute(write_concern=write_concern)
        self.__check_result(result, **expecting_result)

    def __check_number_of_elements(self, count):
        has_count = self.db.collection.count_documents({})
        self.assertEqual(
            has_count, count, f'There is {has_count} documents but there should be {count}'
        )

    def test__insert(self):
        self.bulk_op.insert({'a': 1, 'b': 2})
        self.bulk_op.insert({'a': 2, 'b': 4})
        self.bulk_op.insert({'a': 2, 'b': 6})

        self.__check_number_of_elements(0)
        self.__execute_and_check_result(nInserted=3)
        self.__check_document({'a': 1, 'b': 2})
        self.__check_document({'a': 2, 'b': 4})
        self.__check_document({'a': 2, 'b': 6})

    def test__bulk_update_must_raise_error_if_missed_operator(self):
        self.assertRaises(ValueError, self.bulk_op.find({'a': 1}).update, {'b': 20})

    def test__bulk_execute_must_raise_error_if_bulk_empty(self):
        self.assertRaises(mongomock.InvalidOperation, self.bulk_op.execute)

    def test_update(self):
        self.bulk_op.find({'a': 1}).update({'$set': {'b': 20}})
        self.__execute_and_check_result()
        self.__check_number_of_elements(0)

    def test__update_must_update_all_documents(self):
        self.db.collection.insert_one({'a': 1, 'b': 2})
        self.db.collection.insert_one({'a': 2, 'b': 4})
        self.db.collection.insert_one({'a': 2, 'b': 8})

        self.bulk_op.find({'a': 1}).update({'$set': {'b': 20}})
        self.bulk_op.find({'a': 2}).update({'$set': {'b': 40}})

        self.__check_document({'a': 1, 'b': 2})
        self.__check_document({'a': 2, 'b': 4})
        self.__check_document({'a': 2, 'b': 8})

        self.__execute_and_check_result(nMatched=3, nModified=3)
        self.__check_document({'a': 1, 'b': 20})
        self.__check_document({'a': 2, 'b': 40}, 2)

    def test__ordered_insert_and_update(self):
        self.bulk_op.insert({'a': 1, 'b': 2})
        self.bulk_op.find({'a': 1}).update({'$set': {'b': 3}})
        self.__execute_and_check_result(nInserted=1, nMatched=1, nModified=1)
        self.__check_document({'a': 1, 'b': 3})

    def test__update_one(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 2})

        self.bulk_op.find({'a': 2}).update_one({'$set': {'b': 3}})
        self.__execute_and_check_result(nMatched=1, nModified=1)
        self.__check_document({'a': 2}, count=2)
        self.__check_number_of_elements(2)

    def test__remove(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 2})

        self.bulk_op.find({'a': 2}).remove()

        self.__execute_and_check_result(nRemoved=2)
        self.__check_number_of_elements(0)

    def test__remove_one(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 2})

        self.bulk_op.find({'a': 2}).remove_one()

        self.__execute_and_check_result(nRemoved=1)
        self.__check_document({'a': 2}, 1)
        self.__check_number_of_elements(1)

    def test_upsert_replace_one_on_empty_set(self):
        self.bulk_op.find({}).upsert().replace_one({'x': 1})
        self.__execute_and_check_result(nUpserted=1, upserted=[{'index': 0, '_id': mock.ANY}])

    def test_upsert_replace_one(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 2})
        self.bulk_op.find({'a': 2}).replace_one({'x': 1})
        self.__execute_and_check_result(nModified=1, nMatched=1)
        self.__check_document({'a': 2}, 1)
        self.__check_document({'x': 1}, 1)
        self.__check_number_of_elements(2)

    def test_upsert_update_on_empty_set(self):
        self.bulk_op.find({}).upsert().update({'$set': {'a': 1, 'b': 2}})
        self.__execute_and_check_result(nUpserted=1, upserted=[{'index': 0, '_id': mock.ANY}])
        self.__check_document({'a': 1, 'b': 2})
        self.__check_number_of_elements(1)

    def test_upsert_update(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 2})
        self.bulk_op.find({'a': 2}).upsert().update({'$set': {'b': 3}})
        self.__execute_and_check_result(nMatched=2, nModified=2)
        self.__check_document({'a': 2, 'b': 3}, 2)
        self.__check_number_of_elements(2)

    def test_upsert_update_one(self):
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.db.collection.insert_one({'a': 2, 'b': 1})
        self.bulk_op.find({'a': 2}).upsert().update_one({'$inc': {'b': 1, 'x': 1}})
        self.__execute_and_check_result(nModified=1, nMatched=1)
        self.__check_document({'a': 2, 'b': 1}, 1)
        self.__check_document({'a': 2, 'b': 2, 'x': 1}, 1)
        self.__check_number_of_elements(2)


@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class BulkOperationsWithPymongoTest(TestCase):
    def setUp(self):
        super().setUp()
        self.fake_client = mongomock.MongoClient()
        self.real_client = pymongo.MongoClient(
            host=os.environ.get('TEST_MONGO_HOST', 'mongodb://127.0.0.1:27017/mock?replicaSet=rs0')
        )
        self.db_name = 'mongomock_ng___testing_db'
        self.collection_name = 'mongomock_ng___testing_collection'
        self.real_client[self.db_name][self.collection_name].drop()
        self.cmp = MultiCollection(
            {
                'fake': self.fake_client[self.db_name][self.collection_name],
                'real': self.real_client[self.db_name][self.collection_name],
            }
        )

    def test__bulk_write_insert(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 2, 'b': 2}])
        self.cmp.compare.find(sort=[('a', 1)])

    def test__bulk_write_update_one(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
        self.cmp.do.bulk_write([pymongo.UpdateOne({'a': 1}, {'$set': {'b': 10}})])
        self.cmp.compare.find({'a': 1}, sort=[('b', 1)])

    def test__bulk_write_update_many(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
        self.cmp.do.bulk_write([pymongo.UpdateMany({'a': 1}, {'$set': {'c': 3}})])
        self.cmp.compare.find({'a': 1}, sort=[('b', 1)])

    def test__bulk_write_replace_one(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 2, 'b': 2}])
        self.cmp.do.bulk_write([pymongo.ReplaceOne({'a': 1}, {'a': 1, 'b': 99})])
        self.cmp.compare.find(sort=[('a', 1)])

    def test__bulk_write_delete_one(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
        self.cmp.do.bulk_write([pymongo.DeleteOne({'a': 1})])
        self.cmp.compare.find({'a': 1})

    def test__bulk_write_delete_many(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}, {'a': 2, 'b': 3}])
        self.cmp.do.bulk_write([pymongo.DeleteMany({'a': 1})])
        self.cmp.compare.find(sort=[('a', 1)])

    def test__bulk_write_mixed_operations(self):
        self.cmp.do.insert_many(
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 3, 'b': 3},
                {'a': 4, 'b': 4},
            ]
        )
        self.cmp.do.bulk_write(
            [
                pymongo.UpdateOne({'a': 1}, {'$set': {'b': 10}}),
                pymongo.DeleteOne({'a': 2}),
                pymongo.ReplaceOne({'a': 3}, {'a': 3, 'b': 30}),
                pymongo.UpdateMany({'a': 4}, {'$set': {'b': 40}}),
            ]
        )
        self.cmp.compare.find(sort=[('a', 1)])


@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class CollectionComparisonTest(TestCase):
    def setUp(self):
        super().setUp()
        self.fake_client = mongomock.MongoClient()
        self.real_client = pymongo.MongoClient(
            host=os.environ.get('TEST_MONGO_HOST', 'mongodb://127.0.0.1:27017/mock?replicaSet=rs0')
        )
        self.db_name = 'mongomock_ng___testing_db'
        self.collection_name = 'mongomock_ng___testing_collection'
        self.real_client[self.db_name][self.collection_name].drop()
        self.cmp = MultiCollection(
            {
                'fake': self.fake_client[self.db_name][self.collection_name],
                'real': self.real_client[self.db_name][self.collection_name],
            }
        )

    def test__find_all(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 2, 'b': 2}])
        self.cmp.compare.find(sort=[('a', 1)])

    def test__find_with_filter(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 2, 'b': 2}])
        self.cmp.compare.find({'a': 1})

    def test__insert_and_find(self):
        self.cmp.do.insert_one({'a': 1, 'b': 3})
        self.cmp.do.insert_one({'a': 2, 'c': 1})
        self.cmp.do.insert_one({'a': 2, 'c': 2})
        self.cmp.compare.find(sort=[('a', 1), ('b', 1), ('c', 1)])

    def test__update_and_find(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 1, 'b': 2}])
        self.cmp.do.update_one({'a': 1}, {'$set': {'b': 10}})
        self.cmp.compare.find({'a': 1}, sort=[('b', 1)])

    def test__delete_and_find(self):
        self.cmp.do.insert_many([{'a': 1, 'b': 1}, {'a': 2, 'b': 2}])
        self.cmp.do.delete_one({'a': 1})
        self.cmp.compare.find(sort=[('a', 1)])


@skipIf(version.parse('4.11') > helpers.PYMONGO_VERSION, 'pymongo v4.11 or above required')
class BulkOperationsWithSortTest(TestCase):
    def setUp(self):
        super().setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['test_db']
        self.collection = self.db['test_collection']
        self.collection.drop()
        self.collection.insert_many(
            [
                {'name': 'Alice', 'age': 25, 'score': 100},
                {'name': 'Bob', 'age': 30, 'score': 80},
                {'name': 'Charlie', 'age': 25, 'score': 90},
                {'name': 'David', 'age': 25, 'score': 95},
            ]
        )

    def test_bulk_update_one_with_sort(self):
        bulk_ops = [
            pymongo.UpdateOne({'age': 25}, {'$set': {'status': 'young'}}, sort=[('score', -1)])
        ]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.matched_count, 1)
        updated_docs = list(self.collection.find({'status': 'young'}))
        self.assertEqual(len(updated_docs), 1)
        self.assertEqual(updated_docs[0]['name'], 'Alice')
        self.assertEqual(updated_docs[0]['score'], 100)

    def test_bulk_update_many_without_sort(self):
        bulk_ops = [pymongo.UpdateMany({'age': 25}, {'$set': {'status': 'young'}})]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 3)
        self.assertEqual(result.matched_count, 3)
        updated_docs = list(self.collection.find({'status': 'young'}))
        self.assertEqual(len(updated_docs), 3)

    def test_bulk_replace_one_with_sort(self):
        bulk_ops = [
            pymongo.ReplaceOne(
                {'age': 25},
                {'name': 'Updated', 'age': 25, 'score': 999, 'status': 'replaced'},
                sort=[('score', -1)],
            )
        ]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.matched_count, 1)
        replaced_docs = list(self.collection.find({'status': 'replaced'}))
        self.assertEqual(len(replaced_docs), 1)
        self.assertEqual(replaced_docs[0]['name'], 'Updated')
        self.assertEqual(replaced_docs[0]['score'], 999)

    def test_bulk_update_without_sort(self):
        bulk_ops = [pymongo.UpdateOne({'age': 25}, {'$set': {'status': 'young'}})]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 1)
        bulk_ops = [pymongo.UpdateMany({'age': 30}, {'$set': {'status': 'adult'}})]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 1)
        young_docs = list(self.collection.find({'status': 'young'}))
        adult_docs = list(self.collection.find({'status': 'adult'}))
        self.assertEqual(len(young_docs), 1)
        self.assertEqual(len(adult_docs), 1)
        self.assertEqual(adult_docs[0]['name'], 'Bob')

    def test_bulk_update_with_complex_sort(self):
        self.collection.drop()
        self.collection.insert_many(
            [
                {'name': 'Eve', 'age': 25, 'score': 100, 'priority': 1},
                {'name': 'Frank', 'age': 25, 'score': 100, 'priority': 2},
                {'name': 'Grace', 'age': 25, 'score': 90, 'priority': 1},
            ]
        )
        bulk_ops = [
            pymongo.UpdateOne(
                {'age': 25, 'score': 100},
                {'$set': {'status': 'top_priority'}},
                sort=[('score', -1), ('priority', 1)],
            )
        ]
        result = self.collection.bulk_write(bulk_ops)
        self.assertEqual(result.modified_count, 1)
        updated_docs = list(self.collection.find({'status': 'top_priority'}))
        self.assertEqual(len(updated_docs), 1)
        self.assertEqual(updated_docs[0]['name'], 'Eve')
        self.assertEqual(updated_docs[0]['priority'], 1)


@skipIf(pymongo is None, 'pymongo not installed')
class BulkWriteUpsertedIdsTest(TestCase):
    def setUp(self):
        self.client = mongomock.MongoClient()
        self.db = self.client.testdb
        self.collection = self.db.collection

    def test__bulk_write_upserted_ids_index(self):
        self.collection.create_index('meter_id', unique=True)
        self.collection.insert_many(
            [
                {'meter_id': 'AAA', 'value': 1},
                {'meter_id': 'BBB', 'value': 2},
            ]
        )

        result = self.collection.bulk_write(
            [
                pymongo.UpdateOne({'meter_id': 'AAA'}, {'$set': {'value': 10}}),
                pymongo.UpdateOne({'meter_id': 'BBB'}, {'$set': {'value': 20}}),
                pymongo.UpdateOne({'meter_id': 'CCC'}, {'$set': {'value': 30}}, upsert=True),
                pymongo.UpdateOne({'meter_id': 'DDD'}, {'$set': {'value': 40}}, upsert=True),
            ],
            ordered=False,
        )

        self.assertEqual(result.upserted_count, 2)
        self.assertIn(2, result.upserted_ids)
        self.assertIn(3, result.upserted_ids)
        self.assertNotIn(0, result.upserted_ids)
        self.assertNotIn(1, result.upserted_ids)
