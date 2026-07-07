import os
import unittest
from unittest import skipIf

import mongomock_ng as mongomock
from mongomock_ng.session import ClientSession


try:
    import pymongo
    from pymongo import InsertOne
    from pymongo import MongoClient as PymongoClient
    from pymongo import UpdateOne

    HAVE_PYMONGO = True
except ImportError:
    HAVE_PYMONGO = False


class SessionBasicTests(unittest.TestCase):
    def test_start_session_returns_client_session(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        self.assertIsInstance(session, ClientSession)
        session.end_session()

    def test_session_context_manager(self):
        client = mongomock.MongoClient()
        with client.start_session() as session:
            self.assertFalse(session.has_ended)
        self.assertTrue(session.has_ended)

    def test_session_client_property(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        self.assertIs(session.client, client)
        session.end_session()

    def test_session_id_property(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        self.assertIn('id', session.session_id)
        session.end_session()

    def test_session_options(self):
        client = mongomock.MongoClient()
        session = client.start_session(causal_consistency=False)
        self.assertFalse(session.options.causal_consistency)
        session.end_session()

    def test_end_session_aborts_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        session.start_transaction()
        collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
        session.end_session()

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'original')


class TransactionTests(unittest.TestCase):
    def test_start_transaction_context_manager(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        with session.start_transaction():
            self.assertTrue(session.in_transaction)
        self.assertFalse(session.in_transaction)
        session.end_session()

    def test_double_start_transaction_raises(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        session.start_transaction()
        with self.assertRaises(mongomock.InvalidOperation):
            session.start_transaction()
        session.abort_transaction()
        session.end_session()

    def test_commit_without_transaction_raises(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        with self.assertRaises(mongomock.InvalidOperation):
            session.commit_transaction()
        session.end_session()

    def test_abort_without_transaction_raises(self):
        client = mongomock.MongoClient()
        session = client.start_session()
        with self.assertRaises(mongomock.InvalidOperation):
            session.abort_transaction()
        session.end_session()

    def test_aborted_transaction_does_not_persist_insert(self):
        client = mongomock.MongoClient()
        collection = client.db.collection

        session = client.start_session()
        with session.start_transaction():
            collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
            session.abort_transaction()

        self.assertIsNone(collection.find_one({'_id': 1}))
        session.end_session()

    def test_aborted_transaction_does_not_persist_update(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
            session.abort_transaction()

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'original')
        session.end_session()

    def test_aborted_transaction_does_not_persist_delete(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'test'})

        session = client.start_session()
        with session.start_transaction():
            collection.delete_one({'_id': 1}, session=session)
            session.abort_transaction()

        self.assertIsNotNone(collection.find_one({'_id': 1}))
        session.end_session()

    def test_committed_transaction_persists_insert(self):
        client = mongomock.MongoClient()
        collection = client.db.collection

        session = client.start_session()
        with session.start_transaction():
            collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
            session.commit_transaction()

        self.assertIsNotNone(collection.find_one({'_id': 1}))
        session.end_session()

    def test_committed_transaction_persists_update(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
            session.commit_transaction()

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'modified')
        session.end_session()

    def test_committed_transaction_persists_delete(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'test'})

        session = client.start_session()
        with session.start_transaction():
            collection.delete_one({'_id': 1}, session=session)
            session.commit_transaction()

        self.assertIsNone(collection.find_one({'_id': 1}))
        session.end_session()

    def test_transaction_context_manager_aborts_on_exception(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        try:
            with session.start_transaction():
                collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
                raise ValueError('test error')
        except ValueError:
            pass

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'original')
        session.end_session()

    def test_transaction_context_manager_commits_on_clean_exit(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'modified')
        session.end_session()

    def test_find_one_and_update_in_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            result = collection.find_one_and_update(
                {'_id': 1}, {'$set': {'value': 'modified'}}, session=session
            )
            self.assertEqual(result['value'], 'original')
            session.commit_transaction()

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'modified')
        session.end_session()

    def test_find_one_and_delete_in_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'test'})

        session = client.start_session()
        with session.start_transaction():
            result = collection.find_one_and_delete({'_id': 1}, session=session)
            self.assertIsNotNone(result)
            session.commit_transaction()

        self.assertIsNone(collection.find_one({'_id': 1}))
        session.end_session()

    def test_bulk_write_in_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            collection.bulk_write(
                [
                    InsertOne({'_id': 2, 'value': 'new'}),
                    UpdateOne({'_id': 1}, {'$set': {'value': 'updated'}}),
                ],
                session=session,
            )
            session.commit_transaction()

        self.assertIsNotNone(collection.find_one({'_id': 2}))
        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'updated')
        session.end_session()

    def test_with_transaction_callback(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        def callback(session):
            collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
            return 'success'

        session = client.start_session()
        result = session.with_transaction(callback)
        self.assertEqual(result, 'success')

        doc = collection.find_one({'_id': 1})
        self.assertEqual(doc['value'], 'modified')
        session.end_session()

    def test_read_your_writes_in_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection

        session = client.start_session()
        with session.start_transaction():
            collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
            doc = collection.find_one({'_id': 1}, session=session)
            self.assertIsNotNone(doc)
            self.assertEqual(doc['value'], 'test')
            session.commit_transaction()
        session.end_session()

    def test_isolation_within_transaction(self):
        client = mongomock.MongoClient()
        collection = client.db.collection
        collection.insert_one({'_id': 1, 'value': 'original'})

        session = client.start_session()
        with session.start_transaction():
            collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
            doc_inside = collection.find_one({'_id': 1}, session=session)
            self.assertEqual(doc_inside['value'], 'modified')
            session.commit_transaction()

        doc_after = collection.find_one({'_id': 1})
        self.assertEqual(doc_after['value'], 'modified')
        session.end_session()

    def test_independent_clients_isolated_storage(self):
        client1 = mongomock.MongoClient()
        client2 = mongomock.MongoClient()

        collection1 = client1.db.collection
        collection2 = client2.db.collection

        collection1.insert_one({'_id': 1, 'value': 'client1'})

        self.assertIsNone(collection2.find_one({'_id': 1}))

        session1 = client1.start_session()
        with session1.start_transaction():
            collection1.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session1)
            session1.commit_transaction()

        self.assertIsNone(collection2.find_one({'_id': 1}))
        session1.end_session()


@skipIf(not HAVE_PYMONGO, 'pymongo not installed')
@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class SessionComparisonTests(unittest.TestCase):
    def setUp(self):
        try:
            self.fake_conn = mongomock.MongoClient()
            self.mongo_conn = self._connect_to_local_mongodb()
            self.db_name = 'mongomock___session_test_db'
            self.collection_name = 'mongomock___session_test_collection'
            self.mongo_conn.drop_database(self.db_name)
            self.mongo_collection = self.mongo_conn[self.db_name][self.collection_name]
            self.fake_collection = self.fake_conn[self.db_name][self.collection_name]
            self._transactions_supported = self._check_transactions_supported()
        except Exception:
            self.skipTest('MongoDB not available')

    def _check_transactions_supported(self):
        try:
            session = self.mongo_conn.start_session()
            session.start_transaction()
            session.abort_transaction()
            session.end_session()
            return True
        except pymongo.errors.OperationFailure:
            return False

    def _skip_if_no_transactions(self):
        if not self._transactions_supported:
            self.skipTest('Transactions not supported (standalone MongoDB)')

    def _connect_to_local_mongodb(self, num_retries=60):
        import time

        for retry in range(num_retries):
            if retry > 0:
                time.sleep(0.5)
            try:
                return PymongoClient(
                    host=os.environ.get('TEST_MONGO_HOST', 'localhost'), maxPoolSize=1
                )
            except pymongo.errors.ConnectionFailure as e:
                if retry == num_retries - 1:
                    raise
                if 'connection refused' not in e.message.lower():
                    raise

    def tearDown(self):
        self.mongo_conn.drop_database(self.db_name)
        self.mongo_conn.close()

    def test_start_session_api(self):
        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        self.assertFalse(fake_session.has_ended)
        self.assertFalse(mongo_session.has_ended)

        fake_session.end_session()
        mongo_session.end_session()

        self.assertTrue(fake_session.has_ended)
        self.assertTrue(mongo_session.has_ended)

    def test_transaction_commit_insert(self):
        self._skip_if_no_transactions()
        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.insert_one({'_id': 1, 'value': 'test'}, session=fake_session)
            fake_session.commit_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.insert_one({'_id': 1, 'value': 'test'}, session=mongo_session)
            mongo_session.commit_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertEqual(fake_doc['value'], mongo_doc['value'])

        fake_session.end_session()
        mongo_session.end_session()

    def test_transaction_abort_insert(self):
        self._skip_if_no_transactions()
        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.insert_one({'_id': 1, 'value': 'test'}, session=fake_session)
            fake_session.abort_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.insert_one({'_id': 1, 'value': 'test'}, session=mongo_session)
            mongo_session.abort_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertIsNone(fake_doc)
        self.assertIsNone(mongo_doc)

        fake_session.end_session()
        mongo_session.end_session()

    def test_transaction_commit_update(self):
        self._skip_if_no_transactions()
        self.fake_collection.insert_one({'_id': 1, 'value': 'original'})
        self.mongo_collection.insert_one({'_id': 1, 'value': 'original'})

        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.update_one(
                {'_id': 1}, {'$set': {'value': 'modified'}}, session=fake_session
            )
            fake_session.commit_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.update_one(
                {'_id': 1}, {'$set': {'value': 'modified'}}, session=mongo_session
            )
            mongo_session.commit_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertEqual(fake_doc['value'], mongo_doc['value'])

        fake_session.end_session()
        mongo_session.end_session()

    def test_transaction_abort_update(self):
        self._skip_if_no_transactions()
        self.fake_collection.insert_one({'_id': 1, 'value': 'original'})
        self.mongo_collection.insert_one({'_id': 1, 'value': 'original'})

        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.update_one(
                {'_id': 1}, {'$set': {'value': 'modified'}}, session=fake_session
            )
            fake_session.abort_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.update_one(
                {'_id': 1}, {'$set': {'value': 'modified'}}, session=mongo_session
            )
            mongo_session.abort_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertEqual(fake_doc['value'], 'original')
        self.assertEqual(mongo_doc['value'], 'original')

        fake_session.end_session()
        mongo_session.end_session()

    def test_transaction_commit_delete(self):
        self._skip_if_no_transactions()
        self.fake_collection.insert_one({'_id': 1, 'value': 'test'})
        self.mongo_collection.insert_one({'_id': 1, 'value': 'test'})

        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.delete_one({'_id': 1}, session=fake_session)
            fake_session.commit_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.delete_one({'_id': 1}, session=mongo_session)
            mongo_session.commit_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertIsNone(fake_doc)
        self.assertIsNone(mongo_doc)

        fake_session.end_session()
        mongo_session.end_session()

    def test_transaction_abort_delete(self):
        self._skip_if_no_transactions()
        self.fake_collection.insert_one({'_id': 1, 'value': 'test'})
        self.mongo_collection.insert_one({'_id': 1, 'value': 'test'})

        fake_session = self.fake_conn.start_session()
        mongo_session = self.mongo_conn.start_session()

        with fake_session.start_transaction():
            self.fake_collection.delete_one({'_id': 1}, session=fake_session)
            fake_session.abort_transaction()

        with mongo_session.start_transaction():
            self.mongo_collection.delete_one({'_id': 1}, session=mongo_session)
            mongo_session.abort_transaction()

        fake_doc = self.fake_collection.find_one({'_id': 1})
        mongo_doc = self.mongo_collection.find_one({'_id': 1})

        self.assertIsNotNone(fake_doc)
        self.assertIsNotNone(mongo_doc)

        fake_session.end_session()
        mongo_session.end_session()


if __name__ == '__main__':
    unittest.main()
