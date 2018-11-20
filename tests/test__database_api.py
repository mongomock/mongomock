from unittest import TestCase

import mongomock


class DatabaseAPITest(TestCase):

    def test__get_collection_by_attribute_underscore(self):
        client = mongomock.MongoClient()
        database = client['somedb']

        with self.assertRaises(AttributeError) as err_context:
            database._users  # pylint: disable=pointless-statement

        self.assertIn("Database has no attribute '_users'", str(err_context.exception))

        # No problem accessing it through __get_item__.
        database['_users'].insert_one({'a': 1})
        self.assertEqual(1, database['_users'].find_one().get('a'))

    def test__command_ping(self):
        database = mongomock.MongoClient().somedb
        self.assertEqual({'ok': 1}, database.command({'ping': 1}))

    def test__command(self):
        database = mongomock.MongoClient().somedb
        with self.assertRaises(NotImplementedError):
            database.command({'count': 'user'})
