from unittest import TestCase

import mongomock


class DatabaseAPITest(TestCase):

    def setUp(self):
        self.database = mongomock.MongoClient().somedb

    def test__get_collection_by_attribute_underscore(self):
        with self.assertRaises(AttributeError) as err_context:
            self.database._users  # pylint: disable=pointless-statement

        self.assertIn("Database has no attribute '_users'", str(err_context.exception))

        # No problem accessing it through __get_item__.
        self.database['_users'].insert_one({'a': 1})
        self.assertEqual(1, self.database['_users'].find_one().get('a'))

    def test__list_collection_names(self):
        self.database.test1.create_index('foo')
        self.assertEqual(['test1'], self.database.list_collection_names())

    def test__command_ping(self):
        self.assertEqual({'ok': 1}, self.database.command({'ping': 1}))

    def test__command_ping_string(self):
        self.assertEqual({'ok': 1}, self.database.command('ping'))

    def test__command_fake_ping_string(self):
        with self.assertRaises(NotImplementedError):
            self.assertEqual({'ok': 1}, self.database.command('a_nice_ping'))

    def test__command(self):
        with self.assertRaises(NotImplementedError):
            self.database.command({'count': 'user'})
