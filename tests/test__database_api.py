from unittest import TestCase

import mongomock


class DatabaseAPITest(TestCase):

    def test__get_collection_by_attribute_underscore(self):
        client = mongomock.MongoClient()
        database = client['somedb']

        with self.assertRaises(AttributeError) as err_context:
            database._users

        self.assertIn("Database has no attribute '_users'", str(err_context.exception))

        # No problem accessing it through __get_item__.
        database['_users'].insert_one({'a': 1})
        self.assertEqual(1, database['_users'].find_one().get('a'))
