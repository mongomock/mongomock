import collections
from unittest import TestCase, skipIf

import mongomock

try:
    from bson import codec_options
    from pymongo.read_preferences import ReadPreference
    _HAVE_PYMONGO = True
except ImportError:
    _HAVE_PYMONGO = False


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

    def test__collection_names(self):
        self.database.a.create_index('foo')
        self.database['system.bar'].create_index('foo')
        self.assertEqual(['a'], self.database.collection_names(include_system_collections=False))

    def test__list_collection_names(self):
        self.database.test1.create_index('foo')
        self.assertEqual(['test1'], self.database.list_collection_names())

    def test__session(self):
        with self.assertRaises(NotImplementedError):
            self.database.list_collection_names(session=1)
        with self.assertRaises(NotImplementedError):
            self.database.drop_collection('a', session=1)
        with self.assertRaises(NotImplementedError):
            self.database.create_collection('a', session=1)
        with self.assertRaises(NotImplementedError):
            self.database.dereference(_DBRef('somedb', 'a', 'b'), session=1)

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

    def test__repr(self):
        self.assertEqual(
            "Database(mongomock.MongoClient('localhost', 27017), 'somedb')", repr(self.database))

    def test__rename_unknown_collection(self):
        with self.assertRaises(mongomock.OperationFailure):
            self.database.rename_collection('a', 'b')

    def test__dereference(self):
        self.database.a.insert_one({'_id': 'b', 'val': 42})
        doc = self.database.dereference(_DBRef('somedb', 'a', 'b'))
        self.assertEqual({'_id': 'b', 'val': 42}, doc)

        self.assertEqual(None, self.database.dereference(_DBRef('somedb', 'a', 'a')))
        self.assertEqual(None, self.database.dereference(_DBRef('somedb', 'b', 'b')))

        with self.assertRaises(ValueError):
            self.database.dereference(_DBRef('otherdb', 'a', 'b'))

        with self.assertRaises(TypeError):
            self.database.dereference('b')

    def test__get_collection(self):
        with self.assertRaises(NotImplementedError):
            self.database.get_collection('a', read_concern=3)

    def test__read_preference(self):
        self.assertEqual('Primary', self.database.read_preference.name)
        self.assertEqual(self.database.collection.read_preference, self.database.read_preference)

        with self.assertRaises(TypeError):
            self.database.get_collection('a', read_preference='nearest')

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__get_collection_different_read_preference(self):
        database = mongomock.MongoClient()\
            .get_database('somedb', read_preference=ReadPreference.NEAREST)
        self.assertEqual('Nearest', database.read_preference.name)
        self.assertEqual(database.read_preference, database.collection.read_preference)

        col = database.get_collection('col', read_preference=ReadPreference.PRIMARY)
        self.assertEqual('Primary', col.read_preference.name)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__codec_options(self):
        self.assertEqual(codec_options.CodecOptions(), self.database.codec_options)

    @skipIf(_HAVE_PYMONGO, 'pymongo installed')
    def test__codec_options_without_pymongo(self):
        with self.assertRaises(NotImplementedError):
            self.database.codec_options  # pylint: disable=pointless-statement

        with self.assertRaises(NotImplementedError):
            self.database.with_options(codec_options=3)

    def test__with_options(self):
        other = self.database.with_options(read_preference=self.database.read_preference)
        self.assertNotEqual(other, self.database)

        self.database.coll.insert_one({'_id': 42})
        self.assertEqual({'_id': 42}, other.coll.find_one())

        with self.assertRaises(NotImplementedError):
            self.database.with_options(write_concern=3)

        with self.assertRaises(NotImplementedError):
            self.database.with_options(read_concern=3)

    @skipIf(not _HAVE_PYMONGO, 'pymongo not installed')
    def test__with_options_pymongo(self):
        self.database.with_options(codec_options=codec_options.CodecOptions())
        self.database.with_options()

        with self.assertRaises(NotImplementedError):
            self.database.with_options(codec_options=codec_options.CodecOptions(tz_aware=True))


_DBRef = collections.namedtuple('DBRef', ['database', 'collection', 'id'])
