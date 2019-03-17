from .database import Database
from .store import ServerStore
import itertools
from mongomock import ConfigurationError
from mongomock import read_preferences
import warnings

try:
    from pymongo.uri_parser import parse_uri, split_hosts
    from pymongo import ReadPreference
    _READ_PREFERENCE_PRIMARY = ReadPreference.PRIMARY
except ImportError:
    from .helpers import parse_uri, split_hosts
    _READ_PREFERENCE_PRIMARY = read_preferences.PRIMARY


class MongoClient(object):

    HOST = 'localhost'
    PORT = 27017
    _CONNECTION_ID = itertools.count()

    def __init__(self, host=None, port=None, document_class=dict,
                 tz_aware=False, connect=True, _store=None, read_preference=None,
                 **kwargs):
        if host:
            self.host = host[0] if isinstance(host, (list, tuple)) else host
        else:
            self.host = self.HOST
        self.port = port or self.PORT

        self._tz_aware = tz_aware
        self._database_accesses = {}
        self._store = _store or ServerStore()
        self._id = next(self._CONNECTION_ID)
        self._document_class = document_class
        if read_preference is not None:
            read_preferences.ensure_read_preference_type('read_preference', read_preference)
        self._read_preference = read_preference or _READ_PREFERENCE_PRIMARY

        dbase = None

        if '://' in self.host:
            res = parse_uri(self.host, default_port=self.port, warn=True)
            self.host, self.port = res['nodelist'][0]
            dbase = res['database']
        else:
            self.host, self.port = split_hosts(self.host, default_port=self.port)[0]

        self.__default_datebase_name = dbase

    def __getitem__(self, db_name):
        return self.get_database(db_name)

    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        return "mongomock.MongoClient('{0}', {1})".format(self.host, self.port)

    def close(self):
        pass

    @property
    def is_mongos(self):
        return True

    @property
    def is_primary(self):
        return True

    @property
    def address(self):
        return self.host, self.port

    @property
    def read_preference(self):
        return self._read_preference

    def server_info(self):
        return {
            'version': '3.0.0',
            'sysInfo': 'Mock',
            'versionArray': [3, 0, 0, 0],
            'bits': 64,
            'debug': False,
            'maxBsonObjectSize': 16777216,
            'ok': 1
        }

    def database_names(self):
        warnings.warn('database_names is deprecated. Use list_database_names instead.')
        return self.list_database_names()

    def list_database_names(self):
        return self._store.list_created_database_names()

    def drop_database(self, name_or_db):

        def drop_collections_for_db(_db):
            for col_name in _db.list_collection_names():
                _db.drop_collection(col_name)

        if isinstance(name_or_db, Database):
            db = next(db for db in self._database_accesses.values() if db is name_or_db)
            if db:
                drop_collections_for_db(db)

        elif name_or_db in self._store:
            db = self.get_database(name_or_db)
            drop_collections_for_db(db)

    def get_database(self, name=None, codec_options=None, read_preference=None,
                     write_concern=None):
        if name is None:
            return self.get_default_database()

        db = self._database_accesses.get(name)
        if db is None:
            db_store = self._store[name]
            db = self._database_accesses[name] = Database(
                self, name, read_preference=read_preference or self.read_preference,
                _store=db_store)
        return db

    def get_default_database(self):
        if self.__default_datebase_name is None:
            raise ConfigurationError('No default database defined')

        return self[self.__default_datebase_name]

    def alive(self):
        """The original MongoConnection.alive method checks the status of the server.

        In our case as we mock the actual server, we should always return True.
        """
        return True
