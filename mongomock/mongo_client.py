from .database import Database
from .helpers import parse_dbase_from_uri
import itertools
from mongomock import ConfigurationError


class MongoClient(object):

    HOST = 'localhost'
    PORT = 27017
    _CONNECTION_ID = itertools.count()

    def __init__(self, host=None, port=None, document_class=dict,
                 tz_aware=False, connect=True, **kwargs):
        if host:
            self.host = host[0] if isinstance(host, (list, tuple)) else host
        else:
            self.host = self.HOST
        self.port = port or self.PORT
        self._databases = {}
        self._id = next(self._CONNECTION_ID)
        self._document_class = document_class

        dbase = None

        if "://" in self.host:
            dbase = parse_dbase_from_uri(self.host)

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

    def server_info(self):
        return {
            "version": "3.0.0",
            "sysInfo": "Mock",
            "versionArray": [3, 0, 0, 0],
            "bits": 64,
            "debug": False,
            "maxBsonObjectSize": 16777216,
            "ok": 1
        }

    def database_names(self):
        return list(self._databases.keys())

    def drop_database(self, name_or_db):

        def drop_collections_for_db(_db):
            for col_name in _db.collection_names():
                _db.drop_collection(col_name)

        if isinstance(name_or_db, Database):
            db = next(db for db in self._databases.values() if db is name_or_db)
            if db:
                drop_collections_for_db(db)

        elif name_or_db in self._databases:
            db = self._databases[name_or_db]
            drop_collections_for_db(db)

    def get_database(self, name=None, codec_options=None, read_preference=None,
                     write_concern=None):
        if name is None:
            return self.get_default_database()

        db = self._databases.get(name)
        if db is None:
            db = self._databases[name] = Database(self, name)
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
