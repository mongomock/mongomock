import itertools
from .database import Database

class Connection(object):

    _CONNECTION_ID = itertools.count()

    def __init__(self, host = None, port = None, max_pool_size = 10,
                 network_timeout = None, document_class = dict,
                 tz_aware = False, _connect = True, **kwargs):
        super(Connection, self).__init__()
        self.host = host
        self.port = port
        self._databases = {}
        self._id = next(self._CONNECTION_ID)
        self.document_class = document_class

    def __getitem__(self, db_name):
        db = self._databases.get(db_name, None)
        if db is None:
            db = self._databases[db_name] = Database(self, db_name)
        return db
    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        identifier = []
        host = getattr(self,'host','')
        port = getattr(self,'port',None)
        if host is not None:
            identifier = ["'{0}'".format(host)]
            if port is not None:
                identifier.append(str(port))
        return "mongomock.Connection({0})".format(', '.join(identifier))

    def server_info(self):
        return {
            "version" : "2.0.6",
            "sysInfo" : "Mock",
            "versionArray" : [
                              2,
                              0,
                              6,
                              0
                              ],
            "bits" : 64,
            "debug" : False,
            "maxBsonObjectSize" : 16777216,
            "ok" : 1
    }

#Connection is now depricated, it's called MongoClient instead
class MongoClient(Connection):
    def stub(self):
        pass
