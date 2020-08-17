import collections
import datetime
import six
import threading

lock = threading.RLock()


class ServerStore(object):
    """Object holding the data for a whole server (many databases)."""

    def __init__(self):
        self._databases = {}

    def __getitem__(self, db_name):
        try:
            return self._databases[db_name]
        except KeyError:
            db = self._databases[db_name] = DatabaseStore()
            return db

    def __contains__(self, db_name):
        return self[db_name].is_created

    def list_created_database_names(self):
        return [name for name, db in self._databases.items() if db.is_created]


class DatabaseStore(object):
    """Object holding the data for a database (many collections)."""

    def __init__(self):
        self._collections = {}

    def __getitem__(self, col_name):
        try:
            return self._collections[col_name]
        except KeyError:
            col = self._collections[col_name] = CollectionStore(col_name)
            return col

    def __contains__(self, col_name):
        return self[col_name].is_created

    def list_created_collection_names(self):
        return [name for name, col in self._collections.items() if col.is_created]

    def create_collection(self, name):
        col = self[name]
        col.create()
        return col

    def rename(self, name, new_name):
        col = self._collections.pop(name, CollectionStore(new_name))
        col.name = new_name
        self._collections[new_name] = col

    @property
    def is_created(self):
        return any(col.is_created for col in self._collections.values())


class CollectionStore(object):
    """Object holding the data for a collection."""

    def __init__(self, name):
        self._documents = collections.OrderedDict()
        self.indexes = {}
        self._is_force_created = False
        self.name = name

    def create(self):
        self._is_force_created = True

    @property
    def is_created(self):
        return self._documents or self.indexes or self._is_force_created

    def drop(self):
        self._documents = collections.OrderedDict()
        self.indexes = {}
        self._is_force_created = False

    @property
    def is_empty(self):
        return not self._documents

    def __contains__(self, key):
        return key in self._documents

    def __getitem__(self, key):
        return self._documents[key]

    def __setitem__(self, key, val):
        with lock:
            self._documents[key] = val

    def __delitem__(self, key):
        del self._documents[key]

    def __len__(self):
        return len(self._documents)

    @property
    def documents(self):
        for doc in six.itervalues(self._documents):
            yield doc

    def remove_expired_documents(self):
        for index in six.itervalues(self.indexes):
            if index.get('expireAfterSeconds') is not None:
                self._expire_documents(index)

    def _expire_documents(self, index):
        # Ignore non-integer values
        try:
            expiry = int(index['expireAfterSeconds'])
        except ValueError:
            return

        # Ignore commpound keys
        if len(index['key']) > 1:
            return

        field = index['key'][0][0]
        expired_ids = {
            doc['_id'] for doc in six.itervalues(self._documents) if self._doc_expired(doc, field, expiry)
        }

        for exp_id in expired_ids:
            del self[exp_id]

    @staticmethod
    def _doc_expired(doc, field, expiry):
        return (datetime.datetime.now() - doc.get(field, datetime.datetime.max)).total_seconds() >= expiry
