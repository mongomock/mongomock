from .__version__ import __version__
from .object_id import ObjectId
from sentinels import NOTHING

__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']

def resolve_key_value(key, doc):
    """Resolve keys to their proper value in a document.
    Returns the appropriate nested value if the key includes dot notation.
    """
    if not doc or not isinstance(doc, dict):
        return NOTHING
    else:
        key_parts = key.split('.')
        if len(key_parts) == 1:
            return doc.get(key, NOTHING)
        else:
            sub_key = '.'.join(key_parts[1:])
            sub_doc = doc.get(key_parts[0], {})
            return resolve_key_value(sub_key, sub_doc)

class Connection(object):
    def __init__(self):
        super(Connection, self).__init__()
        self._databases = {}
    def __getitem__(self, db_name):
        db = self._databases.get(db_name, None)
        if db is None:
            db = self._databases[db_name] = Database(self)
        return db
    def __getattr__(self, attr):
        return self[attr]

class Database(object):
    def __init__(self, conn):
        super(Database, self).__init__()
        self._collections = {'system.indexes' : Collection(self)}
    def __getitem__(self, db_name):
        db = self._collections.get(db_name, None)
        if db is None:
            db = self._collections[db_name] = Collection(self)
        return db
    def __getattr__(self, attr):
        return self[attr]
    def collection_names(self):
        return list(self._collections.keys())

class Collection(object):
    def __init__(self, db):
        super(Collection, self).__init__()
        self._documents = {}
    def insert(self, data):
        if isinstance(data, list):
            return [self._insert(element) for element in data]
        return self._insert(data)
    def _insert(self, data):
        object_id = ObjectId()
        assert object_id not in self._documents
        self._documents[object_id] = dict(data, _id=object_id)
        return object_id
    def update(self, spec, document):
        for existing_document in self._iter_documents(spec):
            document_id = existing_document['_id']
            existing_document.clear()
            existing_document.update(document)
            existing_document['_id'] = document_id
    def find(self, filter=None):
        dataset = (document.copy() for document in self._iter_documents(filter))
        return Cursor(dataset)
    def _iter_documents(self, filter=None):
        return (document for document in self._documents.itervalues() if self._filter_applies(filter, document))
    def find_one(self, filter=None):
        try:
            return next(self.find(filter))
        except StopIteration:
            return None
    def _filter_applies(self, search_filter, document):
        """Returns a boolean indicating whether @search_filter applies
        to @document.
        """
        if search_filter is None:
            return True

        for key, search_value in search_filter.iteritems():
            document_value = resolve_key_value(key, document)
            if document_value != search_value:
                return False
        return True

class Cursor(object):
    def __init__(self, dataset):
        super(Cursor, self).__init__()
        self._dataset = dataset
    def __iter__(self):
        return self
    def next(self):
        return next(self._dataset)
