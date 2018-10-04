import warnings

from collections import OrderedDict

from . import CollectionInvalid
from . import InvalidName
from . import OperationFailure
from .collection import Collection

from six import string_types


class Database(object):

    def __init__(self, client, name):
        self.name = name
        self._client = client
        self._collections = {}

    def __getitem__(self, coll_name):
        return self.get_collection(coll_name)

    def __getattr__(self, attr):
        if attr.startswith('_'):
            raise AttributeError(
                "%s has no attribute '%s'. To access the %s collection, use database['%s']." %
                (self.__class__.__name__, attr, attr, attr))
        return self[attr]

    def __repr__(self):
        return "Database({0}, '{1}')".format(self._client, self.name)

    @property
    def client(self):
        return self._client

    def _get_created_collections(self):
        return [name for name, col in self._collections.items() if col._is_created()]

    def collection_names(self, include_system_collections=True, session=None):
        warnings.warn('collection_names is deprecated. Use list_collection_names instead.')
        if include_system_collections:
            return list(self._get_created_collections())

        return self.list_collection_names(session=session)

    def list_collection_names(self, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')

        result = []
        for name in self._get_created_collections():
            if not name.startswith('system.'):
                result.append(name)

        return result

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None, read_concern=None):
        if read_concern:
            raise NotImplementedError('Mongomock does not handle read_concern yet')
        collection = self._collections.get(name)
        if collection is None:
            collection = self._collections[name] = \
                Collection(self, name, write_concern=write_concern)
        return collection

    def drop_collection(self, name_or_collection, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')
        try:
            if isinstance(name_or_collection, Collection):
                collection = next(c for c in self._collections.values() if c is name_or_collection)
                collection._documents = OrderedDict()
                collection._force_created = False
                collection.drop_indexes()
            else:
                if name_or_collection in self._collections:
                    collection = self._collections.get(name_or_collection)
                    if collection:
                        collection._documents = OrderedDict()
                        collection._force_created = False
                        collection.drop_indexes()
        # EAFP paradigm
        # (http://en.m.wikipedia.org/wiki/Python_syntax_and_semantics)
        except Exception:
            pass

    def create_collection(self, name, **kwargs):
        if name in self.list_collection_names():
            raise CollectionInvalid('collection %s already exists' % name)
        if not name or '..' in name:
            raise InvalidName('collection names cannot be empty')

        if kwargs:
            raise NotImplementedError('Special options not supported')

        col = self[name]
        col._force_created = True
        return col

    def rename_collection(self, name, new_name, dropTarget=False):
        """Changes the name of an existing collection."""
        # These are the same checks that are done in pymongo.
        if not isinstance(new_name, string_types):
            raise TypeError('new_name must be an instance of basestring')
        if new_name[0] == '.' or new_name[-1] == '.':
            raise InvalidName("collection names must not start or end with '.'")
        if '$' in new_name:
            raise InvalidName("collection names must not contain '$'")

        # Reference for server implementation:
        # https://docs.mongodb.com/manual/reference/command/renameCollection/
        if name not in self._collections:
            raise OperationFailure(
                'The collection "{0}" does not exist.'.format(name), 10026)
        if new_name in self._collections:
            if dropTarget:
                self.drop_collection(new_name)
            else:
                raise OperationFailure(
                    'The target collection "{0}" already exists'.format(new_name),
                    10027)
        collection = self._collections.pop(name)
        collection.name = new_name
        self._collections[new_name] = collection

    def dereference(self, dbref, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')

        if not dbref.collection or not dbref.id:
            raise TypeError('cannot dereference a %s' % type(dbref))
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError('trying to dereference a DBRef that points to '
                             'another database (%r not %r)' % (dbref.database,
                                                               self.__name))
        return self[dbref.collection].find_one({'_id': dbref.id})
