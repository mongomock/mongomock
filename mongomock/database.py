import warnings

from . import CollectionInvalid
from . import InvalidName
from . import OperationFailure
from .collection import Collection
from mongomock import store

from six import string_types


class Database(object):

    def __init__(self, client, name, _store):
        self.name = name
        self._client = client
        self._collection_accesses = {}
        self._store = _store or store.DatabaseStore()

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
        return self._store.list_created_collection_names()

    def collection_names(self, include_system_collections=True, session=None):
        warnings.warn('collection_names is deprecated. Use list_collection_names instead.')
        if include_system_collections:
            return list(self._get_created_collections())

        return self.list_collection_names(session=session)

    def list_collection_names(self, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')

        return [
            name for name in self._get_created_collections()
            if not name.startswith('system.')
        ]

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None, read_concern=None):
        if read_concern:
            raise NotImplementedError('Mongomock does not handle read_concern yet')
        try:
            return self._collection_accesses[name]
        except KeyError:
            collection = self._collection_accesses[name] = \
                Collection(self, write_concern=write_concern, _store=self._store[name])
            return collection

    def drop_collection(self, name_or_collection, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')
        if isinstance(name_or_collection, Collection):
            name_or_collection._store.drop()
        else:
            self._store[name_or_collection].drop()

    def create_collection(self, name, **kwargs):
        if name in self.list_collection_names():
            raise CollectionInvalid('collection %s already exists' % name)
        if not name or '..' in name:
            raise InvalidName('collection names cannot be empty')

        if kwargs:
            raise NotImplementedError('Special options not supported')

        self._store.create_collection(name)
        return self[name]

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
        if not self._store[name].is_created:
            raise OperationFailure(
                'The collection "{0}" does not exist.'.format(name), 10026)
        if new_name in self._store:
            if dropTarget:
                self.drop_collection(new_name)
            else:
                raise OperationFailure(
                    'The target collection "{0}" already exists'.format(new_name),
                    10027)
        self._store.rename(name, new_name)
        collection = self._collection_accesses.pop(name)
        self._collection_accesses[new_name] = collection

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

    def command(self, command, **unused_kwargs):
        if isinstance(command, string_types):
            command = {command: 1}
        if 'ping' in command:
            return {'ok': 1.}
        # TODO(pascal): Differentiate NotImplementedError for valid commands
        # and OperationFailure if the command is not valid.
        raise NotImplementedError(
            'command is a valid Database method but is not implemented in Mongomock yet')
