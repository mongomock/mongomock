import warnings

from . import CollectionInvalid
from . import InvalidName
from . import OperationFailure
from .collection import Collection
from mongomock import read_preferences
from mongomock import store

from six import string_types

try:
    from pymongo import ReadPreference
    _READ_PREFERENCE_PRIMARY = ReadPreference.PRIMARY
except ImportError:
    _READ_PREFERENCE_PRIMARY = read_preferences.PRIMARY


class Database(object):

    def __init__(self, client, name, _store, read_preference=None):
        self.name = name
        self._client = client
        self._collection_accesses = {}
        self._store = _store or store.DatabaseStore()
        self._read_preference = read_preference or _READ_PREFERENCE_PRIMARY

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

    @property
    def read_preference(self):
        return self._read_preference

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
        if read_preference is not None:
            read_preferences.ensure_read_preference_type('read_preference', read_preference)
        try:
            return self._collection_accesses[name]
        except KeyError:
            collection = self._collection_accesses[name] = Collection(
                self, name=name, write_concern=write_concern,
                read_preference=read_preference or self.read_preference,
                _db_store=self._store)
            return collection

    def drop_collection(self, name_or_collection, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')
        if isinstance(name_or_collection, Collection):
            name_or_collection._store.drop()
        else:
            self._store[name_or_collection].drop()

    def _ensure_valid_collection_name(self, name):
        # These are the same checks that are done in pymongo.
        if not isinstance(name, string_types):
            raise TypeError('name must be an instance of basestring')
        if not name or '..' in name:
            raise InvalidName('collection names cannot be empty')
        if name[0] == '.' or name[-1] == '.':
            raise InvalidName("collection names must not start or end with '.'")
        if '$' in name:
            raise InvalidName("collection names must not contain '$'")
        if '\x00' in name:
            raise InvalidName('collection names must not contain the null character')

    def create_collection(self, name, **kwargs):
        self._ensure_valid_collection_name(name)
        if name in self.list_collection_names():
            raise CollectionInvalid('collection %s already exists' % name)

        if kwargs:
            raise NotImplementedError('Special options not supported')

        self._store.create_collection(name)
        return self[name]

    def rename_collection(self, name, new_name, dropTarget=False):
        """Changes the name of an existing collection."""
        self._ensure_valid_collection_name(new_name)

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
        return {'ok': 1}

    def dereference(self, dbref, session=None):
        if session:
            raise NotImplementedError('Mongomock does not handle sessions yet')

        if not hasattr(dbref, 'collection') or not hasattr(dbref, 'id'):
            raise TypeError('cannot dereference a %s' % type(dbref))
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError('trying to dereference a DBRef that points to '
                             'another database (%r not %r)' % (dbref.database,
                                                               self.name))
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
