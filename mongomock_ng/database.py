try:
    import bson
except ImportError:
    bson = None

from mongomock_ng import codec_options as mongomock_codec_options
from mongomock_ng import read_preferences
from mongomock_ng import store

from . import CollectionInvalid
from . import InvalidName
from . import OperationFailure
from .collection import Collection
from .command_cursor import CommandCursor
from .filtering import filter_applies


try:
    from pymongo import ReadPreference

    _READ_PREFERENCE_PRIMARY = ReadPreference.PRIMARY
except ImportError:
    _READ_PREFERENCE_PRIMARY = read_preferences.PRIMARY

try:
    from pymongo.read_concern import ReadConcern
except ImportError:
    from .read_concern import ReadConcern

try:
    from pymongo.write_concern import WriteConcern
except ImportError:
    from .write_concern import WriteConcern

_LIST_COLLECTION_FILTER_ALLOWED_OPERATORS = frozenset(['$regex', '$eq', '$ne'])


def _verify_list_collection_supported_op(keys):
    if set(keys) - _LIST_COLLECTION_FILTER_ALLOWED_OPERATORS:
        raise NotImplementedError(
            f'list collection names filter operator {keys} is not implemented yet in mongomock-ng '
            f'allowed operators are {_LIST_COLLECTION_FILTER_ALLOWED_OPERATORS}'
        )


class Database:
    def __init__(
        self,
        client,
        name,
        _store,
        read_preference=None,
        codec_options=None,
        read_concern=None,
        write_concern=None,
    ):
        self.name = name
        self._client = client
        self._collection_accesses = {}
        self._store = _store or store.DatabaseStore()
        self._read_preference = (
            read_preference if read_preference is not None else _READ_PREFERENCE_PRIMARY
        )
        mongomock_codec_options.is_supported(codec_options)
        self._codec_options = (
            codec_options if codec_options is not None else mongomock_codec_options.CodecOptions()
        )
        if read_concern is not None and not isinstance(read_concern, ReadConcern):
            raise TypeError('read_concern must be an instance of pymongo.read_concern.ReadConcern')
        self._read_concern = read_concern if read_concern is not None else ReadConcern()
        self._write_concern = write_concern if write_concern is not None else WriteConcern()

    def __getitem__(self, coll_name):
        return self.get_collection(coll_name)

    def __getattr__(self, attr):
        if attr.startswith('_'):
            raise AttributeError(
                f"{self.__class__.__name__} has no attribute '{attr}'. To access the {attr} "
                f"collection, use database['{attr}']."
            )
        return self[attr]

    def __repr__(self):
        return f"Database({self._client}, '{self.name}')"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._client == other._client and self.name == other.name
        return NotImplemented

    def __bool__(self):
        raise NotImplementedError(
            'Database objects do not implement truth '
            'value testing or bool(). Please compare '
            'with None instead: database is not None'
        )

    def __iter__(self):
        return self

    def __next__(self):
        raise TypeError("'Database' object is not iterable")

    def __hash__(self):
        return hash((self._client, self.name))

    @property
    def client(self):
        return self._client

    @property
    def read_preference(self):
        return self._read_preference

    @property
    def codec_options(self):
        return self._codec_options

    @property
    def read_concern(self):
        return self._read_concern

    @property
    def write_concern(self):
        return self._write_concern

    def _get_created_collections(self):
        return self._store.list_created_collection_names()

    def list_collections(self, filter=None, session=None, nameOnly=False):  # noqa: N803
        names = self.list_collection_names(filter=filter)
        if nameOnly:
            return CommandCursor([{'name': n} for n in names])
        return CommandCursor(
            [
                {
                    'name': n,
                    'type': 'collection',
                    'options': {},
                    'info': {'readOnly': False},
                }
                for n in names
            ]
        )

    def list_collection_names(self, filter=None, session=None):
        """filter: only name field type with eq,ne or regex operator

        session: not supported
        for supported operator please see _LIST_COLLECTION_FILTER_ALLOWED_OPERATORS
        """
        field_name = 'name'

        if filter:
            if not filter.get('name'):
                raise NotImplementedError(
                    f'list collection {filter} might be valid but is not '
                    'implemented yet in mongomock-ng'
                )

            filter = (
                {field_name: {'$eq': filter.get(field_name)}}
                if isinstance(filter.get(field_name), str)
                else filter
            )

            _verify_list_collection_supported_op(filter.get(field_name).keys())

            return [
                name
                for name in list(self._store._collections)
                if filter_applies(filter, {field_name: name}) and not name.startswith('system.')
            ]

        return [name for name in self._get_created_collections() if not name.startswith('system.')]

    def get_collection(
        self, name, codec_options=None, read_preference=None, write_concern=None, read_concern=None
    ):
        if read_preference is not None:
            read_preferences.ensure_read_preference_type('read_preference', read_preference)
        mongomock_codec_options.is_supported(codec_options)
        try:
            return self._collection_accesses[name].with_options(
                codec_options=codec_options or self._codec_options,
                read_preference=read_preference or self.read_preference,
                read_concern=read_concern,
                write_concern=write_concern,
            )
        except KeyError:
            self._ensure_valid_collection_name(name)
            collection = self._collection_accesses[name] = Collection(
                self,
                name=name,
                read_concern=read_concern,
                write_concern=write_concern,
                read_preference=read_preference or self.read_preference,
                codec_options=codec_options or self._codec_options,
                _db_store=self._store,
            )
            return collection

    def drop_collection(self, name_or_collection, session=None):
        if isinstance(name_or_collection, Collection):
            name_or_collection._store.drop()
        else:
            self._store[name_or_collection].drop()

    def _ensure_valid_collection_name(self, name):
        # These are the same checks that are done in pymongo.
        if not isinstance(name, str):
            raise TypeError('name must be an instance of str')
        if not name or '..' in name:
            raise InvalidName('collection names cannot be empty')
        if name[0] == '.' or name[-1] == '.':
            raise InvalidName("collection names must not start or end with '.'")
        if '$' in name:
            raise InvalidName("collection names must not contain '$'")
        if '\x00' in name:
            raise InvalidName('collection names must not contain the null character')

    def create_collection(self, name, session=None, **kwargs):
        self._ensure_valid_collection_name(name)
        if name in self.list_collection_names():
            raise CollectionInvalid(f'collection {name} already exists')

        self._store.create_collection(name, **kwargs)
        return self[name]

    def rename_collection(self, name, new_name, dropTarget=False):  # noqa: N803
        """Changes the name of an existing collection."""
        self._ensure_valid_collection_name(new_name)

        # Reference for server implementation:
        # https://docs.mongodb.com/manual/reference/command/renameCollection/
        if not self._store[name].is_created:
            raise OperationFailure(f'The collection "{name}" does not exist.', 10026)
        if new_name in self._store:
            if dropTarget:
                self.drop_collection(new_name)
            else:
                raise OperationFailure(f'The target collection "{new_name}" already exists', 10027)
        self._store.rename(name, new_name)
        result = {'ok': 1}
        if bson is not None:
            result['$clusterTime'] = {
                'clusterTime': bson.Timestamp(0, 0),
                'signature': {'hash': b'\x00' * 20, 'keyId': 0},
            }
            result['operationTime'] = bson.Timestamp(0, 0)
        return result

    def dereference(self, dbref, session=None):
        if not hasattr(dbref, 'collection') or not hasattr(dbref, 'id'):
            raise TypeError(f'cannot dereference a {type(dbref)}')
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError(
                'trying to dereference a DBRef that points to '
                f'another database ({dbref.database!r} not {self.name!r})'
            )
        return self[dbref.collection].find_one({'_id': dbref.id})

    def command(
        self,
        command,
        value=1,
        check=True,
        allowable_errors=None,
        read_preference=None,
        codec_options=None,
        session=None,
        **kwargs,
    ):
        if isinstance(command, str):
            command = {command: value}
        command.update(kwargs)
        if 'ping' in command:
            return {'ok': 1.0}
        if 'collMod' in command:
            return self._command_collmod(command)
        if 'ismaster' in command or 'isMaster' in command:
            _host, _port = self.client.address
            return {
                'ismaster': True,
                'secondary': False,
                'ok': 1.0,
            }
        raise NotImplementedError(
            'command is a valid Database method but is not implemented in Mongomock-ng yet'
        )

    def _command_collmod(self, command):
        validation_opts = {'validator', 'validationLevel', 'validationAction'}
        validation_defaults = {'validationLevel': 'strict', 'validationAction': 'error'}

        coll_name = command.pop('collMod')
        if coll_name not in self._store:
            raise OperationFailure('ns does not exist', code=26)

        coll_store = self._store[coll_name]
        for opt in command:
            if opt not in coll_store._supported_options:
                raise NotImplementedError(
                    'setting the {} option with collMod is not supported '
                    'in Mongomock-ng yet; supported options are: {}'.format(
                        opt, ', '.join(sorted(coll_store._supported_options))
                    )
                )

        coll_store.options.update(command)
        if validation_opts.intersection(command):
            for k, v in validation_defaults.items():
                coll_store.options.setdefault(k, v)

        if 'validator' in coll_store.options and not coll_store.options['validator']:
            del coll_store.options['validator']

        return {'ok': 1.0}

    def with_options(
        self, codec_options=None, read_preference=None, write_concern=None, read_concern=None
    ):
        mongomock_codec_options.is_supported(codec_options)

        if write_concern:
            raise NotImplementedError(
                'write_concern is a valid parameter for with_options but is not implemented yet in '
                'mongomock-ng'
            )

        if read_preference is None or read_preference == self._read_preference:
            return self

        return Database(
            self._client,
            self.name,
            self._store,
            read_preference=read_preference
            if read_preference is not None
            else self._read_preference,
            codec_options=codec_options if codec_options is not None else self._codec_options,
            read_concern=read_concern if read_concern is not None else self._read_concern,
        )
