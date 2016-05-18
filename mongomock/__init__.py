try:
    from pymongo.errors import PyMongoError
except ImportError:
    class PyMongoError(Exception):
        pass

try:
    from pymongo.errors import DuplicateKeyError
except ImportError:
    class DuplicateKeyError(PyMongoError):
        pass

try:
    from pymongo.errors import OperationFailure
except ImportError:
    class OperationFailure(PyMongoError):
        pass

try:
    from pymongo.errors import CollectionInvalid
except ImportError:
    class CollectionInvalid(PyMongoError):
        pass

try:
    from pymongo.errors import InvalidOperation
except ImportError:
    class InvalidOperation(PyMongoError):
        pass

try:
    from pymongo.errors import ConfigurationError
except ImportError:
    class ConfigurationError(PyMongoError):
        pass

try:
    from pymongo.errors import InvalidURI
except ImportError:
    class InvalidURI(ConfigurationError):
        pass

try:
    from pymongo.errors import WriteError
except ImportError:
    class WriteError(OperationFailure):
        pass

from mongomock.__version__ import __version__

from .collection import Collection
from .database import Database
from .helpers import ObjectId  # noqa
from .mongo_client import MongoClient
from .write_concern import WriteConcern


__all__ = [
    '__version__',
    'Database',
    'DuplicateKeyError',
    'Collection',
    'CollectionInvalid',
    'MongoClient',
    'ObjectId',
    'OperationFailure',
    'WriteConcern'
]


