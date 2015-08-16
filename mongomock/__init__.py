from .helpers import ObjectId

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

from mongomock.__version__ import __version__


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


from .write_concern import WriteConcern
from .mongo_client import MongoClient
from .database import Database
from .collection import Collection
