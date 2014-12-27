import sys

from .helpers import ObjectId
try:
    import simplejson as json
except ImportError:
    import json

try:
    from pymongo.errors import DuplicateKeyError
except:
    class DuplicateKeyError(Exception):
        pass

try:
    from pymongo.errors import OperationFailure
except:
    class OperationFailure(Exception):
        pass

try:
    from pymongo.errors import CollectionInvalid
except:
    class CollectionInvalid(Exception):
        pass

from mongomock.__version__ import __version__


__all__ = ['Connection', 'Database', 'Collection', 'ObjectId']


from .connection import Connection, MongoClient
from .database import Database
from .collection import Collection

