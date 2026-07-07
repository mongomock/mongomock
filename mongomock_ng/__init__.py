import os


try:
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover

    class _FallbackPyMongoError(Exception):  # type: ignore[misc]
        pass

    PyMongoError = _FallbackPyMongoError


try:
    from pymongo.errors import OperationFailure
except ImportError:  # pragma: no cover

    class _FallbackOperationFailure(PyMongoError):
        def __init__(self, message, code=None, details=None):
            super().__init__()
            self._message = message
            self._code = code
            self._details = details

        code = property(lambda self: self._code)
        details = property(lambda self: self._details)

        def __str__(self):
            return self._message

    OperationFailure = _FallbackOperationFailure


try:
    from pymongo.errors import WriteError
except ImportError:  # pragma: no cover

    class _FallbackWriteError(OperationFailure):
        pass

    WriteError = _FallbackWriteError


try:
    from pymongo.errors import DuplicateKeyError
except ImportError:  # pragma: no cover

    class _FallbackDuplicateKeyError(WriteError):
        pass

    DuplicateKeyError = _FallbackDuplicateKeyError


try:
    from pymongo.errors import BulkWriteError
except ImportError:  # pragma: no cover

    class _FallbackBulkWriteError(OperationFailure):
        def __init__(self, results):
            super().__init__('batch op errors occurred', 65, results)

    BulkWriteError = _FallbackBulkWriteError


try:
    from pymongo.errors import CollectionInvalid
except ImportError:  # pragma: no cover

    class _FallbackCollectionInvalid(PyMongoError):
        pass

    CollectionInvalid = _FallbackCollectionInvalid


try:
    from pymongo.errors import InvalidName
except ImportError:  # pragma: no cover

    class _FallbackInvalidName(PyMongoError):
        pass

    InvalidName = _FallbackInvalidName


try:
    from pymongo.errors import InvalidOperation
except ImportError:  # pragma: no cover

    class _FallbackInvalidOperation(PyMongoError):
        pass

    InvalidOperation = _FallbackInvalidOperation


try:
    from pymongo.errors import ConfigurationError
except ImportError:  # pragma: no cover

    class _FallbackConfigurationError(PyMongoError):
        pass

    ConfigurationError = _FallbackConfigurationError


try:
    from pymongo.errors import InvalidURI
except ImportError:  # pragma: no cover

    class _FallbackInvalidURI(ConfigurationError):
        pass

    InvalidURI = _FallbackInvalidURI


from .helpers import ObjectId, utcnow  # noqa
from .__version__ import __version__


__all__ = [
    'SERVER_VERSION',
    'ClientSession',
    'Collection',
    'CollectionInvalid',
    'Database',
    'DuplicateKeyError',
    'InvalidName',
    'InvalidURI',
    'MongoClient',
    'ObjectId',
    'OperationFailure',
    'SessionOptions',
    'WriteConcern',
    '__version__',
    'ignore_feature',
    'patch',
    'warn_on_feature',
]

from .collection import Collection
from .database import Database
from .mongo_client import MongoClient
from .not_implemented import ignore_feature
from .not_implemented import warn_on_feature
from .patch import patch
from .session import ClientSession
from .session import SessionOptions
from .write_concern import WriteConcern


# The version of the server faked by mongomock. Callers may patch it before creating connections to
# update the behavior of mongomock.
# Keep the default version in sync with docker-compose.yml and travis.yml.
SERVER_VERSION = os.getenv('MONGODB', '7.0.34')
