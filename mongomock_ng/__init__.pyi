from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union
from unittest import mock

from bson.objectid import ObjectId as ObjectId
from pymongo import MongoClient as MongoClient
from pymongo.collection import Collection as Collection
from pymongo.database import Database as Database
from pymongo.errors import (
    BulkWriteError as BulkWriteError,
    CollectionInvalid as CollectionInvalid,
    ConfigurationError as ConfigurationError,
    DuplicateKeyError as DuplicateKeyError,
    InvalidName as InvalidName,
    InvalidOperation as InvalidOperation,
    InvalidURI as InvalidURI,
    OperationFailure as OperationFailure,
    PyMongoError as PyMongoError,
    WriteError as WriteError,
)

from .write_concern import WriteConcern as WriteConcern

def patch(
    servers: Union[str, Tuple[str, int], Sequence[Union[str, Tuple[str, int]]]] = ...,
    on_new: Literal['error', 'create', 'timeout', 'pymongo'] = ...,
) -> mock._patch: ...

_FeatureName = Literal['collation', 'session']

def ignore_feature(feature: _FeatureName) -> None: ...
def warn_on_feature(feature: _FeatureName) -> None: ...

SERVER_VERSION: str = ...
