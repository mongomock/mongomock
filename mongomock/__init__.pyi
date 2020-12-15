from typing import Any, Callable, Literal, Sequence, Union
from unittest import mock

from bson.objectid import ObjectId as ObjectId
from pymongo import (
    MongoClient as MongoClient,
    Collection as Collection,
    Database as Database,
)
from pymongo.errors import (
    CollectionInvalid as CollectionInvalid,
    DuplicateKeyError as DuplicateKeyError,
    InvalidName as InvalidName,
    OperationFailure as OperationFailure,
)


def patch(
        servers: Union[str, Sequence[str]] = ...,
        on_new: Literal['error', 'create', 'timeout', 'pymongo'] = ...,
        ) -> mock._patch:
    ...


_FeatureName = Literal['collation', 'session']


def ignore_feature(feature: _FeatureName) -> None:
    ...


def warn_on_feature(feature: _FeatureName) -> None:
    ...
