import asyncio
from mongomock.collection import Cursor, Collection
from mongomock.database import Database


def _convert_to_async(func):
    async def inner(*args, **kwargs):
        await asyncio.sleep(0)
        ret = func(*args, **kwargs)
        if isinstance(ret, Cursor):
            return AsyncCursorProxy(ret)
        return ret
    return inner


class AsyncMongoMockProxy:
    def __init__(self, real_obj):
        self.real_obj = real_obj

    def __getattr__(self, item):
        attr = getattr(self.real_obj, item)
        if isinstance(attr, Database):
            return AsyncMongoMockProxy(attr)
        if isinstance(attr, Collection):
            return AsyncCollectionProxy(attr)
        if isinstance(attr, Cursor):
            return AsyncCursorProxy(attr)
        if callable(attr):
            return _convert_to_async(attr)
        return attr


class AsyncCollectionProxy(AsyncMongoMockProxy):
    def __call__(self, *args, **kwargs):
        return self.real_obj(*args, **kwargs)


class AsyncCursorProxy(AsyncMongoMockProxy):
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0)
        try:
            return next(self.real_obj)
        except StopIteration:
            raise StopAsyncIteration
