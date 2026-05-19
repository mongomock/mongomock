try:
    from pymongo.results import BulkWriteResult
    from pymongo.results import DeleteResult
    from pymongo.results import InsertManyResult
    from pymongo.results import InsertOneResult
    from pymongo.results import UpdateResult
except ImportError:  # pragma: no cover

    class _WriteResult:
        def __init__(self, acknowledged=True):
            self.__acknowledged = acknowledged

        @property
        def acknowledged(self):
            return self.__acknowledged

    class _FallbackInsertOneResult(_WriteResult):
        __slots__ = ('__acknowledged', '__inserted_id')

        def __init__(self, inserted_id, acknowledged=True):
            self.__inserted_id = inserted_id
            super().__init__(acknowledged)

        @property
        def inserted_id(self):
            return self.__inserted_id

    class _FallbackInsertManyResult(_WriteResult):
        __slots__ = ('__acknowledged', '__inserted_ids')

        def __init__(self, inserted_ids, acknowledged=True):
            self.__inserted_ids = inserted_ids
            super().__init__(acknowledged)

        @property
        def inserted_ids(self):
            return self.__inserted_ids

    class _FallbackUpdateResult(_WriteResult):
        __slots__ = ('__acknowledged', '__raw_result')

        def __init__(self, raw_result, acknowledged=True):
            self.__raw_result = raw_result
            super().__init__(acknowledged)

        @property
        def raw_result(self):
            return self.__raw_result

        @property
        def matched_count(self):
            if self.upserted_id is not None:
                return 0
            return self.__raw_result.get('n', 0)

        @property
        def modified_count(self):
            return self.__raw_result.get('nModified')

        @property
        def upserted_id(self):
            return self.__raw_result.get('upserted')

    class _FallbackDeleteResult(_WriteResult):
        __slots__ = ('__acknowledged', '__raw_result')

        def __init__(self, raw_result, acknowledged=True):
            self.__raw_result = raw_result
            super().__init__(acknowledged)

        @property
        def raw_result(self):
            return self.__raw_result

        @property
        def deleted_count(self):
            return self.__raw_result.get('n', 0)

    class _FallbackBulkWriteResult(_WriteResult):
        __slots__ = ('__acknowledged', '__bulk_api_result')

        def __init__(self, bulk_api_result, acknowledged):
            self.__bulk_api_result = bulk_api_result
            super().__init__(acknowledged)

        @property
        def bulk_api_result(self):
            return self.__bulk_api_result

        @property
        def inserted_count(self):
            return self.__bulk_api_result.get('nInserted')

        @property
        def matched_count(self):
            return self.__bulk_api_result.get('nMatched')

        @property
        def modified_count(self):
            return self.__bulk_api_result.get('nModified')

        @property
        def deleted_count(self):
            return self.__bulk_api_result.get('nRemoved')

        @property
        def upserted_count(self):
            return self.__bulk_api_result.get('nUpserted')

        @property
        def upserted_ids(self):
            if self.__bulk_api_result:
                return {
                    upsert['index']: upsert['_id'] for upsert in self.bulk_api_result['upserted']
                }

    # Reatribuir aos nomes esperados (sem duplicação de nome para mypy)
    InsertOneResult = _FallbackInsertOneResult
    InsertManyResult = _FallbackInsertManyResult
    UpdateResult = _FallbackUpdateResult
    DeleteResult = _FallbackDeleteResult
    BulkWriteResult = _FallbackBulkWriteResult


__all__ = [
    'BulkWriteResult',
    'DeleteResult',
    'InsertManyResult',
    'InsertOneResult',
    'UpdateResult',
]
