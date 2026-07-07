import uuid

from mongomock_ng import InvalidOperation


class TransactionOptions:
    def __init__(
        self,
        read_concern=None,
        write_concern=None,
        read_preference=None,
        max_commit_time_ms=None,
    ):
        self._read_concern = read_concern
        self._write_concern = write_concern
        self._read_preference = read_preference
        self._max_commit_time_ms = max_commit_time_ms

    @property
    def read_concern(self):
        return self._read_concern

    @property
    def write_concern(self):
        return self._write_concern

    @property
    def read_preference(self):
        return self._read_preference

    @property
    def max_commit_time_ms(self):
        return self._max_commit_time_ms


class SessionOptions:
    def __init__(self, causal_consistency=True, default_transaction_options=None):
        self._causal_consistency = causal_consistency
        self._default_transaction_options = default_transaction_options

    @property
    def causal_consistency(self):
        return self._causal_consistency

    @property
    def default_transaction_options(self):
        return self._default_transaction_options


class _TransactionContext:
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._session.in_transaction:
            return
        if exc_type is not None:
            self._session.abort_transaction()
        else:
            self._session.commit_transaction()


class ClientSession:
    def __init__(self, client, options=None):
        self._client = client
        self._options = options or SessionOptions()
        self._session_id = {'id': uuid.uuid4()}
        self._has_ended = False
        self._in_transaction = False
        self._transaction_collections = set()

    @property
    def client(self):
        return self._client

    @property
    def options(self):
        return self._options

    @property
    def session_id(self):
        return self._session_id

    @property
    def has_ended(self):
        return self._has_ended

    @property
    def in_transaction(self):
        return self._in_transaction

    def start_transaction(
        self,
        read_concern=None,
        write_concern=None,
        read_preference=None,
        max_commit_time_ms=None,
    ):
        if self._has_ended:
            raise InvalidOperation('Cannot use a session that has ended')
        if self._in_transaction:
            raise InvalidOperation('Transaction already in progress')
        self._in_transaction = True
        self._transaction_collections = set()
        return _TransactionContext(self)

    def commit_transaction(self):
        if not self._in_transaction:
            raise InvalidOperation('No transaction started')
        for collection_store in self._transaction_collections:
            collection_store.commit_transaction()
        self._in_transaction = False
        self._transaction_collections = set()

    def abort_transaction(self):
        if not self._in_transaction:
            raise InvalidOperation('No transaction started')
        for collection_store in self._transaction_collections:
            collection_store.abort_transaction()
        self._in_transaction = False
        self._transaction_collections = set()

    def end_session(self):
        if self._in_transaction:
            self.abort_transaction()
        self._has_ended = True

    def with_transaction(
        self,
        callback,
        read_concern=None,
        write_concern=None,
        read_preference=None,
        max_commit_time_ms=None,
    ):
        with self.start_transaction(
            read_concern=read_concern,
            write_concern=write_concern,
            read_preference=read_preference,
            max_commit_time_ms=max_commit_time_ms,
        ):
            return callback(self)

    def _ensure_collection_in_transaction(self, collection_store):
        if self._in_transaction and collection_store not in self._transaction_collections:
            collection_store.begin_transaction()
            self._transaction_collections.add(collection_store)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_session()
