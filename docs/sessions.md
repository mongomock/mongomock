# ClientSession and Transactions

## Overview

mongomock-ng supports `ClientSession` and transactions, allowing you to test transactional code that uses MongoDB sessions.

## Basic Usage

### Starting a Session

```python
import mongomock

client = mongomock.MongoClient()
session = client.start_session()

# Use the session...

session.end_session()
```

### Context Manager

Sessions support the context manager pattern, which automatically calls `end_session()`:

```python
with client.start_session() as session:
    # Use the session...
    pass
# Session is automatically ended
```

## Transactions

### Starting a Transaction

```python
session = client.start_session()
session.start_transaction()

try:
    # Perform operations within the transaction
    collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
    collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
    
    # Commit the transaction
    session.commit_transaction()
except Exception:
    # Abort the transaction on error
    session.abort_transaction()
    raise
finally:
    session.end_session()
```

### Transaction Context Manager

Transactions support the context manager pattern:

```python
with client.start_session() as session:
    with session.start_transaction():
        # Operations are automatically committed on clean exit
        collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
        collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
    
    # If an exception occurs, the transaction is automatically aborted
```

### with_transaction() Helper

The `with_transaction()` method provides a convenient way to execute a callback within a transaction:

```python
def callback(session):
    collection.insert_one({'_id': 1, 'value': 'test'}, session=session)
    collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
    return 'success'

with client.start_session() as session:
    result = session.with_transaction(callback)
    print(result)  # 'success'
```

## Transaction Isolation

mongomock-ng uses a snapshot-based isolation mechanism for transactions:

- When a transaction starts, the first collection accessed is snapshotted
- Writes within the transaction modify the snapshot
- On `commit_transaction()`, the snapshot replaces the original data
- On `abort_transaction()`, the original data is restored

### Example: Abort Transaction

```python
client = mongomock.MongoClient()
collection = client.db.collection
collection.insert_one({'_id': 1, 'value': 'original'})

session = client.start_session()
with session.start_transaction():
    collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
    session.abort_transaction()

# The update was aborted, original value is preserved
doc = collection.find_one({'_id': 1})
print(doc['value'])  # 'original'
```

### Example: Commit Transaction

```python
client = mongomock.MongoClient()
collection = client.db.collection
collection.insert_one({'_id': 1, 'value': 'original'})

session = client.start_session()
with session.start_transaction():
    collection.update_one({'_id': 1}, {'$set': {'value': 'modified'}}, session=session)
    session.commit_transaction()

# The update was committed
doc = collection.find_one({'_id': 1})
print(doc['value'])  # 'modified'
```

## Supported Operations

The `session` parameter is supported in all CRUD operations:

- `insert_one()`, `insert_many()`
- `update_one()`, `update_many()`, `replace_one()`
- `delete_one()`, `delete_many()`
- `find_one()`, `find_one_and_update()`, `find_one_and_replace()`, `find_one_and_delete()`
- `find()` (returns a cursor that respects the session)
- `aggregate()`
- `count_documents()`, `distinct()`
- `bulk_write()`
- `create_index()`, `create_indexes()`, `drop_index()`, `drop_indexes()`
- `drop()`, `rename()`

### Database Operations

The `session` parameter is also supported in database operations:

- `list_collections()`, `list_collection_names()`
- `drop_collection()`
- `create_collection()`
- `dereference()`

## Session Properties

```python
session = client.start_session()

# Check if session has ended
print(session.has_ended)  # False

# Check if transaction is active
print(session.in_transaction)  # False

# Get session ID
print(session.session_id)  # {'id': UUID('...')}

# Get the client that created the session
print(session.client)  # MongoClient instance

# Get session options
print(session.options)  # SessionOptions instance
```

## Client Isolation

Each `MongoClient` instance has isolated storage. Transactions on one client do not affect data on another client:

```python
client1 = mongomock.MongoClient()
client2 = mongomock.MongoClient()

collection1 = client1.db.collection
collection2 = client2.db.collection

collection1.insert_one({'_id': 1, 'value': 'client1'})

# client2 doesn't see client1's data
print(collection2.find_one({'_id': 1}))  # None
```

## Error Handling

### Starting a Transaction When One is Already Active

```python
session = client.start_session()
session.start_transaction()

try:
    session.start_transaction()  # Raises InvalidOperation
except mongomock.InvalidOperation as e:
    print(e)  # "Transaction already in progress"
```

### Committing/Aborting Without an Active Transaction

```python
session = client.start_session()

try:
    session.commit_transaction()  # Raises InvalidOperation
except mongomock.InvalidOperation as e:
    print(e)  # "No transaction started"

try:
    session.abort_transaction()  # Raises InvalidOperation
except mongomock.InvalidOperation as e:
    print(e)  # "No transaction started"
```

### Using an Ended Session

```python
session = client.start_session()
session.end_session()

try:
    session.start_transaction()  # Raises InvalidOperation
except mongomock.InvalidOperation as e:
    print(e)  # "Cannot use a session that has ended"
```

## Limitations

### Snapshot Isolation vs Real MongoDB

mongomock-ng uses a simplified snapshot-based isolation mechanism. This differs from real MongoDB's multi-version concurrency control (MVCC):

- In mongomock-ng, the snapshot is taken on the first collection access within a transaction
- In real MongoDB, the snapshot is taken when the transaction starts

This means that in mongomock-ng, if you perform operations on multiple collections within a transaction, each collection is snapshotted independently when first accessed.

### Concurrent Transactions

mongomock-ng is designed for single-threaded testing. Concurrent transactions on the same client are not supported and may lead to unexpected behavior.

### Real MongoDB Comparison

When comparing mongomock-ng behavior with real MongoDB in tests, be aware that:

- Real MongoDB replica set returns `$clusterTime` and `operationTime` in command responses
- mongomock-ng does not return these fields by default
- Use `excluding()` in test comparisons to ignore these metadata fields:

```python
self.cmp.compare.excluding('$clusterTime', 'operationTime').rename('new_name')
```

## Testing with Real MongoDB

To test your code against both mongomock-ng and real MongoDB, use the comparison pattern:

```python
import mongomock
import pymongo

def test_with_both_clients():
    fake_client = mongomock.MongoClient()
    real_client = pymongo.MongoClient('mongodb://localhost:27017/')
    
    # Test with mongomock
    with fake_client.start_session() as session:
        with session.start_transaction():
            fake_client.db.collection.insert_one({'_id': 1}, session=session)
    
    # Test with real MongoDB (requires replica set for transactions)
    with real_client.start_session() as session:
        with session.start_transaction():
            real_client.db.collection.insert_one({'_id': 1}, session=session)
    
    # Compare results
    fake_doc = fake_client.db.collection.find_one({'_id': 1})
    real_doc = real_client.db.collection.find_one({'_id': 1})
    assert fake_doc == real_doc
```

## See Also

- [MongoDB Sessions Documentation](https://www.mongodb.com/docs/manual/reference/method/Mongo.startSession/)
- [MongoDB Transactions Documentation](https://www.mongodb.com/docs/manual/core/transactions/)
- [PyMongo ClientSession API](https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html)
