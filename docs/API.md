# API Reference

## MongoClient

- `MongoClient(host=None, port=None, **kwargs)` — creates in-memory client
- `client.get_database(name)` — returns Database mock
- `client.list_database_names()` — list DB names
- `client.drop_database(name)` — drop DB
- `client.close()` — no-op (for test compatibility)

## Database

- `db.create_collection(name, **kwargs)` — create explicit collection (supports validator)
- `db.get_collection(name)` — get collection
- `db.list_collection_names(filter=None)` — list colls, supports name filter with $eq/$ne/$regex
- `db.drop_collection(name_or_collection)` — drop collection
- `db[name]` / `db.name` — get collection shorthand

## Collection

- `insert_one(document, bypass_document_validation=False, session=None)` → InsertOneResult
- `insert_many(documents, ordered=True, bypass_document_validation=False, session=None)` → InsertManyResult
- `find(filter=None, projection=None, skip=0, limit=0, sort=None, ...)` → Cursor
- `find_one(filter=None, ...)` — single doc, non-dict filter treated as _id
- `update_one(filter, update, upsert=False, array_filters=None, sort=None, ...)` → UpdateResult
- `update_many(filter, update, upsert=False, array_filters=None, ...)` → UpdateResult
- `replace_one(filter, replacement, upsert=False, ...)` → UpdateResult
- `delete_one(filter, ...)` → DeleteResult
- `delete_many(filter, ...)` → DeleteResult
- `count_documents(filter, ...)` → int
- `estimated_document_count()` → int
- `distinct(key, filter=None)` → list
- `aggregate(pipeline, ...)` → list (returns list, not CommandCursor!)
- `bulk_write(requests, ordered=True, ...)` → BulkWriteResult
- `create_index(keys, **kwargs)` — supports unique, sparse, name, expireAfterSeconds
- `create_indexes(indexes)` — list of IndexModel
- `drop_index(index_or_name)`
- `drop_indexes()`
- `list_indexes()` → generator
- `index_information()` → dict
- `find_one_and_update(filter, update, ...)` → doc or None
- `find_one_and_replace(filter, replacement, ...)` → doc or None
- `find_one_and_delete(filter, ...)` → doc or None
- `drop()` — drop collection
- `rename(new_name)` — rename collection
- `with_options(codec_options, read_preference, write_concern, read_concern)`

## Known quirks

- `aggregate()` returns `list` not `CommandCursor`
- `watch()` not implemented (raises NotImplementedError)
- `find_raw_batches()` / `aggregate_raw_batches()` not implemented
- `collation` raises NotImplementedError on most ops
- `session` mostly no-op (basic ClientSession exists)
