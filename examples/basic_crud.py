#!/usr/bin/env python3
"""Basic CRUD operations example using mongomock_ng.

Demonstrates insert_one, insert_many, find_one, find,
update_one, update_many, delete_one, delete_many on a users
collection.  Designed to be idempotent — drops the database
at startup.
"""

import mongomock_ng


def run_crud_example():
    client = mongomock_ng.MongoClient()
    client.drop_database('example_db')

    db = client['example_db']
    users = db['users']

    # --- Create ---
    doc = {'name': 'Alice', 'email': 'alice@example.com', 'age': 30}
    result = users.insert_one(doc)
    print(f'insert_one -> inserted_id: {result.inserted_id}')

    docs = [
        {'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
        {'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35},
        {'name': 'Diana', 'email': 'diana@example.com', 'age': 28},
    ]
    result_many = users.insert_many(docs)
    print(f'insert_many -> inserted_ids: {result_many.inserted_ids}')

    # --- Read ---
    alice = users.find_one({'name': 'Alice'})
    print(f'find_one Alice: {alice}')

    all_users = list(users.find())
    print(f'find() all -> {len(all_users)} docs')

    # --- Update ---
    upd = users.update_one({'name': 'Alice'}, {'$set': {'age': 31}})
    print(f'update_one matched={upd.matched_count} modified={upd.modified_count}')

    upd_many = users.update_many({'age': {'$lt': 30}}, {'$inc': {'age': 1}})
    print(f'update_many matched={upd_many.matched_count} modified={upd_many.modified_count}')

    # --- Delete ---
    del_one = users.delete_one({'name': 'Bob'})
    print(f'delete_one deleted={del_one.deleted_count}')

    del_many = users.delete_many({'age': {'$gte': 30}})
    print(f'delete_many deleted={del_many.deleted_count}')

    remaining = list(users.find())
    print(f'remaining docs: {remaining}')


if __name__ == '__main__':
    run_crud_example()
