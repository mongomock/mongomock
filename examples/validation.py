"""Schema validation demonstration using mongomock-ng.

Creates a collection with a validator using MongoDB query operators
($type, $gte, $lte, $and). mongomock-ng validates documents against
the query filter — similar to how MongoDB evaluates $jsonSchema but
using the standard query language.

Note: $jsonSchema syntax is not yet implemented in mongomock-ng.
"""

import mongomock_ng


def main():
    client = mongomock_ng.MongoClient()
    client.drop_database('example_db')
    db = client.example_db

    db.create_collection(
        'validated_users',
        validator={
            '$and': [
                {'name': {'$type': 'string'}},
                {'email': {'$type': 'string'}},
                {'age': {'$type': 'int'}},
                {'age': {'$gte': 0}},
                {'age': {'$lte': 150}},
            ],
        },
    )

    users = db.validated_users

    print('=== Valid insert (passes schema) ===')
    users.insert_one({'name': 'Alice', 'email': 'alice@example.com', 'age': 30})
    print('  Inserted: Alice (age 30)')

    users.insert_one({'name': 'Bob', 'email': 'bob@example.com', 'age': 25})
    print('  Inserted: Bob (age 25)')

    all_users = list(users.find({}, {'_id': 0}))
    print(f'  All users: {all_users}')

    print()
    print('=== Invalid insert (fails validation - missing field) ===')
    try:
        users.insert_one({'name': 'NoEmail', 'age': 20})
    except mongomock_ng.WriteError as e:
        print(f'  WriteError: {e}')

    print()
    print('=== Invalid insert (fails validation - wrong type) ===')
    try:
        users.insert_one({'name': 'BadAge', 'email': 'bad@example.com', 'age': 'thirty'})
    except mongomock_ng.WriteError as e:
        print(f'  WriteError: {e}')

    print()
    print('=== Collection options include stored validator ===')
    opts = db['validated_users'].options()
    print(f'  validator keys: {list(opts["validator"].keys())}')

    print()
    print('--- validation.py done ---')


if __name__ == '__main__':
    main()
