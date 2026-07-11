"""TTL (Time-To-Live) index demonstration using mongomock-ng.

Creates an index with expireAfterSeconds and shows how expired
documents are automatically removed on subsequent reads.

Note: mongomock-ng simulates TTL expiry eagerly — expired documents
are removed the next time the collection is accessed (find, count, etc.),
not by a background thread like real MongoDB.
"""

from datetime import timedelta

import mongomock_ng


def main():
    client = mongomock_ng.MongoClient()
    client.drop_database('example_db')
    db = client.example_db

    coll = db.session_logs

    coll.create_index([('created_at', 1)], expireAfterSeconds=10)

    now = mongomock_ng.utcnow()

    future = now + timedelta(hours=1)
    past = now - timedelta(seconds=30)
    recent = now - timedelta(seconds=5)

    coll.insert_many(
        [
            {'_id': 1, 'event': 'login', 'created_at': past},
            {'_id': 2, 'event': 'logout', 'created_at': future},
            {'_id': 3, 'event': 'view', 'created_at': recent},
        ]
    )

    print(f'  now          = {now}')
    print(f'  past (-30s)  = {past}')
    print(f'  future (+1h) = {future}')
    print(f'  recent (-5s) = {recent}')

    print()
    print('  Inserted 3 documents')
    print('  Documents before TTL sweep:')
    for d in coll.find({}, {'_id': 0}):
        print(f'    {d}')

    print()
    print('=== After TTL sweep (count_documents triggers removal) ===')
    remaining = coll.count_documents({})
    print(f'  Remaining documents: {remaining}')
    print('  (past doc expired, future and recent remain)')

    print()
    print('=== Remaining docs ===')
    for d in coll.find({}, {'_id': 0}):
        print(f'  {d}')

    print()
    print('=== TTL with expireAfterSeconds=0 (immediate expiry) ===')
    coll.create_index([('timestamp', 1)], expireAfterSeconds=0)
    coll.insert_one({'event': 'immediate', 'timestamp': mongomock_ng.utcnow()})
    count_before = coll.count_documents({})
    print(f'  Inserted + count_before: {count_before}')
    print('  (document with timestamp=now expires immediately with expireAfterSeconds=0)')

    print()
    print('--- ttl.py done ---')


if __name__ == '__main__':
    main()
