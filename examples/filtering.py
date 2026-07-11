#!/usr/bin/env python3
"""Query operators & cursor methods example using mongomock_ng.

Demonstrates $eq, $gt, $gte, $lt, $lte, $in, $nin, $ne,
$exists, $regex, projection, and sort/skip/limit on a
products collection.  Idempotent — drops database at startup.
"""

import mongomock_ng


def run_filtering_example():
    client = mongomock_ng.MongoClient()
    client.drop_database('example_db')

    db = client['example_db']
    products = db['products']

    products.insert_many(
        [
            {'name': 'Widget', 'price': 9.99, 'category': 'tools', 'tags': ['small', 'metal']},
            {
                'name': 'Gadget',
                'price': 24.99,
                'category': 'electronics',
                'tags': ['small', 'plastic'],
            },
            {'name': 'Doohickey', 'price': 14.99, 'category': 'tools', 'tags': ['medium', 'metal']},
            {'name': 'Thingamajig', 'price': 49.99, 'category': 'widgets', 'tags': ['large']},
            {'name': 'Whatsit', 'price': 5.99, 'category': 'widgets', 'tags': ['small']},
        ]
    )

    # --- Comparison operators ---
    print('--- $eq ---')
    print(list(products.find({'price': {'$eq': 9.99}})))

    print('--- $gt / $gte ---')
    print(list(products.find({'price': {'$gt': 20}})))
    print(list(products.find({'price': {'$gte': 9.99}})))

    print('--- $lt / $lte ---')
    print(list(products.find({'price': {'$lt': 10}})))
    print(list(products.find({'price': {'$lte': 9.99}})))

    print('--- $in / $nin ---')
    print(list(products.find({'category': {'$in': ['tools', 'widgets']}})))
    print(list(products.find({'category': {'$nin': ['electronics']}})))

    print('--- $ne ---')
    print(list(products.find({'category': {'$ne': 'tools'}})))

    # --- Element operator ---
    print('--- $exists ---')
    print(list(products.find({'tags': {'$exists': True}})))

    # --- Regex ---
    print('--- $regex ---')
    print(list(products.find({'name': {'$regex': '^(Widget|Gadget)'}})))

    # --- Projection ---
    print('--- projection (only name + price) ---')
    docs = list(products.find({}, {'_id': 0, 'name': 1, 'price': 1}))
    print(docs)

    # --- Sort, skip, limit ---
    print('--- sort(price, 1).skip(1).limit(2) ---')
    cursor = products.find().sort('price', 1).skip(1).limit(2)
    print(list(cursor))


if __name__ == '__main__':
    run_filtering_example()
