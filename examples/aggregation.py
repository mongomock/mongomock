"""Aggregation pipeline demonstration using mongomock-ng.

Shows $match, $group, $sort, $project, $limit, $unwind, $lookup stages
with two collections (orders + customers) joined via $lookup.
"""

from datetime import datetime

import mongomock_ng


def main():
    client = mongomock_ng.MongoClient()
    client.drop_database('example_db')
    db = client.example_db

    customers = db.customers
    orders = db.orders

    customers.insert_many(
        [
            {'_id': 1, 'name': 'Alice', 'city': 'NYC'},
            {'_id': 2, 'name': 'Bob', 'city': 'LA'},
            {'_id': 3, 'name': 'Charlie', 'city': 'NYC'},
        ]
    )

    orders.insert_many(
        [
            {
                '_id': 101,
                'customer_id': 1,
                'item': 'Widget',
                'qty': 2,
                'price': 10.0,
                'date': datetime(2025, 1, 15),
            },
            {
                '_id': 102,
                'customer_id': 1,
                'item': 'Gadget',
                'qty': 1,
                'price': 50.0,
                'date': datetime(2025, 2, 10),
            },
            {
                '_id': 103,
                'customer_id': 2,
                'item': 'Widget',
                'qty': 5,
                'price': 10.0,
                'date': datetime(2025, 3, 5),
            },
            {
                '_id': 104,
                'customer_id': 3,
                'item': 'Gizmo',
                'qty': 3,
                'price': 20.0,
                'date': datetime(2025, 1, 20),
            },
            {
                '_id': 105,
                'customer_id': 3,
                'item': 'Widget',
                'qty': 1,
                'price': 10.0,
                'date': datetime(2025, 4, 1),
            },
        ]
    )

    print('=== $match + $group + $sort: total spent per customer ===')
    pipeline = [
        {'$group': {'_id': '$customer_id', 'total': {'$sum': {'$multiply': ['$qty', '$price']}}}},
        {'$sort': {'total': -1}},
    ]
    for doc in orders.aggregate(pipeline):
        print(f'  customer_id={doc["_id"]}, total={doc["total"]}')

    print()
    print('=== $match (filter high-value) + $project (rename, exclude _id) ===')
    pipeline = [
        {'$match': {'price': {'$gte': 20}}},
        {'$project': {'_id': 0, 'product': '$item', 'quantity': '$qty', 'cost': '$price'}},
    ]
    for doc in orders.aggregate(pipeline):
        print(f'  {doc}')

    print()
    print('=== $limit + $sort: most recent 2 orders ===')
    pipeline = [
        {'$sort': {'date': -1}},
        {'$limit': 2},
        {'$project': {'_id': 0, 'item': 1, 'date': 1}},
    ]
    for doc in orders.aggregate(pipeline):
        print(f'  item={doc["item"]}, date={doc["date"]}')

    print()
    print('=== $lookup + $unwind: join orders with customers ===')
    pipeline = [
        {
            '$lookup': {
                'from': 'customers',
                'localField': 'customer_id',
                'foreignField': '_id',
                'as': 'customer',
            }
        },
        {'$unwind': '$customer'},
        {
            '$project': {
                '_id': 0,
                'item': 1,
                'qty': 1,
                'customer_name': '$customer.name',
                'city': '$customer.city',
            }
        },
        {'$sort': {'city': 1, 'item': 1}},
    ]
    for doc in orders.aggregate(pipeline):
        print(f'  {doc}')

    print()
    print('=== $group with $sum across all orders ===')
    pipeline = [
        {
            '$group': {
                '_id': None,
                'total_orders': {'$sum': 1},
                'grand_total': {'$sum': {'$multiply': ['$qty', '$price']}},
            }
        },
    ]
    for doc in orders.aggregate(pipeline):
        print(f'  total_orders={doc["total_orders"]}, grand_total={doc["grand_total"]}')

    print()
    print('--- aggregation.py done ---')


if __name__ == '__main__':
    main()
