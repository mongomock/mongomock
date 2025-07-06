from mongomock.mongo_client import MongoClient


def main():
    db = MongoClient().db
    orders = db.orders
    orders.insert_many(
        [
            {'item': 'apple', 'qty': 5},
            {'item': 'apple', 'qty': 3},
            {'item': 'banana', 'qty': 7},
        ]
    )
    result = list(orders.aggregate([{'$group': {'_id': '$item', 'total': {'$sum': '$qty'}}}]))
    print(result)


if __name__ == '__main__':
    main()
