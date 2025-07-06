from mongomock.mongo_client import MongoClient


def main():
    client = MongoClient()
    db = client['test_db']
    users = db['users']
    users.insert_one({'name': 'Alice', 'age': 30})
    user = users.find_one({'name': 'Alice'})
    print(user)


if __name__ == '__main__':
    main()
