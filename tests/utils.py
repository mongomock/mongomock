class DBRef(object):

    def __init__(self, collection, id, database=None):
        self.collection = collection
        self.id = id
        self.database = database
