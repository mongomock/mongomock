class DBRef:
    def __init__(self, collection, id, database=None):
        self.__dict__['collection'] = collection
        self.__dict__['id'] = id
        self.__dict__['database'] = database
        self.__dict__['_kwargs'] = {}

    def __setattr__(self, name, value):
        raise AttributeError(f"DBRef objects are immutable, cannot set '{name}'")

    def __delattr__(self, name):
        raise AttributeError(f"DBRef objects are immutable, cannot delete '{name}'")

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if name in self.__dict__.get('_kwargs', {}):
            return self.__dict__['_kwargs'][name]
        raise AttributeError(f"DBRef has no attribute '{name}'")

    def __hash__(self):
        return hash((self.collection, self.id, self.database))

    def __eq__(self, other):
        if not isinstance(other, DBRef):
            return False
        return (
            self.collection == other.collection
            and self.id == other.id
            and self.database == other.database
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f"DBRef('{self.collection}', {self.id!r})"

    def as_doc(self):
        doc = {'$ref': self.collection, '$id': self.id}
        if self.database is not None:
            doc['$db'] = self.database
        return doc
