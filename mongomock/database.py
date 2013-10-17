from .collection import Collection

class Database(object):
    def __init__(self, conn, name):
        super(Database, self).__init__()
        self.name = name
        self._Database__connection = conn
        self._collections = {'system.indexes' : Collection(self, 'system.indexes')}

    def __getitem__(self, coll_name):
        coll = self._collections.get(coll_name, None)
        if coll is None:
            coll = self._collections[coll_name] = Collection(self, coll_name)
        return coll

    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        return "Database({0}, '{1}')".format(self._Database__connection, self.name)

    @property
    def connection(self):
        return self._Database__connection

    def collection_names(self, include_system_collections=True):
        if include_system_collections:
            return list(self._collections.keys())

        result = []
        for name in self._collections.keys():
            if name.startswith("system."): continue
            result.append(name)

        return result

    def drop_collection(self, name_or_collection):
        try:
            # FIXME a better way to remove an entry by value ?
            if isinstance(name_or_collection, Collection):
                for collection in self._collections.items():
                    if collection[1] is name_or_collection:
                        del self._collections[collection[0]]
            else:
                del self._collections[name_or_collection]
        except:  # EAFP paradigm (http://en.m.wikipedia.org/wiki/Python_syntax_and_semantics)
            pass
