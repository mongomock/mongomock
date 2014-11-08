from .collection import Collection
from . import CollectionInvalid


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
                collections_keys = list(self._collections.keys())
                for collection_name in collections_keys:
                    tmp_collection = self._collections.get(collection_name)
                    if tmp_collection is name_or_collection:
                        if tmp_collection:
                            tmp_collection.drop()
                        del self._collections[collection_name]
            else:
                if name_or_collection in self._collections:
                    collection = self._collections.get(name_or_collection)
                    if collection:
                        collection.drop()

                del self._collections[name_or_collection]
        except:  # EAFP paradigm (http://en.m.wikipedia.org/wiki/Python_syntax_and_semantics)
            pass

    def create_collection(self, name, **kwargs):
        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        if kwargs:
            raise NotImplementedError("Special options not supported")

        return self[name]

    def dereference(self, dbref):

        if not dbref.collection or not dbref.id:
            raise TypeError("cannot dereference a %s" % type(dbref))
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError("trying to dereference a DBRef that points to "
                             "another database (%r not %r)" % (dbref.database,
                                                               self.__name))
        return self[dbref.collection].find_one({"_id": dbref.id})