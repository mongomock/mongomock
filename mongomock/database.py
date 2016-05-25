from . import CollectionInvalid
from .collection import Collection


class Database(object):

    def __init__(self, client, name):
        self.name = name
        self._client = client
        self._collections = {
            'system.indexes': Collection(self, 'system.indexes')}

    def __getitem__(self, coll_name):
        return self.get_collection(coll_name)

    def __getattr__(self, attr):
        return self[attr]

    def __repr__(self):
        return "Database({0}, '{1}')".format(self._client, self.name)

    @property
    def client(self):
        return self._client

    def collection_names(self, include_system_collections=True):
        if include_system_collections:
            return list(self._collections.keys())

        result = []
        for name in self._collections.keys():
            if not name.startswith("system."):
                result.append(name)

        return result

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None):
        collection = self._collections.get(name)
        if collection is None:
            collection = self._collections[name] = Collection(self, name)
        return collection

    def drop_collection(self, name_or_collection):
        try:
            if isinstance(name_or_collection, Collection):
                for name, collection in self._collections.items():
                    if collection is name_or_collection:
                        collection.drop()
                        del self._collections[name]
                        break
            else:
                if name_or_collection in self._collections:
                    collection = self._collections.get(name_or_collection)
                    if collection:
                        collection.drop()
                del self._collections[name_or_collection]
        # EAFP paradigm
        # (http://en.m.wikipedia.org/wiki/Python_syntax_and_semantics)
        except Exception:
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
