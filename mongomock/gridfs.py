
def enable_gridfs_integration():

    """This function enables the use of mongomock Database's and Collection's inside gridfs

    Gridfs library use `isinstance` to make sure the passed elements
    are valid `pymongo.Database/Collection`. Hence we have to monkeypatch
    isinstance behaviour to also accept `mongomock.Database/Collection`.
    Note we only patch isinstance within the gridfs module, especially because
    overloading this builtins makes the code really slow.
    """

    import builtins
    from importlib import import_module
    from pymongo.collection import Collection as PyMongoCollection
    from pymongo.database import Database as PyMongoDatabase
    from mongomock import Database as MongoMockDatabase, Collection as MongoMockCollection

    def isinstance_patched(object, classinfo):
        if type(classinfo) is tuple:
            classesinfo = list(classinfo)
        else:
            classesinfo = [classinfo]
        mocked_needed = []
        for cls in classesinfo:
            if cls is PyMongoCollection:
                mocked_needed.append(MongoMockCollection)
            if cls is PyMongoDatabase:
                mocked_needed.append(MongoMockDatabase)
        return builtins.isinstance(object, tuple(classesinfo + mocked_needed))

    for modname in ('gridfs', 'gridfs.grid_file', 'gridfs.errors'):
        mod = import_module(modname)
        mod.__builtins__ = mod.__builtins__.copy()
        mod.__builtins__['isinstance'] = isinstance_patched
