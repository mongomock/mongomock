import sys

if sys.version_info < (2, 7):
    from unittest2 import TestCase, skipIf
else:
    from unittest import TestCase, skipIf


class DBRef(object):

    def __init__(self, collection, id, database):
        self.collection = collection
        self.id = id
        self.database = database
