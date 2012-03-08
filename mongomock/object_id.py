import uuid

class ObjectId(object):
    def __init__(self):
        super(ObjectId, self).__init__()
        self._id = uuid.uuid1()
    def __eq__(self, other):
        return isinstance(other, ObjectId) and other._id == self._id
    def __ne__(self, other):
        return not (self == other)
    def __hash__(self):
        return hash(self._id)
    def __repr__(self):
        return 'ObjectId({})'.format(self._id)
    def __str__(self):
        return self._id
