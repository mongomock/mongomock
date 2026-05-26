class CommandCursor:
    def __init__(self, collection, curser_info=None, address=None, retrieved=0):
        self._collection = iter(collection)
        self._id = None
        self._address = address
        self._data = {}
        self._retrieved = retrieved
        self._batch_size = 0
        self._killed = self._id == 0
        self._exhausted = False

    @property
    def address(self):
        return self._address

    def close(self):
        self._killed = True
        self._exhausted = True

    def batch_size(self, batch_size):
        return self

    @property
    def alive(self):
        return not self._exhausted

    def __iter__(self):
        return self

    def next(self):
        try:
            return next(self._collection)
        except StopIteration:
            self._exhausted = True
            raise

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return
