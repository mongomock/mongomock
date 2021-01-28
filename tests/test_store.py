from mongomock.store import RWLock


def test_rwlock_exception():
    """Asserts the locks are cleaned correctly in if exceptions occur between
    a lock's acquire/release"""
    lock = RWLock()

    for method in [lock.reader, lock.writer]:
        try:
            with method():
                raise ValueError
        except ValueError:
            pass

        assert not lock._no_writers.locked()
        assert not lock._no_readers.locked()
