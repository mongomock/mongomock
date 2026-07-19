import unittest

from mongomock_ng.command_cursor import CommandCursor


class CommandCursorTest(unittest.TestCase):
    def setUp(self):
        self.cursor = CommandCursor([], address=('localhost', 27017))

    def test_address_property(self):
        self.assertEqual(self.cursor.address, ('localhost', 27017))

    def test_address_default(self):
        cursor = CommandCursor([])
        self.assertIsNone(cursor.address)

    def test_batch_size_returns_self(self):
        result = self.cursor.batch_size(100)
        self.assertIs(result, self.cursor)

    def test_alive_initially(self):
        self.assertTrue(self.cursor.alive)

    def test_close_sets_killed_and_exhausted(self):
        self.cursor.close()
        self.assertFalse(self.cursor.alive)

    def test_iteration_exhausts_cursor(self):
        cursor = CommandCursor([{'a': 1}, {'a': 2}], address=('localhost', 27017))
        results = list(cursor)
        self.assertEqual(len(results), 2)
        self.assertFalse(cursor.alive)

    def test_context_manager(self):
        with CommandCursor([], address=('localhost', 27017)) as cursor:
            self.assertTrue(cursor.alive)
        self.assertFalse(cursor.alive)


if __name__ == '__main__':
    unittest.main()
