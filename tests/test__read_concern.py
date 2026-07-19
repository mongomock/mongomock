import unittest

from mongomock_ng.read_concern import ReadConcern


class ReadConcernTest(unittest.TestCase):
    def test_default_constructor(self):
        rc = ReadConcern()
        self.assertEqual(rc.document, {})
        self.assertIsNone(rc.level)

    def test_constructor_with_level(self):
        rc = ReadConcern(level='local')
        self.assertIn('level', rc.document)
        self.assertEqual(rc.document['level'], 'local')

    def test_level_property(self):
        rc = ReadConcern(level='majority')
        self.assertEqual(rc.level, 'majority')

    def test_level_property_default(self):
        rc = ReadConcern()
        self.assertIsNone(rc.level)

    def test_ok_for_legacy(self):
        rc = ReadConcern()
        self.assertTrue(rc.ok_for_legacy)
        rc2 = ReadConcern(level='local')
        self.assertTrue(rc2.ok_for_legacy)

    def test_equality(self):
        rc1 = ReadConcern(level='local')
        rc2 = ReadConcern(level='local')
        self.assertEqual(rc1, rc2)

    def test_inequality(self):
        rc1 = ReadConcern(level='local')
        rc2 = ReadConcern(level='majority')
        self.assertNotEqual(rc1, rc2)

    def test_document_copy(self):
        rc = ReadConcern(level='local')
        doc = rc.document
        doc['level'] = 'changed'
        self.assertEqual(rc.document['level'], 'local')


if __name__ == '__main__':
    unittest.main()
