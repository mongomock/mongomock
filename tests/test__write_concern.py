import unittest

from mongomock_ng.write_concern import WriteConcern


class WriteConcernTest(unittest.TestCase):
    def test_default_constructor(self):
        wc = WriteConcern()
        self.assertEqual(wc.document, {})
        self.assertTrue(wc.acknowledged)
        self.assertTrue(wc.is_server_default)

    def test_constructor_with_w(self):
        wc = WriteConcern(w=1)
        self.assertIn('w', wc.document)
        self.assertEqual(wc.document['w'], 1)

    def test_constructor_with_wtimeout(self):
        wc = WriteConcern(wtimeout=5000)
        self.assertIn('wtimeout', wc.document)
        self.assertEqual(wc.document['wtimeout'], 5000)

    def test_constructor_with_j(self):
        wc = WriteConcern(j=True)
        self.assertIn('j', wc.document)
        self.assertTrue(wc.document['j'])

    def test_constructor_with_fsync(self):
        wc = WriteConcern(fsync=True)
        self.assertIn('fsync', wc.document)
        self.assertTrue(wc.document['fsync'])

    def test_constructor_all_params(self):
        wc = WriteConcern(w=2, wtimeout=1000, j=False, fsync=True)
        self.assertEqual(wc.document['w'], 2)
        self.assertEqual(wc.document['wtimeout'], 1000)
        self.assertFalse(wc.document['j'])
        self.assertTrue(wc.document['fsync'])

    def test_equality(self):
        wc1 = WriteConcern(w=1)
        wc2 = WriteConcern(w=1)
        self.assertEqual(wc1, wc2)

    def test_equality_default_and_explicit_w1(self):
        wc_default = WriteConcern()
        wc_explicit = WriteConcern(w=1)
        self.assertEqual(wc_default, wc_explicit)

    def test_inequality(self):
        wc1 = WriteConcern(w=1)
        wc2 = WriteConcern(w=2)
        self.assertNotEqual(wc1, wc2)

    def test_ne_with_wrong_type_returns_not_implemented(self):
        wc = WriteConcern(w=1)
        result = wc.__ne__('not_a_write_concern')
        self.assertIs(result, NotImplemented)

    def test_eq_with_wrong_type_returns_not_implemented(self):
        wc = WriteConcern(w=1)
        result = wc.__eq__('not_a_write_concern')
        self.assertIs(result, NotImplemented)

    def test_document_copy(self):
        wc = WriteConcern(w=1)
        doc = wc.document
        doc['w'] = 99
        self.assertEqual(wc.document['w'], 1)

    def test_is_server_default(self):
        self.assertTrue(WriteConcern().is_server_default)
        self.assertFalse(WriteConcern(w=1).is_server_default)
        self.assertFalse(WriteConcern(wtimeout=100).is_server_default)


if __name__ == '__main__':
    unittest.main()
