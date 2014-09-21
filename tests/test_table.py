import unittest

from happybase_mock import Connection


class TestTable(unittest.TestCase):

    def setUp(self):
        self.conn = Connection()
        self.conn.create_table('person', {'d': dict()})
        self.table = self.conn.table('person')

    def test_put_and_get(self):
        self.table.put('john', {'d:name': 'John'})
        self.assertEqual(self.table.row('john'), {
            'd:name': 'John'
        })
