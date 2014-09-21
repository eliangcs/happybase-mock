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

    def test_put_get_with_timestamps(self):
        self.table.put('01', {'d:name': 'John'}, timestamp=1)
        self.table.put('01', {'d:name': 'Joe'}, timestamp=2)
        self.assertEqual(self.table.row('01'), {'d:name': 'Joe'})
        self.assertEqual(self.table.row('01', timestamp=2), {'d:name': 'John'})
        self.assertEqual(self.table.row('01', timestamp=3), {'d:name': 'Joe'})

        self.table.put('01', {
            'd:name': 'Jack',
            'd:age': '20'
        }, timestamp=2)
        self.table.put('01', {
            'd:name': 'Jacky',
            'd:sex': 'M'
        })
        self.assertEqual(self.table.row('01'), {
            'd:name': 'Jacky',
            'd:age': '20',
            'd:sex': 'M'
        })

    def test_max_versions(self):
        # Default max_versions is 3
        self.table.put('01', {'d:name': 'Alice'}, timestamp=1)
        self.table.put('01', {'d:name': 'Bob'}, timestamp=2)
        self.table.put('01', {'d:name': 'Cate'}, timestamp=3)
        self.table.put('01', {'d:name': 'Dave'}, timestamp=4)

        self.assertEqual(self.table.row('01', timestamp=1), {'d:name': 'Bob'})
        self.assertEqual(self.table.row('01', timestamp=2), {'d:name': 'Bob'})
        self.assertEqual(self.table.row('01', timestamp=3), {'d:name': 'Bob'})
        self.assertEqual(self.table.row('01', timestamp=4), {'d:name': 'Cate'})
        self.assertEqual(self.table.row('01', timestamp=5), {'d:name': 'Dave'})

    def test_get_partial_columns(self):
        self.table.put('01', {
            'd:name': 'Elisa',
            'd:age': '20',
            'd:sex': 'F'
        }, timestamp=1)
        self.table.put('01', {
            'd:name': 'Elsa',
            'd:email': 'elsa@example.com'
        })
        row = self.table.row('01', columns=('d:name', 'd:email'))
        self.assertEqual(row, {
            'd:name': 'Elsa',
            'd:email': 'elsa@example.com'
        })

    def test_no_such_column_family(self):
        with self.assertRaises(IOError):
            self.table.put('01', {'bad_cf:name': 'Dont Care'})
