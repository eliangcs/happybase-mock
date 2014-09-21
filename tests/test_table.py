import time
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

    def test_get_include_timestamps(self):
        self.table.put('01', {'d:name': 'John', 'd:age': '20'}, timestamp=1)
        self.table.put('01', {'d:name': 'Joe'}, timestamp=2)

        self.assertEqual(self.table.row('01', include_timestamp=True), {
            'd:name': ('Joe', 2),
            'd:age': ('20', 1)
        })

    def test_max_versions(self):
        # Default max_versions is 3
        self.table.put('01', {'d:name': 'Alice'}, timestamp=1)
        self.table.put('01', {'d:name': 'Bob'}, timestamp=2)
        self.table.put('01', {'d:name': 'Cate'}, timestamp=3)
        self.table.put('01', {'d:name': 'Dave'}, timestamp=4)

        self.assertEqual(self.table.row('01', timestamp=1), {})
        self.assertEqual(self.table.row('01', timestamp=2), {})
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

    def test_get_multiple_rows(self):
        self.table.put('01', {'d:name': 'One'}, timestamp=10)
        self.table.put('02', {'d:name': 'Two'}, timestamp=20)
        self.table.put('03', {'d:name': 'Three'}, timestamp=30)

        self.assertEqual(self.table.rows(['03', '01', '02']), [
            ('03', {'d:name': 'Three'}),
            ('01', {'d:name': 'One'}),
            ('02', {'d:name': 'Two'})
        ])

        # Include timestamps
        self.assertEqual(
            self.table.rows(['03', '01', '02'], include_timestamp=True), [
                ('03', {'d:name': ('Three', 30)}),
                ('01', {'d:name': ('One', 10)}),
                ('02', {'d:name': ('Two', 20)})
            ])

    def test_cells(self):
        self.table.put('k', {'d:a': 'a1'}, timestamp=1)
        self.table.put('k', {'d:a': 'a2'}, timestamp=2)
        future_time = int((time.time() * 1000) * 2)
        self.table.put('k', {'d:a': 'a999'}, timestamp=future_time)

        self.assertEqual(self.table.cells('k', 'd:a'), ['a999', 'a2', 'a1'])
        self.assertEqual(
            self.table.cells('k', 'd:a', timestamp=time.time()), ['a2', 'a1'])

        # Include timestamps
        self.assertEqual(
            self.table.cells('k', 'd:a', include_timestamp=True), [
                ('a999', future_time), ('a2', 2), ('a1', 1)
            ])

    def test_no_such_column_family(self):
        with self.assertRaises(IOError):
            self.table.put('01', {'bad_cf:name': 'Dont Care'})

    def test_delete_whole_row(self):
        self.table.put('1', {'d:name': 'Gary'}, timestamp=1)
        self.table.put('1', {'d:age': '21'}, timestamp=1)
        self.table.put('2', {'d:name': 'Frank'})
        self.table.delete('1')

        self.assertEqual(self.table.row('1'), {})
        self.assertEqual(self.table.row('2'), {'d:name': 'Frank'})

    def test_delete_some_columns(self):
        self.table.put('1', {
            'd:name': 'Harry',
            'd:age': '16',
            'd:sex': 'M'
        })
        self.table.delete('1', columns=('d:age', 'd:sex'))
        self.assertEqual(self.table.row('1'), {'d:name': 'Harry'})

    def test_delete_some_timestamps(self):
        # Create a row like this:
        #     d:a   d:b   d:c
        #  1   a1    b1    c1
        #  2   a2    b2    c2
        #  3   a3    b3    c3
        for i in xrange(1, 4):
            self.table.put('key', {
                'd:a': 'a%d' % i, 'd:b': 'b%d' % i, 'd:c': 'c%d' % i
            }, timestamp=i)

        # After deleting a1, b1, a2, b2, the cells should become:
        #     d:a   d:b   d:c
        #  1               c1
        #  2               c2
        #  3   a3    b3    c3
        self.table.delete('key', columns=('d:a', 'd:b'), timestamp=2)
        self.assertEqual(self.table.row('key'), {
            'd:a': 'a3', 'd:b': 'b3', 'd:c': 'c3'
        })
        self.assertEqual(self.table.row('key', timestamp=3), {
            'd:c': 'c2'
        })
