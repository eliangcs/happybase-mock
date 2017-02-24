import time
from six.moves import xrange

from .base import BaseTestCase
from happybase_mock import Connection


class TestTable(BaseTestCase):

    def setUp(self):
        self.conn = Connection()
        self.conn.create_table('person', {'d': dict()})
        self.table = self.conn.table('person')

    def test_repr(self):
        self.assertEqual(repr(self.table),
                         "<happybase_mock.table.Table name='person'>")

    def test_regions(self):
        # Table.regions() is meaningless for in-memory mock, it only returns
        # a fixed data
        regions = self.table.regions()
        self.assertEqual(len(regions), 1)
        self.assertTrue(regions[0].get('name'))

        # Test non-existing table
        table = self.conn.table('no_such_table')
        self.assertEqual(table.regions(), [])

    def test_put_and_get(self):
        self.table.put(b'john', {b'd:name': b'John'})
        self.assertEqual(self.table.row(b'john'), {
            b'd:name': b'John'
        })

    def test_put_get_with_timestamps(self):
        self.table.put(b'01', {b'd:name': b'John'}, timestamp=1)
        self.table.put(b'01', {b'd:name': b'Joe'}, timestamp=2)
        self.assertEqual(self.table.row(b'01'), {b'd:name': b'Joe'})
        self.assertEqual(self.table.row(b'01', timestamp=2), {b'd:name': b'John'})
        self.assertEqual(self.table.row(b'01', timestamp=3), {b'd:name': b'Joe'})

        self.table.put(b'01', {
            b'd:name': b'Jack',
            b'd:age': b'20'
        }, timestamp=2)
        self.table.put(b'01', {
            b'd:name': b'Jacky',
            b'd:sex': b'M'
        })
        self.assertEqual(self.table.row(b'01'), {
            b'd:name': b'Jacky',
            b'd:age': b'20',
            b'd:sex': b'M'
        })

    def test_get_include_timestamps(self):
        self.table.put(b'01', {b'd:name': b'John', b'd:age': b'20'}, timestamp=1)
        self.table.put(b'01', {b'd:name': b'Joe'}, timestamp=2)

        self.assertEqual(self.table.row(b'01', include_timestamp=True), {
            b'd:name': (b'Joe', 2),
            b'd:age': (b'20', 1)
        })

        self.assertEqual(
            self.table.row(b'01', timestamp=2, include_timestamp=True),
            {b'd:name': (b'John', 1), b'd:age': (b'20', 1)})

    def test_max_versions(self):
        # Default max_versions is 3
        self.table.put(b'01', {b'd:name': b'Alice'}, timestamp=1)
        self.table.put(b'01', {b'd:name': b'Bob'}, timestamp=2)
        self.table.put(b'01', {b'd:name': b'Cate'}, timestamp=3)
        self.table.put(b'01', {b'd:name': b'Dave'}, timestamp=4)

        self.assertEqual(self.table.row(b'01', timestamp=1), {})
        self.assertEqual(self.table.row(b'01', timestamp=2), {})
        self.assertEqual(self.table.row(b'01', timestamp=3), {b'd:name': b'Bob'})
        self.assertEqual(self.table.row(b'01', timestamp=4), {b'd:name': b'Cate'})
        self.assertEqual(self.table.row(b'01', timestamp=5), {b'd:name': b'Dave'})

    def test_get_partial_columns(self):
        self.table.put(b'01', {
            b'd:name': b'Elisa',
            b'd:age': b'20',
            b'd:sex': b'F'
        }, timestamp=1)
        self.table.put(b'01', {
            b'd:name': b'Elsa',
            b'd:email': b'elsa@example.com'
        })
        row = self.table.row(b'01', columns=(b'd:name', b'd:email'))
        self.assertEqual(row, {
            b'd:name': b'Elsa',
            b'd:email': b'elsa@example.com'
        })

    def test_get_future_timestamp(self):
        future_time = int(time.time() * 1000 * 2)
        self.table.put(b'k', {b'd:a': b'data'}, timestamp=future_time)
        self.assertEqual(self.table.row(b'k'), {b'd:a': b'data'})

    def test_get_multiple_rows(self):
        self.table.put(b'01', {b'd:name': b'One'}, timestamp=10)
        self.table.put(b'02', {b'd:name': b'Two'}, timestamp=20)
        self.table.put(b'03', {b'd:name': b'Three'}, timestamp=30)

        self.assertEqual(self.table.rows([b'03', b'01', b'02']), [
            (b'03', {b'd:name': b'Three'}),
            (b'01', {b'd:name': b'One'}),
            (b'02', {b'd:name': b'Two'})
        ])

        # Include timestamps
        self.assertEqual(
            self.table.rows([b'03', b'01', b'02'], include_timestamp=True), [
                (b'03', {b'd:name': (b'Three', 30)}),
                (b'01', {b'd:name': (b'One', 10)}),
                (b'02', {b'd:name': (b'Two', 20)})
            ])

    def test_cells(self):
        self.table.put(b'k', {b'd:a': b'a1'}, timestamp=1)
        self.table.put(b'k', {b'd:a': b'a2'}, timestamp=2)
        future_time = int(time.time() * 1000 * 2)
        self.table.put(b'k', {b'd:a': b'a999'}, timestamp=future_time)

        self.assertEqual(self.table.cells(b'k', b'd:a'), [b'a999', b'a2', b'a1'])
        self.assertEqual(
            self.table.cells(b'k', b'd:a', timestamp=time.time()), [b'a2', b'a1'])

        # Include timestamps
        self.assertEqual(
            self.table.cells(b'k', b'd:a', include_timestamp=True), [
                (b'a999', future_time), (b'a2', 2), (b'a1', 1)
            ])

    def test_no_such_column_family(self):
        with self.assertRaises(IOError):
            self.table.put(b'01', {b'bad_cf:name': b'Dont Care'})

    def test_delete_whole_row(self):
        self.table.put(b'1', {b'd:name': b'Gary'}, timestamp=1)
        self.table.put(b'1', {b'd:age': b'21'}, timestamp=1)
        self.table.put(b'2', {b'd:name': b'Frank'})
        self.table.delete(b'1')

        self.assertEqual(self.table.row(b'1'), {})
        self.assertEqual(self.table.row(b'2'), {b'd:name': b'Frank'})

    def test_delete_some_columns(self):
        self.table.put(b'1', {
            b'd:name': b'Harry',
            b'd:age': b'16',
            b'd:sex': b'M'
        })
        self.table.delete(b'1', columns=(b'd:age', b'd:sex'))
        self.assertEqual(self.table.row(b'1'), {b'd:name': b'Harry'})

    def test_delete_columns_and_timestamp(self):
        # Create a row like this:
        #     d:a   d:b   d:c
        #  1   a1    b1    c1
        #  2   a2    b2    c2
        #  3   a3    b3    c3
        self.table.put(b'key', {
            b'd:a': b'a1', b'd:b': b'b1', b'd:c': b'c1'
        }, timestamp=1)
        self.table.put(b'key', {
            b'd:a': b'a2', b'd:b': b'b2', b'd:c': b'c2'
        }, timestamp=2)
        self.table.put(b'key', {
            b'd:a': b'a3', b'd:b': b'b3', b'd:c': b'c3'
        }, timestamp=3)

        # After deleting a1, b1, a2, b2, the cells should become:
        #     d:a   d:b   d:c
        #  1               c1
        #  2               c2
        #  3   a3    b3    c3
        self.table.delete(b'key', columns=(b'd:a', b'd:b'), timestamp=2)
        self.assertEqual(self.table.row(b'key'), {
            b'd:a': b'a3', b'd:b': b'b3', b'd:c': b'c3'
        })
        self.assertEqual(self.table.row(b'key', timestamp=3), {
            b'd:c': b'c2'
        })

    def test_delete_timestamp(self):
        # Create a row like this:
        #     d:a   d:b   d:c
        #  1   a1    b1    c1
        #  2   a2    b2    c2
        #  3   a3    b3    c3
        self.table.put(b'key', {
            b'd:a': b'a1', b'd:b': b'b1', b'd:c': b'c1'
        }, timestamp=1)
        self.table.put(b'key', {
            b'd:a': b'a2', b'd:b': b'b2', b'd:c': b'c2'
        }, timestamp=2)
        self.table.put(b'key', {
            b'd:a': b'a3', b'd:b': b'b3', b'd:c': b'c3'
        }, timestamp=3)

        # Delete timestamp <= 2:
        #     d:a   d:b   d:c
        #  1
        #  2
        #  3   a3    b3    c3
        self.table.delete(b'key', timestamp=2)
        self.assertEqual(self.table.row(b'key'), {
            b'd:a': b'a3', b'd:b': b'b3', b'd:c': b'c3'
        })
        self.assertEqual(self.table.row(b'key', timestamp=3), {})

    def test_scan(self):
        for i in xrange(1, 10):
            self.table.put(str(i).encode('utf-8'), {
                b'd:count': str(i).encode('utf-8')
            }, timestamp=i)

        # Scan all
        self.assertEqual(len(list(self.table.scan())), 9)

        # Prefix scan
        self.assertEqual(list(self.table.scan(row_prefix=b'2')), [
            (b'2', {b'd:count': b'2'})
        ])
        self.assertEqual(
            list(self.table.scan(row_prefix=b'3', include_timestamp=True)),
            [(b'3', {b'd:count': (b'3', 3)})])

        # Range scan
        self.assertEqual(list(self.table.scan(row_start=b'4', row_stop=b'6')), [
            (b'4', {b'd:count': b'4'}), (b'5', {b'd:count': b'5'})
        ])
        self.assertEqual(list(self.table.scan(row_start=b'8')), [
            (b'8', {b'd:count': b'8'}), (b'9', {b'd:count': b'9'})
        ])

        # scan with columns
        self.assertEqual(list(self.table.scan(row_prefix=b'2', columns=[b'd:count'])), [
            (b'2', {b'd:count': b'2'})
        ])
        self.assertEqual(list(self.table.scan(row_prefix=b'2', columns=['d:count'])), [
            (b'2', {b'd:count': b'2'})
        ])


    def test_scan_invalid_arguments(self):
        with self.assertRaises(TypeError):
            self.table.scan(row_start=b'1', row_stop=b'2', row_prefix=b'3')

    def test_counter(self):
        # Counter is 0 if the row/column does not exist
        self.assertEqual(self.table.counter_get(b'tina', b'd:age'), 0)

        self.table.counter_set(b'tina', b'd:age', 20)
        self.assertEqual(self.table.counter_get(b'tina', b'd:age'), 20)

        # Internal representation should be an 8-byte signed integer in big
        # endian
        self.assertEqual(self.table.row(b'tina'),
            {b'd:age': b'\x00\x00\x00\x00\x00\x00\x00\x14'}
        )

        self.table.counter_inc(b'tina', b'd:age', value=5)
        self.assertEqual(self.table.counter_get(b'tina', b'd:age'), 25)

        self.table.counter_dec(b'tina', b'd:age', value=30)
        self.assertEqual(self.table.counter_get(b'tina', b'd:age'), -5)
