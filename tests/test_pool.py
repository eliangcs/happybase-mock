from .base import BaseTestCase
from happybase_mock.pool import Connection, ConnectionPool


class TestConnectionPool(BaseTestCase):

    def tearDown(self):
        Connection._instances.clear()

    def test_connection(self):
        pool = ConnectionPool(5, host='myhost', port=9999, table_prefix='test')
        with pool.connection() as conn:
            self.assertEqual(conn.host, 'myhost')
            self.assertEqual(conn.port, 9999)
            self.assertEqual(conn.table_prefix, 'test')

            # Test creating table and putting data
            conn.create_table('hello', {'d': dict()})
            table = conn.table('hello')
            table.put('key', {'d:data': 'world'})
            self.assertEqual(table.row('key'), {'d:data': 'world'})
