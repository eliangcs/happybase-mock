import unittest

from happybase_mock import Connection


class TestConnectionSingleton(unittest.TestCase):

    def test_same_host_port(self):
        conn1 = Connection(host='myhost', port=9000)
        conn2 = Connection(host='myhost', port=9000)
        self.assertIs(conn1, conn2)

    def test_different_hosts(self):
        conn1 = Connection(host='myhost', port=9000)
        conn2 = Connection(host='yourhost', port=9001)
        self.assertIsNot(conn1, conn2)

    def test_different_ports(self):
        conn1 = Connection(host='myhost', port=9000)
        conn2 = Connection(host='myhost', port=9001)
        self.assertIsNot(conn1, conn2)

    def test_same_table_prefix(self):
        conn1 = Connection(table_prefix='app', table_prefix_separator='__')
        conn2 = Connection(table_prefix='app', table_prefix_separator='__')
        self.assertIs(conn1, conn2)

    def test_different_table_prefixes(self):
        conn1 = Connection(table_prefix='app')
        conn2 = Connection(table_prefix='app', table_prefix_separator='__')
        self.assertIsNot(conn1, conn2)
