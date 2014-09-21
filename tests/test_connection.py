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


class TestConnection(unittest.TestCase):

    def test_create_table(self):
        conn = Connection()
        conn.create_table('contact', {
            'd': {
                'max_versions': 10,
                'block_cache_enabled': True
            },
            'm': {}
        })
        table = conn.table('contact')
        self.assertEqual(table.families(), {
            'd': {
                'block_cache_enabled': True,
                'bloom_filter_nb_hashes': 0,
                'bloom_filter_type': 'NONE',
                'bloom_filter_vector_size': 0,
                'compression': 'NONE',
                'in_memory': False,
                'max_versions': 10,
                'name': 'd',
                'time_to_live': -1
            },
            'm': {
                'block_cache_enabled': False,
                'bloom_filter_nb_hashes': 0,
                'bloom_filter_type': 'NONE',
                'bloom_filter_vector_size': 0,
                'compression': 'NONE',
                'in_memory': False,
                'max_versions': 3,
                'name': 'm',
                'time_to_live': -1
            }
        })
