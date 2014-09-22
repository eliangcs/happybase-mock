import unittest

from happybase_mock import Connection


class TestBatch(unittest.TestCase):

    def setUp(self):
        self.conn = Connection()
        self.conn.create_table('movie', {'d': dict()})
        self.table = self.conn.table('movie')
        self.batch = self.table.batch()

    def test_put(self):
        self.batch.put('matrix', {'d:title': 'The Matrix'})
        self.batch.put('godfather', {'d:title': 'The Godfather'})
        self.batch.put('inception', {'d:title': 'Inception'})

        # No data is saved before send
        self.assertEqual(list(self.table.scan()), [])

        self.batch.send()
        self.assertEqual(list(self.table.scan()), [
            ('godfather', {'d:title': 'The Godfather'}),
            ('inception', {'d:title': 'Inception'}),
            ('matrix', {'d:title': 'The Matrix'})
        ])

    def test_delete(self):
        self.batch.put('matrix', {'d:title': 'The Matrix'})
        self.batch.put('godfather', {'d:title': 'The Godfather'})
        self.batch.put('inception', {'d:title': 'Inception'})
        self.batch.delete('matrix')

        # No data is saved before send
        self.assertEqual(list(self.table.scan()), [])

        self.batch.send()
        self.assertEqual(list(self.table.scan()), [
            ('godfather', {'d:title': 'The Godfather'}),
            ('inception', {'d:title': 'Inception'})
        ])
