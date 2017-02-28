from .base import BaseTestCase
from happybase_mock import Connection


class TestBatch(BaseTestCase):

    def setUp(self):
        self.conn = Connection()
        self.conn.create_table('movie', {'d': dict()})
        self.table = self.conn.table('movie')
        self.batch = self.table.batch()

    def test_put(self):
        self.batch.put(b'matrix', {b'd:title': b'The Matrix'})
        self.batch.put(b'godfather', {b'd:title': b'The Godfather'})
        self.batch.put(b'inception', {b'd:title': b'Inception'})

        # No data is saved before send
        self.assertEqual(list(self.table.scan()), [])

        self.batch.send()
        self.assertEqual(list(self.table.scan()), [
            (b'godfather', {b'd:title': b'The Godfather'}),
            (b'inception', {b'd:title': b'Inception'}),
            (b'matrix', {b'd:title': b'The Matrix'})
        ])

    def test_delete(self):
        self.batch.put(b'matrix', {b'd:title': b'The Matrix'})
        self.batch.put(b'godfather', {b'd:title': b'The Godfather'})
        self.batch.put(b'inception', {b'd:title': b'Inception'})
        self.batch.delete(b'matrix')

        # No data is saved before send
        self.assertEqual(list(self.table.scan()), [])

        self.batch.send()
        self.assertEqual(list(self.table.scan()), [
            (b'godfather', {b'd:title': b'The Godfather'}),
            (b'inception', {b'd:title': b'Inception'})
        ])

    def test_context_manager(self):
        # Mutations is automatically sent at the end of `with` block
        with self.table.batch() as batch:
            batch.put(b'wizofoz', {b'd:title': b'The Wizard of Oz'})
            batch.put(b'frozen', {b'd:title': b'Frozen'})
            batch.put(b'goodfellas', {b'd:title': b'Goodfellas'})
            batch.delete(b'wizofoz')

        self.assertEqual(list(self.table.scan()), [
            (b'frozen', {b'd:title': b'Frozen'}),
            (b'goodfellas', {b'd:title': b'Goodfellas'})
        ])
