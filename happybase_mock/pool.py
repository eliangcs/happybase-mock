import contextlib

from .connection import Connection


class ConnectionPool(object):

    def __init__(self, size, **kwargs):
        self._conn = Connection(**kwargs)

    @contextlib.contextmanager
    def connection(self, timeout=None):
        self._conn.open()
        yield self._conn
