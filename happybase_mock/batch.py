import functools


class Batch(object):

    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=True):
        self._table = table
        self._timestamp = timestamp
        self._partials = []

    def send(self):
        for p in self._partials:
            p()
        del self._partials[:]

    def put(self, *args, **kwargs):
        self._add_partial(self._table.put, *args, **kwargs)

    def delete(self, *args, **kwargs):
        self._add_partial(self._table.delete, *args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.send()

    def _add_partial(self, func, *args, **kwargs):
        kwargs['timestamp'] = self._timestamp
        p = functools.partial(func, *args, **kwargs)
        self._partials.append(p)
