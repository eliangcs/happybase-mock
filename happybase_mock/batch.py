class Batch(object):

    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=True):
        pass

    def send(self):
        pass

    def put(self, row, data, wal=None):
        pass

    def delete(self, row, columns=None, wal=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
