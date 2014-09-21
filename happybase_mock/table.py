import time


def _check_table_existence(method):
    def wrap(table, *args, **kwargs):
        if table.name not in table.connection._tables:
            raise IOError('TableNotFoundException: %s' % table.name)
        return method(table, *args, **kwargs)
    return wrap


class Table(object):

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
        self._enabled = True
        self._data = {}

    def __repr__(self):
        return '<%s.%s name=%r>' % (
            __name__,
            self.__class__.__name__,
            self.name,
        )

    def families(self):
        return self._families

    def regions(self):
        pass

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        data = self._data.get(row, {})
        result = {}

        if not columns:
            columns = data.keys()

        for colname in columns:
            if colname in data:
                last_timestamp = sorted(data[colname].keys())[-1]
                result[colname] = data[colname][last_timestamp]

        return result

    def rows(self, rows, columns=None, timestamp=None,
             include_timestamp=False):
        pass

    def cells(self, row, column, versions=None, timestamp=None,
              include_timestamp=False):
        pass

    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, filter=None, timestamp=None,
             include_timestamp=False, batch_size=1000, scan_batching=None,
             limit=None, sorted_columns=False):
        pass

    @_check_table_existence
    def put(self, row, data, timestamp=None, wal=True):
        # Check data against column families
        for colname in data:
            cf = colname.split(':')[0]
            if cf not in self._families:
                raise IOError('NoSuchColumnFamilyException: %s' % cf)

        if timestamp is None:
            timestamp = int(time.time() * 1000)

        columns = self._data.get(row)
        if columns is None:
            columns = {}
            self._data[row] = columns

        for colname, value in data.iteritems():
            column = columns.get(colname)
            if column is None:
                column = {}
                columns[colname] = column

            column[timestamp] = value

    def delete(self, row, columns=None, timestamp=None, wal=True):
        pass

    def batch(self, timestamp=None, batch_size=None, transaction=False,
              wal=True):
        pass

    def counter_get(self, row, column):
        pass

    def counter_set(self, row, column, value=0):
        pass

    def counter_inc(self, row, column, value=1):
        pass

    def counter_dec(self, row, column, value=1):
        pass

    def _set_families(self, families):
        # Default family options
        defaults = {
            'block_cache_enabled': False,
            'bloom_filter_nb_hashes': 0,
            'bloom_filter_type': 'NONE',
            'bloom_filter_vector_size': 0,
            'compression': 'NONE',
            'in_memory': False,
            'max_versions': 3,
            'time_to_live': -1
        }
        self._families = {}
        for name, opts in families.iteritems():
            family_options = defaults.copy()
            family_options['name'] = name
            family_options.update(opts)
            self._families[name] = family_options
