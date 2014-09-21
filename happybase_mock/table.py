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

        # A multi-dimentional map, _data[rowkey][colname][timestamp] = value
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
                cell = data[colname]
                timestamps = sorted(cell.keys(), reverse=True)
                if timestamp is None:
                    # Use latest version if timestamp isn't specified
                    ts = timestamps[0]
                    result[colname] = cell[ts]
                else:
                    # Find the first ts < timestamp
                    for ts in timestamps:
                        if ts < timestamp:
                            result[colname] = cell[ts]
                            break

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

            # Check if it exceeds max_versions
            cf = colname.split(':')[0]
            max_versions = self._max_versions(cf)
            if len(column) > max_versions:
                # Delete cell with minimum timestamp
                del column[min(column.keys())]

    def delete(self, row, columns=None, timestamp=None, wal=True):
        if not columns and timestamp is None:
            # Delete whole row
            self._data.pop(row, None)
        elif row in self._data:
            data = self._data[row]
            if not columns:
                # Delete all columns if not specified
                columns = data.keys()

            if timestamp is None:
                timestamp = int(time.time() * 1000)

            to_be_deleted = []
            for colname in columns:
                for ts in data[colname]:
                    if ts <= timestamp:
                        to_be_deleted.append((colname, ts))

            for colname, ts in to_be_deleted:
                del data[colname][ts]
                if not data[colname]:
                    # Delete a column if it doesn't have any timestamps
                    del data[colname]

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

    def _max_versions(self, cf):
        return self._families[cf]['max_versions']

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
