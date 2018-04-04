import struct
import time
from six.moves import xrange
from six import iteritems

from .batch import Batch


def _check_table_existence(method):
    def wrap(table, *args, **kwargs):
        if not table._exists():
            raise IOError('TableNotFoundException: %s' % table.name)
        return method(table, *args, **kwargs)
    return wrap


# Copied from happybase.util
def _str_increment(s):
    if not isinstance(s, str):
        s = s.decode('utf-8')
    result = None
    for i in xrange(len(s) - 1, -1, -1):
        if s[i] != '\xff':
            result = s[:i] + chr(ord(s[i]) + 1)
            break
    return result.encode('utf-8')


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

    @_check_table_existence
    def families(self):
        return self._families

    def regions(self):
        if not self._exists():
            return []

        # Table.regions() is meaningless for in-memory mocking, so just return
        # some fake data
        return [{
            'end_key': '',
            'id': 1,
            'name': '%s,,1.1234' % self.name,
            'port': 60000,
            'server_name': 'localhost',
            'start_key': '',
            'version': 1
        }]

    @_check_table_existence
    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        if not isinstance(row, bytes):
            row = row.encode('utf-8')
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
                    if include_timestamp:
                        result[colname] = cell[ts], ts
                    else:
                        result[colname] = cell[ts]
                else:
                    # Find the first ts < timestamp
                    for ts in timestamps:
                        if ts < timestamp:
                            if include_timestamp:
                                result[colname] = cell[ts], ts
                            else:
                                result[colname] = cell[ts]
                            break
        return result

    @_check_table_existence
    def rows(self, rows, columns=None, timestamp=None,
             include_timestamp=False):
        result = []
        for row in rows:
            data = self.row(row, columns, timestamp, include_timestamp)
            result.append((row, data))
        return result

    @_check_table_existence
    def cells(self, row, column, versions=None, timestamp=None,
              include_timestamp=False):
        if not isinstance(row, bytes):
            row = row.encode('utf-8')
        if not isinstance(column, bytes):
            column = column.encode('utf-8')
        result = []
        timestamps = sorted(self._data.get(row, {}).get(column, {}).keys(),
                            reverse=True)
        for ts in timestamps:
            value = self._data[row][column][ts]
            if timestamp is None or ts < timestamp:
                if include_timestamp:
                    result.append((value, ts))
                else:
                    result.append(value)
        return result

    @_check_table_existence
    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, timestamp=None, include_timestamp=False,
             batch_size=1000, scan_batching=None, limit=None,
             reverse=False, sorted_columns=False, **kwargs):
        # encode columns key and data (for python3 compatibility)
        if columns:
          for i, col in enumerate(columns):
            if not isinstance(col, bytes):
                columns[i] = col.encode('utf-8')
        if row_prefix is not None:
            if not isinstance(row_prefix, bytes):
                row_prefix = row_prefix.encode('utf-8')
            if row_start is not None or row_stop is not None:
                raise TypeError(
                    "'row_prefix' cannot be combined with 'row_start' "
                    "or 'row_stop'")

            row_start = row_prefix
            row_stop = _str_increment(row_prefix)

        if row_start is None:
            row_start = b''
        else:
            if not isinstance(row_start, bytes):
                row_start = row_start.encode('utf-8')

        rows = filter(lambda k: k >= row_start, self._data)
        if row_stop is not None:
            if not isinstance(row_stop, bytes):
                row_stop = row_stop.encode('utf-8')
            rows = filter(lambda k: k < row_stop, rows)

        result = sorted([
            (row, self.row(row, columns, timestamp, include_timestamp))
            for row in rows
        ], reverse=reverse)

        if limit:
            if len(result) < limit:
                result = result[:limit]
        
        return iter(result)

    @_check_table_existence
    def put(self, row, data, timestamp=None, wal=True):
        # encode row key and data before put (for python3 compatibility)
        if not isinstance(row, bytes):
            row = row.encode('utf-8')
        data = {
            (k if isinstance(k, bytes) else k.encode('utf-8')):
            (v if isinstance(v, bytes) else v.encode('utf-8'))
            for k, v in iteritems(data)
        }

        # Check data against column families
        for colname in data:
            cf = colname.decode('utf-8').split(':')[0]
            if cf not in self._families:
                raise IOError('NoSuchColumnFamilyException: %s' % cf)

        if timestamp is None:
            timestamp = int(time.time() * 1000)

        columns = self._data.get(row)
        if columns is None:
            columns = {}
            self._data[row] = columns

        for colname, value in iteritems(data):
            column = columns.get(colname)
            if column is None:
                column = {}
                columns[colname] = column

            column[timestamp] = value

            # Check if it exceeds max_versions
            cf = colname.decode('utf-8').split(':')[0]
            max_versions = self._max_versions(cf)
            if len(column) > max_versions:
                # Delete cell with minimum timestamp
                del column[min(column.keys())]

    @_check_table_existence
    def delete(self, row, columns=None, timestamp=None, wal=True):
        if not isinstance(row, bytes):
            row = row.encode('utf-8')
        if columns:
            columns = [
                column if isinstance(column, bytes)
                else column.encode('utf-8')
                for column in columns
            ]
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
        return Batch(self, timestamp, batch_size, transaction, wal)

    def counter_get(self, row, column):
        # Decode as long integer, big endian
        value = self.row(row, (column,)).get(column)
        if not value:
            return 0
        return struct.unpack('>q', value)[0]

    @_check_table_existence
    def counter_set(self, row, column, value=0):
        # Encode as long integer, big endian
        value = struct.pack('>q', value)
        self.delete(row, (column,))
        self.put(row, {column: value})

    @_check_table_existence
    def counter_inc(self, row, column, value=1):
        orig_value = self.counter_get(row, column)
        self.counter_set(row, column, orig_value + value)

    @_check_table_existence
    def counter_dec(self, row, column, value=1):
        orig_value = self.counter_get(row, column)
        self.counter_set(row, column, orig_value - value)

    def _exists(self):
        return self.name in self.connection._tables

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
        for name, opts in iteritems(families):
            family_options = defaults.copy()
            family_options['name'] = name
            family_options.update(opts)
            self._families[name] = family_options
