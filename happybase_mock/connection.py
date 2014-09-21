from .table import Table


# Default values copied from happybase
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.96'


class Connection(object):

    # A dict that stores singleton instances, where key is
    # 'host:port/table_prefix', value is Connection object
    _instances = {}

    @classmethod
    def _get_instance_id(cls, host=DEFAULT_HOST, port=DEFAULT_PORT,
                         table_prefix=None, table_prefix_separator='_'):
        if table_prefix:
            table_prefix += table_prefix_separator
        else:
            table_prefix = ''

        return '%s:%s/%s' % (host, port, table_prefix)

    def __new__(cls, *args, **kwargs):
        instance_id = cls._get_instance_id(**kwargs)
        if instance_id not in cls._instances:
            cls._instances[instance_id] = (
                super(Connection, cls).__new__(cls, *args, **kwargs))

        return cls._instances[instance_id]

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None,
                 autoconnect=True, table_prefix=None,
                 table_prefix_separator='_', compat=DEFAULT_COMPAT,
                 transport=DEFAULT_TRANSPORT):
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat

        # key: table name, value: Table object
        self._tables = {}

    def open(self):
        pass

    def close(self):
        pass

    def __del__(self):
        # Delete self from Connection._instances
        instance_id = Connection._get_instance_id(
            self.host, self.port, self.table_prefix,
            self.table_prefix_separator)
        Connection._instances.pop(instance_id, None)

    def table(self, name, use_prefix=True):
        if use_prefix:
            name = self._table_name(name)
        return self._tables.get(name) or Table(name, self)

    def tables(self):
        pass

    def create_table(self, name, families):
        name = self._table_name(name)

        table = Table(name, self)
        table._set_families(families)
        self._tables[name] = table

    def delete_table(self, name, disable=False):
        pass

    def enable_table(self, name):
        name = self._table_name(name)
        try:
            table = self._tables[name]
        except KeyError:
            raise IOError('TableNotFoundException: %s', name)
        table._enabled = True

    def disable_table(self, name):
        name = self._table_name(name)
        try:
            table = self._tables[name]
        except KeyError:
            raise IOError('TableNotFoundException: %s' % name)
        table._enabled = False

    def is_table_enabled(self, name):
        name = self._table_name(name)
        try:
            table = self._tables[name]
        except KeyError:
            return True
        return table._enabled

    def compact_table(self, name, major=False):
        pass

    def _table_name(self, name):
        if self.table_prefix is None:
            return name

        return self.table_prefix + self.table_prefix_separator + name
