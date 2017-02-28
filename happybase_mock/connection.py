from .table import Table


# Default values copied from happybase
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.96'


class _Singleton(type):
    """A python 2/3 compatible Singleton class
    Based on:
    https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    A dict that stores singleton instances, where key is
    'host:port/table_prefix', value is Connection object
    """
    _instances = {}

    @classmethod
    def _get_instance_id(cls, host=DEFAULT_HOST, port=DEFAULT_PORT,
                         table_prefix=None, table_prefix_separator='_'):
        if table_prefix:
            table_prefix += table_prefix_separator
        else:
            table_prefix = ''

        return '%s:%s/%s' % (host, port, table_prefix)

    def __call__(cls, *args, **kwargs):
        instance_id = cls._get_instance_id(**kwargs)
        if instance_id not in cls._instances:
            cls._instances[instance_id] = super(_Singleton, cls).__call__(
                *args, **kwargs)
        return cls._instances[instance_id]


class Singleton(_Singleton('SingletonMeta', (object,), {})):
    pass


class Connection(Singleton):

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
        if not hasattr(self, '_tables'):
            self._tables = {}

        # TODO: check if connection is opened on some methods
        self._opened = False
        if autoconnect:
            self.open()

    def open(self):
        self._opened = True

    def close(self):
        self._opened = False

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
        names = self._tables.keys()

        # Filter using prefix, and strip prefix from names
        if self.table_prefix is not None:
            prefix = self._table_name('')
            offset = len(prefix)
            names = [n[offset:] for n in names if n.startswith(prefix)]

        return sorted(names)

    def create_table(self, name, families):
        name = self._table_name(name)

        table = Table(name, self)
        table._set_families(families)
        self._tables[name] = table

    def delete_table(self, name, disable=False):
        fullname = self._table_name(name)
        table = self._tables.get(fullname)
        if not table:
            raise IOError('table does not exist')

        if disable:
            self.disable_table(name)

        if self.is_table_enabled(name):
            raise IOError('TableNotDisabledException: %s' % name)

        del self._tables[fullname]

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
