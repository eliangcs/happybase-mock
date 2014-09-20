# Default values copied from happybase
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.96'


class Connection(object):

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
        pass

    def open(self):
        pass

    def close(self):
        pass

    def __del__(self):
        pass

    def table(self, name, use_prefix=True):
        pass

    def tables(self):
        pass

    def create_table(self, name, families):
        pass

    def delete_table(self, name, disable=False):
        pass

    def enable_table(self, name):
        pass

    def disable_table(self, name):
        pass

    def is_table_enabled(self, name):
        pass

    def compact_table(self, name, major=False):
        pass
