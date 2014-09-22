HappyBase-mock
==============

.. image:: https://badge.fury.io/py/happybase-mock.svg
    :target: http://badge.fury.io/py/happybase-mock

.. image:: https://travis-ci.org/eliangcs/happybase-mock.svg?branch=master
    :target: https://travis-ci.org/eliangcs/happybase-mock

.. image:: https://coveralls.io/repos/eliangcs/happybase-mock/badge.png?branch=master
    :target: https://coveralls.io/r/eliangcs/happybase-mock

A mocking library for HappyBase_.

Installing HBase_ is not easy. Running HBase_ also costs high system resource.
This library simulates HappyBase_ API in local memory, so you don't have to
set up HBase_. This is handy if you want to do fast in-memory testing.


Installation
------------

To install HappyBase-mock, just do::

    pip install happybase-mock


Usage
-----

The API and package structure of HappyBase-mock is a mimic of HappyBase_. They
are almost identical, so you can use it like you normally would do in
HappyBase_.

For example, you can replace ``happybase`` package with ``happybase_mock``.
Then all of the operations will be performed in memory::

    import happybase_mock as happybase

    pool = happybase.ConnectionPool(host='localhost', table_prefix='app')
    with pool.connection() as conn:
        table = conn.table('table_name')
        table.put('rowkey', {'d:data': 'value'})

TIP: You can also use Mock_ library to help you patch HappyBase_ on runtime.


.. _HappyBase: https://github.com/wbolster/happybase
.. _HBase: http://hbase.apache.org/
.. _Mock: http://www.voidspace.org.uk/python/mock/


Contribute
----------

Running Tests
~~~~~~~~~~~~~

Install test requirements::

    pip install -r requirements-test.txt

Then run the test::

    py.test
