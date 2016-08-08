==================
InMemory Connector
==================

The InMemory connector stores all data and metadata in RAM on workers
and both are discarded when Presto restarts.

.. warning::

    This connector is in early experimental stage it is not recommended
    to use it in a production environment. The intended use of this
    connector at this point is purely for testing.

.. warning::

    This connector does not support ``DROP TABLE`` or any other method
    to release held resources. Once table is populated it will consume
    memory until next Presto restart.

.. warning::

    When one worker fails/restarts all data that were stored in it's
    memory will be lost forever. In other words in case of worker
    crash/stop/restart there will be partial and silent data loss.
    If all worker nodes fail/restart when coordinator is keep
    running tables will end up empty.

.. warning::

    When coordinator fails/restarts all metadata about tables will
    be lost, but tables' data will be still present on the workers
    however they will be inaccessible.

.. warning::

    This connector will not work properly with multiple coordinators,
    since each coordinator will have a different metadata.

Configuration
-------------

To configure the InMemory connector, create a catalog properties file
``etc/catalog/inmemory.properties`` with the following contents:

.. code-block:: none

    connector.name=inmemory

Examples
--------

Create a table using the InMemory connector::

    CREATE TABLE inmemory.default.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the InMemory connector::

    INSERT INTO inmemory.default.nation
    SELECT * FROM tpch.tiny.nation;

Select from the InMemory connector::

    SELECT * FROM inmemory.default.nation;
