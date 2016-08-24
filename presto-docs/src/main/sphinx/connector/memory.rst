================
Memory Connector
================

The Memory connector stores all data and metadata in RAM on workers
and both are discarded when Presto restarts.

.. warning::

    This connector is in early experimental stage it is not recommended
    to use it in a production environment. The intended use of this
    connector at this point is purely for testing.

.. warning::

    After ``DROP TABLE`` memory is not released immediately. It is released
    after next write access to memory connector.

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

To configure the Memory connector, create a catalog properties file
``etc/catalog/memory.properties`` with the following contents:

.. code-block:: none

    connector.name=memory

Examples
--------

Create a table using the Memory connector::

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the Memory connector::

    INSERT INTO memory.default.nation
    SELECT * FROM tpch.tiny.nation;

Select from the Memory connector::

    SELECT * FROM memory.default.nation;

Drop table::

    DROP TABLE memory.default.nation;
