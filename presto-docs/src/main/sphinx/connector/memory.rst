================
Memory Connector
================

The Memory connector stores all data and metadata in RAM on workers
and both are discarded when Presto restarts.


Configuration
-------------

To configure the Memory connector, create a catalog properties file
``etc/catalog/memory.properties`` with the following contents:

.. code-block:: none

    connector.name=memory
    memory.max-data-per-node=128MB

``memory.max-data-per-node`` defines memory limit for pages stored in this
connector per each node (default value is 128MB).

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


Memory Connector Limitations
----------------------------

    * After ``DROP TABLE`` memory is not released immediately. It is
      released after next write access to memory connector.
    * When one worker fails/restarts all data that were stored in its
      memory will be lost forever. To prevent silent data loss this
      connector will throw an error on any read access to such
      corrupted table.
    * When query fails for any reason during writing to memory table,
      table will be in undefined state. Such table should be dropped
      and recreated manually. Reading attempt from such table may fail
      or may return partial data.
    * When coordinator fails/restarts all metadata about tables will
      be lost, but tables' data will be still present on the workers
      however they will be inaccessible.
    * This connector will not work properly with multiple
      coordinators, since each coordinator will have a different
      metadata.
