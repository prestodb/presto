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

SQL Support
-----------

The Memory connector allows querying and creating tables and schemas in memory. Here are some examples of the SQL operations supported:

CREATE SCHEMA
^^^^^^^^^^^^^

Create a new schema named ``default1``:

.. code-block:: sql

     CREATE SCHEMA memory.default1;

CREATE TABLE
^^^^^^^^^^^^

Create a new table named ``my_table`` in the ``default1`` schema:

.. code-block:: sql

    CREATE TABLE memory.default1.my_table (id integer, name varchar, age integer);

INSERT INTO
^^^^^^^^^^^

Insert data into the ``my_table`` table:

.. code-block:: sql

    INSERT INTO memory.default1.my_table (id, name, age) VALUES (1, 'John Doe', 30);

SELECT
^^^^^^

Select data from the ``my_table`` table:

.. code-block:: sql

   SELECT * FROM memory.default1.my_table;

DROP TABLE
^^^^^^^^^^

To delete an existing table:

.. code-block:: sql

    DROP TABLE memory.default.nation;

.. note:: After using ``DROP TABLE``, memory is not released immediately. It is released after the next write access to the memory connector.

ALTER VIEW
^^^^^^^^^^

Alter view operations to alter the name of an existing view to a new name is supported in the Memory connector.

.. code-block:: sql

    ALTER VIEW memory.default.nation RENAME TO memory.default.new_nation;

Memory Connector Limitations
----------------------------

The following SQL statements are not supported:

* :doc:`/sql/alter-table`
* :doc:`/sql/delete`
* :doc:`/sql/update`

Limitations
^^^^^^^^^^^

* When one worker fails or restarts, all data stored in its
  memory is lost forever. To prevent silent data loss, this
  connector generates an error on any read access to such a
  corrupted table.
* When a query fails for any reason during writing to memory table,
  the table is in undefined state. Such a table should be dropped
  and recreated manually. Reading from such tables may fail
  or may return partial data.
* When the coordinator fails or restarts, all metadata about tables is
  lost. The tables' data is still present on the workers,
  but that data is inaccessible.
* This connector will not work properly with multiple
  coordinators, because each coordinator has a different
  metadata.
