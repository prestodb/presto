====================
Black Hole Connector
====================

The Black Hole connector works in similar way as /dev/null device file in Linux operating system. 
SELECT statement from any of its tables will return no rows and INSERT will also be mocked and no real
writes will be performed.

All information (meta data) about created tables live only in memory, so when you restart the server they
will be discarded.

Configuration
-------------

To configure the Black Hole connector, create a catalog properties file
``etc/catalog/blackhole.properties`` with the following contents:

.. code-block:: none

    connector.name=blackhole

Example usage
-------------

Create table using blackhole connector:

.. code-block:: sql

    CREATE TABLE blackhole.default.nation AS SELECT * from tpch.tiny.nation;

Insert data into table in blackhole connector:

.. code-block:: sql

    INSERT INTO blackhole.default.nation SELECT * FROM tpch.tiny.nation;

Read from blackhole connector:

.. code-block:: sql

    SELECT COUNT(*) FROM blackhole.default.nation;
