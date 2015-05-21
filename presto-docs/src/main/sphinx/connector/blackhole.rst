====================
Black Hole Connector
====================

The Black Hole connector works like the /dev/null device file in the Linux operating system.
Metadata for any tables created via this connector is kept in memory and discarded when
Presto restarts. Tables are always empty, and any data written to them will be ignored.

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
