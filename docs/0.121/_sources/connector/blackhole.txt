====================
Black Hole Connector
====================

The Black Hole connector works like the ``/dev/null`` device on Unix-like
operating systems. Metadata for any tables created via this connector is
kept in memory on the coordinator and discarded when Presto restarts.
Tables are always empty, and any data written to them will be ignored.

.. warning::

    This connector will not work properly with multiple coordinators,
    since each coordinator will have a different metadata.

Configuration
-------------

To configure the Black Hole connector, create a catalog properties file
``etc/catalog/blackhole.properties`` with the following contents:

.. code-block:: none

    connector.name=blackhole

Examples
--------

Create a table using the blackhole connector::

    CREATE TABLE blackhole.test.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the blackhole connector::

    INSERT INTO blackhole.test.nation
    SELECT * FROM tpch.tiny.nation;

Select from the blackhole connector::

    SELECT COUNT(*) FROM blackhole.test.nation;

The above query will always return ``0``.
