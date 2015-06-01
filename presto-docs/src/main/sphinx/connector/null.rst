==============
NULL Connector
==============

The NULL connector works in similar way as /dev/null. Reads from any
table will return no rows and writes will also be mocked and no real
writes will be performed.

All created tables live only in memory, so when you restart server they
will be discarded.

Example usage
-------------

Create table using null connector::

    CREATE TABLE "null".default.nation AS SELECT * from tpch.tiny.nation;

Insert data to table in null connector::

    INSERT INTO "null".default.nation SELECT * FROM tpch.tiny.nation;

Read from null connector::

    SELECT COUNT(*) FROM "null".default.nation; => 0