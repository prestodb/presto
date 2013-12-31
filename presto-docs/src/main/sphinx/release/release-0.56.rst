============
Release 0.56
============

Table Creation
--------------

Tables can be created from the result of a query::

    CREATE TABLE orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Tables are created in Hive without partitions (unpartitioned) and use
RCFile with the Binary SerDe (``LazyBinaryColumnarSerDe``) as this is
currently the best format for Presto.

Cross Joins
-----------

Cross joins are supported using the standard ANSI SQL syntax::

    SELECT *
    FROM a
    CROSS JOIN b
