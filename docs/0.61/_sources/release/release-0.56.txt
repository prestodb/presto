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

.. note::
    This is a backwards incompatible change to ``ConnectorMetadata`` in the SPI,
    so if you have written a connector, you will need to update your code before
    deploying this release. We recommend changing your connector to extend from
    the new ``ReadOnlyConnectorMetadata`` abstract base class unless you want to
    support table creation.

Cross Joins
-----------

Cross joins are supported using the standard ANSI SQL syntax::

    SELECT *
    FROM a
    CROSS JOIN b

Inner joins that result in a cross join due to the join criteria evaluating
to true at analysis time are also supported.
