===============
Cost in EXPLAIN
===============

During planning cost is computed for each plan node based on root table statistics.
Cost is exposed in output of ``EXPLAIN`` statement.

Cost structure consist of two fields:
 * output rows count - expected number of rows outputted by plan node during execution
 * output size - expected amount of data in bytes outputted by plan node

Example output:

.. code-block:: none

    presto:default> EXPLAIN SELECT comment FROM nation_with_column_stats WHERE nationkey > 3

    - Output[comment] => [comment:varchar(152)] {rows: ?, bytes: ?}
    - RemoteExchange[GATHER] => comment:varchar(152) {rows: 12, bytes: ?}
        - ScanFilterProject[table = hive:hive:default:nation_with_column_stats,
                                    originalConstraint = (""nationkey"" > BIGINT '3'),
                                    filterPredicate = (""nationkey"" > BIGINT '3')] => [comment:varchar(152)] {rows: 25, bytes: ?}/{rows: 12, bytes: ?}/{rows: 12, bytes: ?}
                LAYOUT: hive
                nationkey := HiveColumnHandle{clientId=hive, name=nationkey, hiveType=bigint, hiveColumnIndex=0, columnType=REGULAR}
                comment := HiveColumnHandle{clientId=hive, name=comment, hiveType=varchar(152), hiveColumnIndex=3, columnType=REGULAR}


The cost information is exposed usign ``{rows: XX, bytes: XX}`` snippets in printed plan tree.
``rows`` denotes output rows count and ``bytes`` denotes output size.
If one of the values is not know the ``?`` is printed.

Generally there is single cost section printed per plan node.
Exception from that rule is ``Scan`` operator which can be combined with ``Filter`` and/or ``Project``. Then multiple cost structures will be printed.
Each one will correspond to individual logical part of combined meta operator.
E.g. for ``ScanFilterProject`` operator three cost structures will be printed.

 * first will correspond to ``Scan`` part of operator
 * second will correspond to ``Filter`` part of opertor
 * third will corresponde to ``Project`` part of operator

Cost will also be printed in ``EXPLAIN ANALYZE`` output.

