===============
Cost in EXPLAIN
===============

During planning, the cost associated with each node of the plan is computed based on the root table statistics
for the tables in the query. This calculated cost is printed as part of the output of an ``EXPLAIN`` statement.

Cost information is displayed in the plan tree using the format ``{rows: XX, bytes: XX}``.  ``rows`` refers to the
expected number of rows output by each plan node during execution.  ``bytes`` refers to the expected size of the
data output by each plan node in bytes. If any of the values is not known, a ``?`` is printed.

For example:

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

Generally there is only one cost printed for each plan node.
However, when a ``Scan`` operator is combined with a ``Filter`` and/or ``Project`` operator, then multiple cost structures will be printed,
each corresponding to an individual logical part of the combined meta-operator.
For example, for a ``ScanFilterProject`` operator three cost structures will be printed.

 * the first will correspond to ``Scan`` part of operator
 * the second will correspond to ``Filter`` part of opertor
 * the third will corresponde to ``Project`` part of operator

Cost is also printed in ``EXPLAIN ANALYZE`` output.

