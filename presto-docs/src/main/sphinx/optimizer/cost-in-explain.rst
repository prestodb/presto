===============
Cost in EXPLAIN
===============

During planning, the cost associated with each node of the plan is computed based on the root table statistics
for the tables in the query. This calculated cost is printed as part of the output of an ``EXPLAIN`` statement.

Cost information is displayed in the plan tree using the format ``{rows: XX (XX), cpu: XX, memory: XX, network: XX}``.
``rows`` refers to the expected number of rows output by each plan node during execution.  Value in the parentheses
following the number of rows refers to the expected size of the data output by each plan node in bytes. Other parameters
indicate estimated amount of cpu, memory and network component for a plan node. These values are not representative in
any actual unit but are numbers that are used for comparison between two plan nodes to choose which plan node will suit
better for executing a query. If any of the values is not known, a ``?`` is printed.

For example:

.. code-block:: none

 presto:default> EXPLAIN SELECT comment FROM nation_with_column_stats WHERE nationkey > 3

 - Output[comment] => [comment:varchar(152)]
         Cost: {rows: 22 (218B), cpu: 1218.75, memory: 0.00, network: 218.75}
     - RemoteExchange[GATHER] => comment:varchar(152)
             Cost: {rows: 22 (218B), cpu: 1218.75, memory: 0.00, network: 218.75}
         - ScanFilterProject[table = tpch:tpch:nation:sf1.0, originalConstraint = ("nationkey" > BIGINT '3'), filterPredicate = ("nationkey" > BIGINT '3')] => [comment:varchar(152)]
                 Cost: {rows: 25 (500B), cpu: 500.00, memory: 0.00, network: 0.00}/{rows: 22 (437B), cpu: 1000.00, memory: 0.00, network: 0.00}/{rows: 22 (218B), cpu: 1218.75, memory: 0.00, network: 0.00}
                 nationkey := tpch:nationkey
                 comment := tpch:comment

Generally there is only one cost printed for each plan node.
However, when a ``Scan`` operator is combined with a ``Filter`` and/or ``Project`` operator, then multiple cost structures will be printed,
each corresponding to an individual logical part of the combined meta-operator.
For example, for a ``ScanFilterProject`` operator three cost structures will be printed.

 * the first will correspond to ``Scan`` part of operator
 * the second will correspond to ``Filter`` part of opertor
 * the third will corresponde to ``Project`` part of operator

Estimated cost is also printed in ``EXPLAIN ANALYZE`` in addition to actual runtime statistics.

