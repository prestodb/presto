===============
Cost in EXPLAIN
===============

During planning, the cost associated with each node of the plan is computed
based on the table statistics for the tables in the query. This calculated
cost is printed as part of the output of an :doc:`/sql/explain` statement.

Cost information is displayed in the plan tree using the format ``{rows: XX
(XX), cpu: XX, memory: XX, network: XX}``.  ``rows`` refers to the expected
number of rows output by each plan node during execution.  The value in the
parentheses following the number of rows refers to the expected size of the data
output by each plan node in bytes. Other parameters indicate the estimated
amount of CPU, memory, and network utilized by the execution of a plan node.
These values do not represent any actual unit, but are numbers that are used to
compare the relative costs between plan nodes, allowing the optimizer to choose
the best plan for executing a query. If any of the values is not known, a ``?``
is printed.

For example:

.. code-block:: none

 presto:default> EXPLAIN SELECT comment FROM tpch.sf1.nation WHERE nationkey > 3;

 - Output[comment] => [[comment]]
         Estimates: {rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 1734.25}
     - RemoteExchange[GATHER] => [[comment]]
             Estimates: {rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 1734.25}
         - ScanFilterProject[table = tpch:nation:sf1.0, filterPredicate = ("nationkey" > BIGINT '3')] => [[comment]]
                 Estimates: {rows: 25 (1.94kB), cpu: 2207.00, memory: 0.00, network: 0.00}/{rows: 22 (1.69kB), cpu: 4414.00, memory: 0.00, network: 0.00}/{rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 0.00}
                 nationkey := tpch:nationkey
                 comment := tpch:comment

Generally, there is only one cost printed for each plan node.  However, when a
``Scan`` operator is combined with a ``Filter`` and/or ``Project`` operator,
then multiple cost structures will be printed, each corresponding to an
individual logical part of the combined operator. For example, three cost
structures will be printed for a ``ScanFilterProject`` operator, corresponding
to the ``Scan``, ``Filter``, and ``Project`` parts of the operator, in that order.

Estimated cost is also printed in :doc:`/sql/explain-analyze` in addition to actual
runtime statistics.

