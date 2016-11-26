*****************
Logical Planner
*****************

Presto currently has a rule based optimizer. A given SQL query goes through syntactic
analysis, semantic analysis, logical planning, physical planning and then execution.
Syntactic analysis, or parsing, is used to break down the query into its component structures
and check that the query conforms to the syntactic rules of SQL. The next step is semantic analysis,
which gathers the semantic information from the query that is necessary for planning. It deals
with things like type checking and verifying that the column names used in the query exist for a given table.
The logical planner then takes this syntactic and semantic information for a query and using metadata
and session information creates a base logical plan.

Consider this example:

.. code-block:: sql

    SELECT nationkey FROM nation JOIN region ON nation.regionkey = region.regionkey WHERE nationkey > 9

For this query Presto will generate a logical base plan:

.. code-block:: none

            ProjectNode
                |
            FilterNode
                |
            JoinNode
               /   \
    ProjectNode     ProjectNode
        |               |
    TableScanNode   TableScanNode
    (nation)        (region)


The base logical plan is then optimized using a list of optimizer transformation rules like
predicate push down or pruning of unused columns to get the final logical plan. For example the
optimization predicate push down ensures that the predicates are applied as early in the plan
as possible to reduce the amount of data that has to be processed. In the given query, the filter
on nationkey column will be pushed below the `JoinNode` and the `ProjectNode` towards the `TableScanNode`
for nation table and the transformed plan will look like this:

.. code-block:: none

            ProjectNode
                |
            JoinNode
               /   \
    ProjectNode     ProjectNode
        |               |
    FilterNode      TableScanNode
        |           (region)
    TableScanNode
    (nation)

The final optimized logical plan is then fragmented into sub-plans during physical planning. These fragments
are shipped for execution to distributed nodes.
