===================
Velox in 10 minutes
===================

This is a quick introduction into Velox, the new C++ vectorized database
acceleration library aimed at optimizing query engines and data processing
systems. This tutorial was inspired by `TorchArrow in 10 minutes
<https://github.com/facebookresearch/torcharrow/blob/main/tutorial/tutorial.ipynb>`_
which itself was based on a `10 minutes to pandas tutorial <https://pandas.pydata.org/docs/user_guide/10min.html>`_.

We are going to touch the following major components:
 * data representation,
 * expression evaluation,
 * aggregations,
 * sorting,
 * joins,
 * accessing data via connectors.

Note: Velox doesn't provide a SQL interface, but in this tutorial to keep
things simple we are going to use SQL and utilities that were designed
to simplify test development. These utilities use `DuckDB <https://duckdb.org/>`_
to parse SQL.

The code used in this tutorial is available in `velox/exec/tests/VeloxIn10MinDemo.cpp <https://github.com/facebookincubator/velox/blob/main/velox/exec/tests/VeloxIn10MinDemo.cpp>`_.

Let’s get started.

Vectors
-------

Velox uses columnar representation of the data similar to Arrow called vectors.
Let’s create two vectors to store 64-bit integers and one vector to store strings.

.. code-block:: c++

    auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
    auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
    auto dow = makeFlatVector<std::string>({"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"});

And let’s print out the contents of these vectors:

.. code-block:: c++

    auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

    std::cout << data->toString() << std::endl;
    std::cout << data->toString(0, data->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT,dow:VARCHAR>: 7 elements, no nulls]
    0: {0, 0, monday}
    1: {1, 5, tuesday}
    2: {2, 10, wednesday}
    3: {3, 15, thursday}
    4: {4, 20, friday}
    5: {5, 25, saturday}
    6: {6, 30, sunday}

Check out :doc:`Vectors <develop/vectors>` guide to learn more about
Velox vectors.

Expressions
-----------

Now, let’s compute a sum of 'a' and 'b' by evaluating 'a + b' expression.

First we use DuckDB SQL parser to parse the expression into a fully typed
expression tree. Then, we use Velox APIs to compile the expression tree into
an executable ExprSet. Check out the implementation of the compileExpression
helper method in VeloxIn10MinDemo.cpp for details.

.. code-block:: c++

    auto exprSet = compileExpression("a + b", asRowType(data->type()));

Let's print out the compiled expression tree:

.. code-block:: c++

    std::cout << exprSet->toString(false /*compact*/) << std::endl;

.. code-block::

    plus -> BIGINT [#1]
       a -> BIGINT [#2]
       b -> BIGINT [#3]

The expression tree consists of a root node "plus", which represents
a function call, and two child nodes "a" and "b", which represent field
access.

Now we are ready to evaluate the expression on a batch of data.

.. code-block:: c++

    auto c = evaluate(*exprSet, data);

And here are the results:

.. code-block:: c++

    auto abc = makeRowVector({"a", "b", "c"}, {a, b, c});

    std::cout << abc->toString() << std::endl;
    std::cout << abc->toString(0, c->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT,c:BIGINT>: 7 elements, no nulls]
    0: {0, 0, 0}
    1: {1, 5, 6}
    2: {2, 10, 12}
    3: {3, 15, 18}
    4: {4, 20, 24}
    5: {5, 25, 30}
    6: {6, 30, 36}

Let's try a slightly more complex expression: `2 * a + b % 3`.

.. code-block:: c++

    exprSet = compileExpression("2 * a + b % 3", asRowType(data->type()));
    std::cout << exprSet->toString(false /*compact*/) << std::endl;

.. code-block::

    plus -> BIGINT [#1]
       multiply -> BIGINT [#2]
          2:BIGINT -> BIGINT [#3]
          a -> BIGINT [#4]
       mod -> BIGINT [#5]
          b -> BIGINT [#6]
          3:BIGINT -> BIGINT [#7]

.. code-block:: c++

    auto d = evaluate(*exprSet, data);

    auto abd = makeRowVector({"a", "b", "d"}, {a, b, d});
    std::cout << abd->toString() << std::endl;
    std::cout << abd->toString(0, d->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT,d:BIGINT>: 7 elements, no nulls]
    0: {0, 0, 0}
    1: {1, 5, 4}
    2: {2, 10, 5}
    3: {3, 15, 6}
    4: {4, 20, 10}
    5: {5, 25, 11}
    6: {6, 30, 12}

Let's transform 'dow' column into a 3-letter prefix with first letter
capitalized, e.g. Mon, Tue, etc.

.. code-block:: c++

    exprSet = compileExpression(
          "concat(upper(substr(dow, 1, 1)), substr(dow, 2, 2))",
          asRowType(data->type()));
    std::cout << exprSet->toString(false /*compact*/) << std::endl;

.. code-block::

    concat -> VARCHAR [#1]
       upper -> VARCHAR [#2]
          substr -> VARCHAR [#3]
             dow -> VARCHAR [#4]
             1:BIGINT -> BIGINT [#5]
             1:BIGINT -> BIGINT [CSE #5]
       substr -> VARCHAR [#6]
          dow -> VARCHAR [CSE #4]
          2:BIGINT -> BIGINT [#7]
          2:BIGINT -> BIGINT [CSE #7]

.. code-block:: c++

    auto shortDow = evaluate(*exprSet, data);
    std::cout << shortDow->toString() << std::endl;
    std::cout << shortDow->toString(0, shortDow->size()) << std::endl;

.. code-block::

    > [FLAT VARCHAR: 7 elements, no nulls]
    0: Mon
    1: Tue
    2: Wed
    3: Thu
    4: Fri
    5: Sat
    6: Sun

You can construct and evaluate arbitrary SQL expressions using :doc:`Presto
functions <functions>` and special forms like IF, AND, OR, COALESCE, TRY and more.
If a function you need is not available, feel free to add a new one by following
the :doc:`How to add a scalar function? <develop/scalar-functions>` guide.

Check out :doc:`Expression Evaluation <develop/expression-evaluation>` article
to learn what happens under the hood or play with `velox/examples/ExpressionEval.cpp
<https://github.com/facebookincubator/velox/blob/main/velox/examples/ExpressionEval.cpp>`_
which contains a detailed step-by-step example of constructing and evaluating
expression trees using non-test library APIs.

Queries
-------

Aggregations
~~~~~~~~~~~~

We can calculate sum and average of 'a' and 'b' by creating and
executing a query plan with an aggregation node:

.. code-block:: c++

    auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {"sum(a) AS sum_a",
                       "avg(a) AS avg_a",
                       "sum(b) AS sum_b",
                       "avg(b) AS avg_b"})
                  .planNode();

    auto sumAvg = getResults(plan);

And here are the results:

.. code-block:: c++

    std::cout << sumAvg->toString() << std::endl;
    std::cout << sumAvg->toString(0, sumAvg->size()) << std::endl;

.. code-block::

    > [ROW ROW<sum_a:BIGINT,avg_a:DOUBLE,sum_b:BIGINT,avg_b:DOUBLE>: 1 elements, no nulls]
    0: {15, 2.5, 75, 12.5}

You can use any of the available :doc:`Presto aggregate functions <functions/aggregate>`
or add a new one by following :doc:`How to add an aggregate function? <develop/aggregate-functions>`
guide. Check out :doc:`Aggregations <develop/aggregations>` article for a deep dive
into aggregation-specific optimizations available in Velox.

Sorting
~~~~~~~

We can sort data using the OrderBy plan node.

.. code-block:: c++

    plan = PlanBuilder()
        .values({data})
        .orderBy({"a DESC"}, false /*isPartial*/)
        .planNode();

    auto sorted = getResults(plan);
    std::cout << sorted->toString() << std::endl;
    std::cout << sorted->toString(0, sorted->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT>: 6 elements, no nulls]
    0: {5, 25}
    1: {4, 20}
    2: {3, 15}
    3: {2, 10}
    4: {1, 5}
    5: {0, 0}

And we can get top 3 rows using TopN node:

.. code-block:: c++

    plan = PlanBuilder()
        .values({data})
        .topN({"a DESC"}, 3, false /*isPartial*/)
        .planNode();

    auto top3 = getResults(plan);
    std::cout << top5->toString() << std::endl;
    std::cout << top5->toString(0, top3->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT>: 3 elements, no nulls]
    0: {5, 25}
    1: {4, 20}
    2: {3, 15}

Filtering
~~~~~~~~~

We can filter data using Filter node.

.. code-block:: c++

    plan = PlanBuilder().values({data}).filter("a % 2 == 0").planNode();

    auto evenA = AssertQueryBuilder(plan).copyResults(pool());
    std::cout << std::endl << "> rows with even values of 'a': " << evenA->toString() << std::endl;
    std::cout << evenA->toString(0, evenA->size()) << std::endl;

.. code-block::

    > [ROW ROW<a:BIGINT,b:BIGINT,dow:VARCHAR>: 4 elements, no nulls]
    0: {0, 0, monday}
    1: {2, 10, wednesday}
    2: {4, 20, friday}
    3: {6, 30, sunday}

Connectors
~~~~~~~~~~

We have seen how to use Velox to perform computation on in-memory vectors
provided by the caller. Velox can also pull data from connectors. There are
two connectors to choose from. Hive connector reads DWRF and Parquet
files. TPC-H connector generates TPC-H tables on the fly.

Let's read from TPC-H nation table. We need to use a TableScan plan node and
provide a split.

.. code-block:: c++

  plan = PlanBuilder()
             .tableScan(
                 tpch::Table::TBL_NATION,
                 {"n_nationkey", "n_name"},
                 1 /*scaleFactor*/)
             .planNode();

  auto nations = AssertQueryBuilder(plan).split(makeTpchSplit()).copyResults(pool());

  std::cout << std::endl
            << "> first 10 rows from TPC-H nation table: "
            << nations->toString() << std::endl;
  std::cout << nations->toString(0, 10) << std::endl;

.. code-block::

    0: {0, ALGERIA}
    1: {1, ARGENTINA}
    2: {2, BRAZIL}
    3: {3, CANADA}
    4: {4, EGYPT}
    5: {5, ETHIOPIA}
    6: {6, FRANCE}
    7: {7, GERMANY}
    8: {8, INDIA}
    9: {9, INDONESIA}

Joins
~~~~~

We can now join TPC-H nation and region tables to count number of nations in
each region and sort results by region name. We need to use one TableScan node
for nations table and another for region table. We also need to provide splits
for each TableScan node. We will use two PlanBuilders: one for the probe
side of the join and another one for the build side. We will also use
PlanNodeIdGenerator to ensure that all plan nodes in the final plan have unique
IDs.

.. code-block:: c++

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(
                 tpch::Table::TBL_NATION, {"n_regionkey"}, 1 /*scaleFactor*/)
             .capturePlanNodeId(nationScanId)
             .hashJoin(
                 {"n_regionkey"},
                 {"r_regionkey"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(
                         tpch::Table::TBL_REGION,
                         {"r_regionkey", "r_name"},
                         1 /*scaleFactor*/)
                     .capturePlanNodeId(regionScanId)
                     .planNode(),
                 "", // extra filter
                 {"r_name"})
             .singleAggregation({"r_name"}, {"count(1) as nation_cnt"})
             .orderBy({"r_name"}, false)
             .planNode();

  auto nationCnt = AssertQueryBuilder(plan)
                       .split(nationScanId, makeTpchSplit())
                       .split(regionScanId, makeTpchSplit())
                       .copyResults(pool());

  std::cout << std::endl
            << "> number of nations per region in TPC-H: "
            << nationCnt->toString() << std::endl;
  std::cout << nationCnt->toString(0, 10) << std::endl;

.. code-block::

    0: {AFRICA, 5}
    1: {AMERICA, 5}
    2: {ASIA, 5}
    3: {EUROPE, 5}
    4: {MIDDLE EAST, 5}

Check out :doc:`Joins <develop/joins>` to learn more about joins and
join-specific optimizations in Velox.

You can mix and match as many plan nodes as you need in a query plan. The
list of available plan nodes can be found in  :doc:`Plan Nodes and Operators <develop/operators>`
or in PlanNodeId.h and PlanBuilder.h files in the code.

Curious about reading data from a Hive connector? Check out
`velox/examples/ScanAndSort.cpp <https://github.com/facebookincubator/velox/blob/main/velox/examples/ScanAndSort.cpp>`_
example in the code.
