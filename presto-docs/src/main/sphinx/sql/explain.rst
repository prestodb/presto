=======
EXPLAIN
=======

Synopsis
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

where ``option`` can be one of:

.. code-block:: none

    FORMAT { TEXT | GRAPHVIZ | JSON }
    TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }

Description
-----------

Show the logical or distributed execution plan of a statement, or validate the statement.
Use ``TYPE DISTRIBUTED`` option to display a fragmented plan. Each
`plan fragment <https://prestodb.io/docs/current/overview/concepts.html#plan-fragment>`_
is executed by a single or multiple Presto nodes. Fragment type specifies how the fragment
is executed by Presto nodes and how the data is distributed between fragments:

``SINGLE``
    Fragment is executed on a single node.

``HASH``
    Fragment is executed on a fixed number of nodes with the input data
    distributed using a hash function.

``ROUND_ROBIN``
    Fragment is executed on a fixed number of nodes with the input data
    distributed in a round-robin fashion.

``BROADCAST``
    Fragment is executed on a fixed number of nodes with the input data
    broadcast to all nodes.

``SOURCE``
    Fragment is executed on nodes where input splits are accessed.

Examples
--------

Logical plan:

.. code-block:: none

    presto:tiny> EXPLAIN SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                                    Query Plan
    ----------------------------------------------------------------------------------------------------------
     - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
             _col1 := count
         - RemoteExchange[GATHER] => regionkey:bigint, count:bigint
             - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                    count := "count"("count_8")
                 - LocalExchange[HASH][$hashvalue] ("regionkey") => regionkey:bigint, count_8:bigint, $hashvalue:bigint
                     - RemoteExchange[REPARTITION][$hashvalue_9] => regionkey:bigint, count_8:bigint, $hashvalue_9:bigint
                         - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
                                 $hashvalue_10 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
                             - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                                     count_8 := "count"(*)
                                 - TableScan[tpch:tpch:nation:sf0.1, originalConstraint = true] => [regionkey:bigint]
                                         regionkey := tpch:regionkey

Distributed plan:

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                              Query Plan
    ----------------------------------------------------------------------------------------------
     Fragment 0 [SINGLE]
         Output layout: [regionkey, count]
         Output partitioning: SINGLE []
         - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
                 _col1 := count
             - RemoteSource[1] => [regionkey:bigint, count:bigint]

     Fragment 1 [HASH]
         Output layout: [regionkey, count]
         Output partitioning: SINGLE []
         - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                 count := "count"("count_8")
             - LocalExchange[HASH][$hashvalue] ("regionkey") => regionkey:bigint, count_8:bigint, $hashvalue:bigint
                 - RemoteSource[2] => [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]

     Fragment 2 [SOURCE]
         Output layout: [regionkey, count_8, $hashvalue_10]
         Output partitioning: HASH [regionkey][$hashvalue_10]
         - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
                 $hashvalue_10 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
             - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                     count_8 := "count"(*)
                 - TableScan[tpch:tpch:nation:sf0.1, originalConstraint = true] => [regionkey:bigint]
                         regionkey := tpch:regionkey

Validate:

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE VALIDATE) SELECT regionkey, count(*) FROM nation GROUP BY 1;
     result
    -------
     true

IO:

.. code-block:: none


    presto:hive> EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_nation SELECT * FROM nation WHERE regionkey = 2;
                Query Plan
    -----------------------------------
     {
       "inputTableColumnInfos" : [ {
         "table" : {
           "catalog" : "hive",
           "schemaTable" : {
             "schema" : "tpch",
             "table" : "nation"
           }
         },
         "columns" : [ {
           "columnName" : "regionkey",
           "type" : "bigint",
           "domain" : {
             "nullsAllowed" : false,
             "ranges" : [ {
               "low" : {
                 "value" : "2",
                 "bound" : "EXACTLY"
               },
               "high" : {
                 "value" : "2",
                 "bound" : "EXACTLY"
               }
             } ]
           }
         } ]
       } ],
       "outputTable" : {
         "catalog" : "hive",
         "schemaTable" : {
           "schema" : "tpch",
           "table" : "test_nation"
         }
       }
     }

DDL Statements
^^^^^^^^^^^^^^

``EXPLAIN`` can also be used with DDL statements such as ``CREATE TABLE`` and ``DROP TABLE``.
For these statements, the output shows a summary of the operation rather than an execution plan.
This is useful for validating DDL syntax and understanding what operation will be performed
without actually executing it.

CREATE TABLE
""""""""""""

The ``EXPLAIN CREATE TABLE`` statement shows a summary of the table creation operation.
It validates the syntax and table structure without actually creating the table.

Syntax
''''''

.. code-block:: none

    EXPLAIN CREATE TABLE [ IF NOT EXISTS ] table_name (
      column_name data_type [NOT NULL] [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
      [, ...]
    )
    [ COMMENT table_comment ]
    [ WITH ( property_name = expression [, ...] ) ]

CREATE TABLE Examples
'''''''''''''''''''''

Basic table creation:

.. code-block:: none

    presto:tiny> EXPLAIN CREATE TABLE new_table (id BIGINT, name VARCHAR);
            Query Plan
    --------------------------
     CREATE TABLE new_table

Table creation with IF NOT EXISTS clause:

.. code-block:: none

    presto:tiny> EXPLAIN CREATE TABLE IF NOT EXISTS new_table (id BIGINT, name VARCHAR);
                  Query Plan
    --------------------------------------
     CREATE TABLE IF NOT EXISTS new_table

Table creation with column constraints and properties:

.. code-block:: none

    presto:tiny> EXPLAIN CREATE TABLE orders (
              ->   orderkey BIGINT NOT NULL,
              ->   orderstatus VARCHAR,
              ->   totalprice DOUBLE COMMENT 'Price in cents',
              ->   orderdate DATE
              -> )
              -> COMMENT 'Orders table'
              -> WITH (format = 'ORC');
                  Query Plan
    --------------------------------------
     CREATE TABLE orders

Table creation with LIKE clause:

.. code-block:: none

    presto:tiny> EXPLAIN CREATE TABLE new_orders (
              ->   LIKE orders INCLUDING PROPERTIES
              -> );
                  Query Plan
    --------------------------------------
     CREATE TABLE new_orders

.. note::

    ``EXPLAIN CREATE TABLE`` validates the syntax and checks if the table structure is valid,
    but it does not verify if the table already exists. The actual table creation will fail
    if the table exists (unless ``IF NOT EXISTS`` is specified).

DROP TABLE
""""""""""

The ``EXPLAIN DROP TABLE`` statement shows a summary of the table drop operation.

DROP TABLE Examples
'''''''''''''''''''

Basic table drop:

.. code-block:: none

    presto:tiny> EXPLAIN DROP TABLE test_table;
                            Query Plan
    --------------------------------------------------------------
     DROP TABLE test_table

Table drop with IF EXISTS clause:

.. code-block:: none

    presto:tiny> EXPLAIN DROP TABLE IF EXISTS test_table;
                            Query Plan
    --------------------------------------------------------------
     DROP TABLE IF EXISTS test_table


See Also
--------

:doc:`explain-analyze`
