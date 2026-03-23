===============
Table Functions
===============

A table function is a function returning a table. It can be invoked inside the FROM clause of a query: ::

    SELECT * FROM TABLE(my_function(1, 100))

The row type of the returned table can depend on the arguments passed when invoking 
the function. If either the input or output row types can be different, the function 
is a polymorphic table function.

Polymorphic table functions allow you to dynamically invoke custom logic from within 
the SQL query. They can be used for working with external systems as well as for 
enhancing Presto with capabilities going beyond the SQL standard.

Built-in table functions
========================

``exclude_columns`` table function
----------------------------------

Use the ``exclude_columns`` table function to return a new table based on an input table 
``table``, with the exclusion of all columns specified in ``descriptor``:
::
    exclude_columns(input => table, columns => descriptor) → table

The argument ``input`` is a table or a query. The argument ``columns`` is a descriptor 
without types.

Example query using the orders table from the TPC-H dataset, provided by the :doc:`/connector/tpch`:

::

    SELECT *
    FROM TABLE(exclude_columns(
                            input => TABLE(orders),
                            columns => DESCRIPTOR(clerk, comment)));

The table function is useful for queries where you want to return nearly all columns 
from tables with many columns. You can avoid enumerating all columns, and only need 
to specify the columns to exclude.

``sequence`` table function
---------------------------

Use the ``sequence`` table function to return a table with a single column ``sequential_number`` containing a sequence of bigint:

::

    sequence(start => bigint, stop => bigint, step => bigint) -> table(sequential_number bigint)

* ``start`` is the first element in the sequence. The default value is ``0``.

* ``stop`` is the end of the range, inclusive. The last element in the sequence is 
equal to ``stop``, or it is the last value within range, reachable by steps.

* ``step`` is the difference between subsequent values. The default value is ``1``.

Example query:

::

    SELECT *
    FROM TABLE(sequence(
                    start => 1000000,
                    stop => -2000000,
                    step => -3));

The result of the ``sequence`` table function might not be ordered. To enforce 
ordering in the enclosing query:

::

    SELECT *
    FROM TABLE(sequence(
                    start => 0,
                    stop => 100,
                    step => 5))
    ORDER BY sequential_number;

Table function invocation
=========================

Invoke a table function in the ``FROM`` clause of a query. Table function invocation 
syntax is similar to a scalar function call.

Function resolution
-------------------

Every table function is provided by a catalog, and it belongs to a schema in the 
catalog. You can qualify the function name with a schema name, or with 
catalog and schema names:

::

    SELECT * FROM TABLE(schema_name.my_function(1, 100))
    SELECT * FROM TABLE(catalog_name.schema_name.my_function(1, 100))

Otherwise, the standard Presto name resolution is applied. The connection between 
the function and the catalog must be identified, because the function is executed 
by the corresponding connector. If the function is not registered by the specified 
catalog, the query fails.

The table function name is resolved as case-insensitive, analogously to scalar 
function and table name resolution.

Arguments
---------

There are three types of arguments: *scalar*, *descriptor*, and *table*. 

Scalar arguments
^^^^^^^^^^^^^^^^

Scalars must be constant expressions. They can be of any SQL type that is compatible 
with the declared argument type:

::

    factor => 42

Descriptor arguments
^^^^^^^^^^^^^^^^^^^^

Descriptors consist of fields with names and optional data types:

::

    schema => DESCRIPTOR(id BIGINT, name VARCHAR)
    columns => DESCRIPTOR(date, status, comment)

To pass ``null`` for a descriptor, use

::

    schema => CAST(null AS DESCRIPTOR)

Table arguments
^^^^^^^^^^^^^^^

To pass a table name or a query, use the keyword ``TABLE``:

::

    input => TABLE(orders)
    data => TABLE(SELECT * FROM region, nation WHERE region.regionkey = nation.regionkey)

If the table argument is declared as set semantics, you can specify partitioning and 
ordering. Each partition is processed independently by the table function. If you do 
not specify partitioning, the argument is processed as a single partition. You can 
also specify ``PRUNE WHEN EMPTY`` or ``KEEP WHEN EMPTY``. 

* With ``PRUNE WHEN EMPTY`` you declare that you are not interested in the function 
  result if the argument is empty. This information is used by Presto to optimize 
  the query. 
* The ``KEEP WHEN EMPTY`` option indicates that the function should be executed even 
  if the table argument is empty. 
  
By specifying ``KEEP WHEN EMPTY`` or ``PRUNE WHEN EMPTY``, you override the property 
set for the argument by the function author.

The following example shows how the table argument properties should be ordered:

::

    input => TABLE(orders)
                        PARTITION BY orderstatus
                        KEEP WHEN EMPTY
                        ORDER BY orderdate

Argument passing conventions
----------------------------

There are two conventions for passing arguments to a table function:

Arguments passed by name
^^^^^^^^^^^^^^^^^^^^^^^^

::

    SELECT * FROM TABLE(my_function(row_count => 100, column_count => 1))

In this convention, you can pass the arguments in arbitrary order. Arguments declared 
with default values can be skipped. Argument names are resolved case-sensitive, and 
with automatic uppercasing of unquoted names.

Arguments passed positionally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
::

    SELECT * FROM TABLE(my_function(1, 100))

In this convention, you must follow the order in which the arguments are declared. 
You can skip a suffix of the argument list, provided that all the skipped arguments 
are declared with default values.

You cannot mix the argument conventions in one invocation.

To use parameters in arguments:

::

    PREPARE stmt FROM
    SELECT * FROM TABLE(my_function(row_count => ? + 1, column_count => ?));

    EXECUTE stmt USING 100, 1;
