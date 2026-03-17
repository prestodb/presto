===============
Table Functions
===============

Table functions return tables. They allow users to dynamically invoke custom logic 
from within the SQL query. They are invoked in the ``FROM`` clause of a query, and the 
calling convention is similar to a scalar function call. For description of table 
functions usage, see :doc:`/functions/table`.

Presto supports adding custom table functions. They are declared by connectors 
through implementing dedicated interfaces.

Table function declaration
==========================

To declare a table function, you must implement ``ConnectorTableFunction``. Subclassing 
``AbstractConnectorTableFunction`` is a convenient way to do it. The connector's 
``getTableFunctions()`` method must return a set of your implementations.

The constructor
---------------

::

    public class MyFunction
            extends AbstractConnectorTableFunction
    {
        public MyFunction()
        {
            super(
                    "system",
                    "my_function",
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("COLUMN_COUNT")
                                    .type(INTEGER)
                                    .defaultValue(2)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ROW_COUNT")
                                    .type(INTEGER)
                                    .build()),
                      GENERIC_TABLE);
        }
    }

The constructor takes the following arguments:

* **schema name**

  The schema name helps you organize functions, and it is used for function 
  resolution. When a table function is invoked, the right implementation is 
  identified by the catalog name, the schema name, and the function name.

  The function can use the schema name, for example to use data from the 
  indicated schema, or ignore it.

* **function name**

* **list of expected arguments**

  Three different types of arguments are supported: scalar arguments, 
  descriptor arguments, and table arguments. See :ref:`develop/table-functions:argument types` 
  for details. 
  
  You can specify default values for scalar and descriptor arguments. 
  The arguments with specified defaults can be skipped during table 
  function invocation.

* **returned row type**

  It describes the row type produced by the table function.

  If a table function takes table arguments, it can additionally pass the 
  columns of the input tables to output using the `pass-through mechanism`. 
  The returned row type is supposed to describe only the columns produced 
  by the function, as opposed to the pass-through columns.

  In the example, the returned row type is ``GENERIC_TABLE``, which means 
  that the row type is not known statically, and it is determined 
  dynamically based on the passed arguments.

  When the returned row type is known statically, you can declare it using:

  ::

    new DescribedTable(descriptor)

  If a table function does not produce any columns, and it only outputs the pass-through 
  columns, use ``ONLY_PASS_THROUGH`` as the returned row type.

.. note::

    A table function must return at least one column. It can either be a proper 
    column that is produced by the function, or a pass-through column.

Argument types
--------------

Table functions take three types of arguments: 

* :ref:`develop/table-functions:scalar arguments`
* :ref:`develop/table-functions:descriptor arguments`
* :ref:`develop/table-functions:table arguments`

Scalar arguments
^^^^^^^^^^^^^^^^

They can be of any supported data type. You can specify a default value.

::

    ScalarArgumentSpecification.builder()
            .name("COLUMN_COUNT")
            .type(INTEGER)
            .defaultValue(2)
            .build()

::

    ScalarArgumentSpecification.builder()
            .name("ROW_COUNT")
            .type(INTEGER)
            .build()

Descriptor arguments
^^^^^^^^^^^^^^^^^^^^

Descriptors consist of fields with names and optional data types. They are a 
convenient way to pass the required result row type to the function, or to 
inform the function which input columns it should use. You can specify default 
values for descriptor arguments. A descriptor argument can be ``null``.

:: 

    DescriptorArgumentSpecification.builder()
            .name("SCHEMA")
            .defaultValue(null)
            .build()

Table arguments
^^^^^^^^^^^^^^^

A table function can take any number of input relations. It allows you to 
process multiple data sources simultaneously.

When declaring a table argument, you must specify characteristics to determine 
how the input table is processed. You cannot specify a default value for a 
table argument.

::

    TableArgumentSpecification.builder()
            .name("INPUT")
            .rowSemantics()
            .pruneWhenEmpty()
            .passThroughColumns()
            .build()

**Set or row semantics**

*Set semantics* is the default for table arguments. A table argument with 
set semantics is processed on a partition-by-partition basis. During 
function invocation, the user can specify partitioning and ordering 
for the argument. If no partitioning is specified, the argument is 
processed as a single partition.

A table argument with *row semantics* is processed on a row-by-row 
basis. Partitioning or ordering is not applicable.

**Prune or keep when empty**


The *prune when empty* property indicates that if the given table argument is 
empty, the function returns an empty result. This property is used to optimize 
queries involving table functions. 

The *keep when empty* property indicates that the function should be executed 
even if the table argument is empty. The user can override this property when 
invoking the function. Using the *keep when empty* property can negatively affect 
performance when the table argument is not empty.

**Pass-through columns**


If a table argument has *pass-through columns*, all of its columns are passed on 
output. For a table argument without this property, only the partitioning columns 
are passed on output.

The analyze() method
--------------------

To provide all the necessary information to the Presto engine, the class must implement 
the ``analyze()`` method. This method is called by the engine during the analysis phase 
of query processing. The ``analyze()`` method is also the place to perform custom checks 
on the arguments:

::
  
  @Override
  public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
  {
      long columnCount = (long) ((ScalarArgument) arguments.get("COLUMN_COUNT")).getValue();
      long rowCount = (long) ((ScalarArgument) arguments.get("ROW_COUNT")).getValue();

      // custom validation of arguments
      if (columnCount < 1 || columnCount > 3) {
          throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "column_count must be in range [1, 3]");
      }

      if (rowCount < 1) {
          throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "row_count must be positive");
      }

      // determine the returned row type
      List<Descriptor.Field> fields = List.of("col_a", "col_b", "col_c").subList(0, (int) columnCount).stream()
              .map(name -> new Descriptor.Field(name, Optional.of(BIGINT)))
              .collect(toList());

      Descriptor returnedType = new Descriptor(fields);

      return TableFunctionAnalysis.builder()
              .returnedType(returnedType)
              .handle(new MyHandle(columnCount, rowCount))
              .build();
  }

The ``analyze()`` method returns a ``TableFunctionAnalysis`` object which comprises all the information required 
by the engine to analyze, plan, and execute the table function invocation:

* The returned row type, specified as an optional ``Descriptor``. It should be passed if and only if the table 
  function is declared with the ``GENERIC_TABLE`` returned type.

* Required columns from the table arguments, specified as a map of table argument names to lists of column indexes.

* Any information gathered during analysis that is useful during planning or execution, in the form of 
  a ``ConnectorTableFunctionHandle``. ``ConnectorTableFunctionHandle`` is a marker interface intended to carry 
  information throughout subsequent phases of query processing in a manner that is opaque to the engine.

Table function execution
========================

There are two paths of execution available for table functions:

* Pushdown to the connector

  The connector that provides the table function implements the ``applyTableFunction()`` 
  method. This method is called during the optimization phase of query processing. It returns 
  a ``ConnectorTableHandle`` and a list of ```ColumnHandle`` s representing the table function 
  result. The table function invocation is then replaced with a ``TableScanNode``.

  This execution path is convenient for table functions whose results are easy to represent 
  as a ``ConnectorTableHandle``, for example query pass-through. Pushdown to the connector 
  only supports scalar and descriptor arguments.

* Execution by operator

  Presto has a dedicated operator for table functions. It can handle table functions with any 
  number of table arguments as well as scalar and descriptor arguments. To use this execution 
  path, you provide an implementation of a processor.

  If your table function has one or more table arguments, you must implement ``TableFunctionDataProcessor``. 
  It processes pages of input data.

  If your table function is a source operator (it does not have table arguments), you 
  must implement ``TableFunctionSplitProcessor``. It processes splits. The connector 
  that provides the function must provide a ``ConnectorSplitSource`` for the function. 
  With splits, the task can be divided so that each split represents a subtask.

Access control
==============

The access control for table functions can be provided both on system and connector 
level. It is based on the fully qualified table function name, which consists of 
the catalog name, the schema name, and the function name, in the syntax of 
catalog.schema.function.
