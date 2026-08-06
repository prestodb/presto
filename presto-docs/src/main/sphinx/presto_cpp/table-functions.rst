======================================================
Table-Valued Functions (TVF) — C++ API Design
======================================================

Overview
========

Table-Valued Functions (TVFs) are SQL functions that return tables rather than scalar values.
They appear in the ``FROM`` clause and can accept other tables, descriptors, and scalar constants as arguments.
This document covers the **C++ native worker Table Function SPI** and provides worked examples for it.

Reference: `RFC-0020-tvf <https://github.com/prestodb/rfcs/blob/main/RFC-0020-tvf.md>`_

--------

SQL Syntax
==========

.. code-block:: sql

   SELECT * FROM TABLE(
       catalog.schema.my_function(
           input  => TABLE(SELECT * FROM orders) t PARTITION BY custkey ORDER BY orderdate,
           layout => DESCRIPTOR(order_id bigint, status varchar),
           limit  => BIGINT '100'
       )
   ) result

Argument Types
--------------

+------------+--------------------------------------------------+------------------------------------------------------+
| Kind       | SQL Form                                         | Description                                          |
+============+==================================================+======================================================+
| Table      | ``TABLE(query) alias PARTITION BY … ORDER BY …``| A full relation; rows arrive partitioned and ordered|
+------------+--------------------------------------------------+------------------------------------------------------+
| Descriptor | ``DESCRIPTOR(col1 type1, col2)``                 | A list of column names with optional types          |
+------------+--------------------------------------------------+------------------------------------------------------+
| Scalar     | A typed constant, e.g., ``BIGINT '42'``          | A single constant value                              |
+------------+--------------------------------------------------+------------------------------------------------------+

Table Argument Modifiers
-------------------------

+-------------------------------+---------------------------------------------------------------+
| Modifier                      | Meaning                                                       |
+===============================+===============================================================+
| ``PARTITION BY cols``         | Distribute rows across parallel worker instances              |
+-------------------------------+---------------------------------------------------------------+
| ``ORDER BY cols``             | Sort rows within each partition                               |
+-------------------------------+---------------------------------------------------------------+
| ``COPARTITION (t1, t2)``      | Co-locate two table arguments on matching partition columns  |
+-------------------------------+---------------------------------------------------------------+
| ``PRUNE WHEN EMPTY`` (default)| Short-circuit to empty output when input is empty             |
+-------------------------------+---------------------------------------------------------------+
| ``KEEP WHEN EMPTY``           | Invoke the processor even when the input is empty            |
+-------------------------------+---------------------------------------------------------------+
| ``PASSTHROUGH COLUMNS``       | Input columns forwarded unchanged to the output              |
+-------------------------------+---------------------------------------------------------------+

--------

Polymorphism for Table Functions
=================================

Table-valued functions are inherently polymorphic because their input and output schemas are not fixed at declaration
time. They vary depending on which arguments the user supplies in the SQL.

Unlike scalar and aggregate functions whose types can be resolved at registration time,
TVFs require a dynamic analysis phase during query planning to determine the output schema and runtime context.
This design enables maximum flexibility while keeping the C++ SPI clean and intuitive.

Query Processing Flow for a SQL with a Table Function
======================================================

Parsing
-------

The SQL parser recognises the TABLE(…) wrapper in the FROM clause and builds an
AstNode for the table function invocation. Each argument is categorised by its
syntactic form — TABLE(subquery) becomes a table argument, DESCRIPTOR(…) becomes a
descriptor argument, and typed constants become scalar arguments. PARTITION BY,
ORDER BY, and COPARTITION clauses attached to table arguments are also captured at
this stage.

Statement Analysis
------------------

During statement analysis, the coordinator looks up the table function in the AstNode
with its (catalog.schema.function_name) in the TableFunctionRegistry. It then calls the
function's registered analyze() method passing the resolved argument values:

* **ScalarArgument** — Comprises the constant value and its Velox type.
* **TableArgument** — Consists of the row type of the input relation, plus any partition/sort keys the caller specified.
* **DescriptorArgument** — has the field names (and optional types) from the DESCRIPTOR(…) in the table function invocation.

The TableFunction::analyze() method is responsible for:

* Validating the argument values (types, constraints, cardinality).
* Computing the dynamic output schema if required. The returned schema can be a Descriptor or type.
* Declaring requiredColumns. requiredColumns are the minimal set of input column indexes the function actually needs from its input table. This helps the optimizer to prune all other columns.
* Producing a TableFunctionHandle (serializable) that carries all runtime context gathered during analysis to the workers.

The result is a TableFunctionAnalysis struct containing these three pieces.

Logical Planning
----------------

The planner materialises a TableFunctionNode in the logical plan tree. This node
records the function name, the resolved arguments, the output variables, the list of
source PlanNodes (one per table argument), per-argument properties (passThroughColumns,
pruneWhenEmpty, rowSemantics, partition/sort keys), any COPARTITION lists, and the
TableFunctionHandle from analysis.

The planner then rewrites TableFunctionNode into a PlanNode subtree based on the input shape:

+------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| Input shape                              | Physical plan                                                                                                                     |
+==========================================+===================================================================================================================================+
| No table inputs (leaf)                   | Planned like a TableScan driven by splits produced by the function getSplits() method                                            |
+------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| Single table, row semantics              | Inlined as a Project/Filter node                                                                                                  |
+------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| Single table, set semantics + PARTITION BY| Planned like a window function — data is repartitioned and sorted by the partition/order keys before reaching the operator       |
+------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| Multiple table inputs                    | A full-outer-join tree with row-number window functions per input; join conditions align partition boundaries and handle         |
|                                          | PRUNE/KEEP WHEN EMPTY semantics. This is the input to the TableFunction node                                                     |
+------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------+

At this point the TableFunctionNode is materialized as a TableFunctionProcessorNode. This node is the Velox PlanNode
responsible for Table function execution. More in the design section.

Split Generation (leaf functions only)
---------------------------------------

Like TableScans, Leaf TableFunctions require splits to process. This is done using the TableFunctionSplitGenerator.
This produces a list of TableSplitHandle objects — each representing an independent unit of
work (e.g., a sub-range for sequence). These splits are scheduled across workers
exactly like ordinary connector splits.

Serialization and dispatch
---------------------------

The TableFunctionHandle and each TableSplitHandle are serialized (via
velox::ISerializable / Jackson on the Java side) and included in the task descriptors
sent to worker nodes. The workers deserialize them using a name-to-factory
mapping registered by the function.

Execution on Workers
--------------------

TableFunctionProcessorNode could execute as:

* **LeafTableFunctionOperator**: if the function has no input tables
* **TableFunctionOperator**: if the function has input tables

LeafTableFunctionOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^

This operator is used when the function has no input tables, so it operates as a
TableScan by getting splits from the TableFunctionSplitGenerator.

For each assigned split, the worker:

* Deserializes the TableSplitHandle.
* Instantiates a fresh TableFunctionSplitProcessor via the registered factory.
* Calls processor.apply(splitHandle) in a loop until kFinished is returned,
* Returns the output RowVectors from apply output.

TableFunctionOperator
^^^^^^^^^^^^^^^^^^^^^

For functions with table inputs, the operator:

* Receives input RowVectors from upstream operators.
* The TableFunctionOperator partitions and sorts the input RowVectors to identify the table function partitions.
* For each table function partition, the operator creates a TableFunctionDataProcessor from the factory and calls its TableFunctionDataProcessor::apply(input) in a loop for all the input RowVectors.
* The TableFunctionDataProcessor returns kProcessed with output vectors, kBlocked to pause (async dependency), or kFinished when the partition is complete.
* Result Assembly: The output vectors from TableFunctionDataProcessor::apply are returned to the downstream operators.

C++ SPI
=======

Defining and registering a Table Function
------------------------------------------

To define and register a Table Function, the user can perform the following steps:

* Identify the Table function arguments. These are represented with ArgumentSpecification classes when registering the function, but are resolved to Argument classes when instantiated in a SQL query.
* Identify the Table function return type. The Table function returns a table though its columns could be generic (decided at analysis time), described (known at specification time) or PassThrough (no new columns, the input table is passthrough)
* Decide the TableFunctionAnalyzer logic. The TableFunctionAnalyzer is invoked by the Presto planner during StatementAnalysis of the received query. The analysis validates input arguments to their specs, determines the required input columns, determines the ReturnType and also builds a TableFunctionHandle which could contain any metadata that the function would want to pass to the TableFunctionOperator instances during execution.
* Decide if the processing will be done by a TableFunctionDataProcessor or a TableFunctionSplitProcessor (if there are no input table arguments).
* If using a TableFunctionSplitProcessor, then also write a TableFunctionSplitGenerator function which will be invoked by the coordinator for scheduling splits.

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/TableFunction.h

   struct TableFunctionEntry {
     TableArgumentSpecList                argumentSpecs;
     ReturnSpecPtr                        returnTypeSpec;
     TableFunctionAnalyzer                analyzer;
     TableFunctionSplitGenerator          splitGenerator;      // null for data processors
     TableFunctionDataProcessorFactory    dataProcessorFactory;  // null for split processors
     TableFunctionSplitProcessorFactory   splitProcessorFactory; // null for data processors
   };

Each section below goes over the SPI for each of these parts.

Argument and ArgumentSpecification Classes
-------------------------------------------

1a. ArgumentSpecification
^^^^^^^^^^^^^^^^^^^^^^^^^

Base class for each ArgumentSpecification. It specifies the name, if the argument is required or optional and its default value.
Arguments can be Scalar, Table or Descriptor arguments each of which is described subsequently.

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/Argument.h
   class ArgumentSpecification {
    public:
     ArgumentSpecification(
         std::string name,
         bool required,
         std::optional<velox::variant> defaultValue = std::nullopt);

     const std::string& name() const;
     bool required() const;
     const std::optional<velox::variant>& defaultValue() const;
   };

1b. ScalarArgumentSpecification / ScalarArgument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/ScalarArgument.h

   // Declared at registration time — describes a scalar parameter.
   class ScalarArgumentSpecification : public ArgumentSpecification {
    public:
     velox::TypePtr rowType() const;   // the argument's Velox type
    private:
     velox::TypePtr type_;
   };

   // Received at analysis time — carries the caller-supplied constant.
   class ScalarArgument : public Argument {
    public:
     velox::TypePtr rowType() const;
     velox::VectorPtr value() const;   // constant value as a single-row vector
    private:
     velox::TypePtr type_;
     velox::VectorPtr constantValue_;
   };

1c. TableArgumentSpecification / TableArgument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/TableArgument.h

   // Declared at registration time.
   class TableArgumentSpecification : public ArgumentSpecification {
    public:
     bool rowSemantics;       // row-by-row processing; disables PARTITION BY
     bool pruneWhenEmpty;     // (default) return empty output when input is empty
     bool passThroughColumns; // forward all input columns to the output
   };

   // Received at analysis time — carries the input relation's schema.
   class TableArgument : public Argument {
    public:
     // Row type of the input table.
     velox::RowTypePtr rowType() const;
     // Partition and sort keys specified by the caller in SQL.
     const std::vector<velox::core::FieldAccessTypedExprPtr>& partitionKeys() const;
     const std::vector<velox::core::FieldAccessTypedExprPtr>& sortingKeys() const;
     const std::vector<velox::core::SortOrder>& sortingOrders() const;
   };

1d. DescriptorArgumentSpecification / DescriptorArgument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/DescriptorArgument.h

   class DescriptorArgumentSpecification : public ArgumentSpecification {};

   class DescriptorArgument : public Argument {
    public:
     const Descriptor& descriptor() const;
   };

1e. Descriptor
^^^^^^^^^^^^^^

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/Descriptor.h

   class Descriptor {
    public:
     // Names only (no types).
     explicit Descriptor(std::vector<std::string> names);
     // Names with corresponding Velox types.
     Descriptor(std::vector<std::string> names, std::vector<velox::TypePtr> types);

     const std::vector<std::string>& names() const;
     const std::vector<velox::TypePtr>& types() const;
     // Field access expressions for use in plan nodes.
     std::vector<velox::core::FieldAccessTypedExprPtr> typedExprs() const;
   };

   using DescriptorPtr = std::shared_ptr<const Descriptor>;

--------

Return Type Specification
--------------------------

Specifies the Table Function Return type. This could be GENERIC (so determined at Analysis time), DESCRIBED (so fixed
and known at Table function specification time) and PASSTHROUGH (no new columns added, and outputs are passthrough
from input tables)

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/ReturnTypeSpecification.h

   class ReturnTypeSpecification {
    public:
     enum class ReturnType { kGenericTable, kDescribedTable, kOnlyPassThrough };
     virtual ReturnType returnType() const = 0;
   };

   using ReturnSpecPtr = std::shared_ptr<ReturnTypeSpecification>;

   // Output schema determined dynamically in analyze().
   class GenericTableReturnTypeSpecification : public ReturnTypeSpecification {};

   // Output schema fixed and fully known at registration time.
   class DescribedTableReturnTypeSpecification : public ReturnTypeSpecification {
    public:
     explicit DescribedTableReturnTypeSpecification(DescriptorPtr descriptor);
     DescriptorPtr descriptor() const;
   };

   // No new columns added; output is purely pass-through from input tables.
   class OnlyPassThroughReturnTypeSpecification : public ReturnTypeSpecification {};

--------

Table Function Analysis
-----------------------

``analyze()`` runs on the **coordinator** at planning time. It validates arguments,
determines the output schema, the required input columns, and packages runtime context into a
``TableFunctionHandle`` that is serialized and shipped to workers.

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/TableFunctionAnalysis.h

   // Opaque coordinator-to-worker context. Subclass and serialize via
   // velox::ISerializable (implement serialize() and name()).
   class TableFunctionHandle : public velox::ISerializable {
    public:
     virtual folly::dynamic serialize() const = 0;
     virtual std::string name() const = 0;
   };

   using TableFunctionHandlePtr = std::shared_ptr<TableFunctionHandle>;

   // Result returned by analyze().
   struct TableFunctionAnalysis {
     // Dynamic output schema. Required when GenericTableReturnTypeSpecification
     // is used; leave null otherwise.
     DescriptorPtr returnType_;

     // Opaque context passed to workers and to getSplits().
     TableFunctionHandlePtr tableFunctionHandle_;

     // For each table argument name, the list of column indexes actually read.
     // The optimizer prunes all other columns before they reach the processor.
     std::unordered_map<std::string, std::vector<int>> requiredColumns_;
   };

--------

Execution Processors
--------------------

Table Functions are processed as TableFunctionDataProcessor(s) or TableFunctionSplitProcessor(s)

TableFunctionDataProcessor
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Used when the function consumes one or more table arguments (set semantics).
One processor instance is created per partition.

.. code-block:: cpp

   class TableFunctionDataProcessor {
    public:
     virtual ~TableFunctionDataProcessor() = default;

     // Called repeatedly until kFinished is returned.
     //
     // input  — one RowVectorPtr per table argument, in declaration order.
     //          Each vector contains only the columns listed in requiredColumns_.
     //          An empty vector (zero rows) means the source is not yet ready.
     //          A null pointer means all sources are exhausted.
     virtual TableFunctionResult apply(
         const std::vector<velox::RowVectorPtr>& input) = 0;

    protected:
     velox::memory::MemoryPool* pool_;
     velox::HashStringAllocator* allocator_;
   };

TableFunctionSplitProcessor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Used for leaf functions that generate data from scratch (no table inputs).
One processor instance is created per split.

.. code-block:: cpp

   class TableFunctionSplitProcessor {
    public:
     virtual ~TableFunctionSplitProcessor() = default;

     // Called repeatedly until kFinished is returned.
     //
     // split  — the split to process.
     //          null when KEEP WHEN EMPTY is set and the source was empty.
     virtual TableFunctionResult apply(const TableSplitHandlePtr& split) = 0;

    protected:
     velox::memory::MemoryPool* pool_;
     velox::HashStringAllocator* allocator_;
   };

TableFunctionResult
^^^^^^^^^^^^^^^^^^^

Both processor types return ``TableFunctionResult`` from every ``apply()`` call.

.. code-block:: cpp

   // presto_cpp/main/tvf/spi/TableFunctionResult.h

   /// This class represents the result of processing input by
   /// {@link TableFunctionDataProcessor} or {@link TableFunctionSplitProcessor}.
   /// It can optionally include a portion of output data in the form of a
   /// RowVectorPtr.
   /// The returned RowVectorPtr should consist of:
   /// -- proper columns produced by the table function
   /// -- one column of type {@code BIGINT} for each table function's input table
   /// having the pass-through property (see {@link
   /// TableArgumentSpecification#isPassThroughColumns}), in order of the
   /// corresponding argument specifications. Entries in these columns are the
   /// indexes of input rows (from partition start) to be attached to output, or
   /// null to indicate that a row of nulls should be attached instead of an input
   /// row. The indexes are validated to be within the portion of the partition
   /// provided to the function so far. Note: when the input is empty, the only
   /// valid index value is null, because there are no input rows that could be
   /// attached to output. In such case, for performance reasons, the validation of
   /// indexes is skipped, and all pass-through columns are filled with nulls.
   class TableFunctionResult {
    public:
     enum class TableFunctionState {
       kBlocked,
       kFinished,
       kProcessed,
     };

     // For finished state
     TableFunctionResult(TableFunctionState state);

     // For processed state
     TableFunctionResult(bool usedInput, velox::RowVectorPtr result);

     // For blocked state
     TableFunctionResult(velox::ContinueFuture* future);

     TableFunctionResult::TableFunctionState state() const;

     bool usedInput();

     [[nodiscard]] velox::RowVectorPtr result() const;

     [[nodiscard]] velox::ContinueFuture* future() const;
   };

--------

Summary: Choosing the Right Processor
======================================

+-------------------+-----------------------------------+-------------------------------------+
| Criterion         | ``TableFunctionSplitProcessor``   | ``TableFunctionDataProcessor``      |
+===================+===================================+=====================================+
| Input tables      | None — generates data itself      | One or more table arguments         |
+-------------------+-----------------------------------+-------------------------------------+
| Parallelism unit  | One processor per split           | One processor per partition         |
+-------------------+-----------------------------------+-------------------------------------+
| Split source      | ``TableFunctionSplitGenerator``   | Engine-managed from input operators |
|                   | callback                          |                                     |
+-------------------+-----------------------------------+-------------------------------------+
| Typical use case  | Sequence generators, external     | Column transforms, analytics,       |
|                   | data pulls                        | ML inference                        |
+-------------------+-----------------------------------+-------------------------------------+
| Null/empty input  | ``KEEP WHEN EMPTY`` — input was   | All sources exhausted               |
|                   | empty                             |                                     |
+-------------------+-----------------------------------+-------------------------------------+

--------

Key Design Constraints
======================

* ``analyze()`` runs on the **coordinator** at planning time — keep it fast and side-effect free.
* ``requiredColumns_`` drives optimizer column pruning — always declare the minimal set of columns actually read.
* ``TableFunctionHandle`` and ``TableSplitHandle`` must implement ``velox::ISerializable`` (``serialize()`` + ``name()``) as they are shipped over the wire.
* ``TableFunctionSplitProcessor`` instances are **not** reused across splits.
* ``TableFunctionDataProcessor`` receives vectors in the order table arguments were declared in the registration call.
* Pass-through columns are represented as trailing BIGINT columns in the output vectors; each value is a 0-based row index into the current partition, or null to substitute a row of nulls.
