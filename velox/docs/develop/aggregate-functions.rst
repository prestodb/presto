=================================
How to add an aggregate function?
=================================

Aggregate functions are calculated by the HashAggregation operator.
There can be one or more aggregate functions in a single operator.
Here are some examples.

Global aggregation (no grouping keys), single aggregate “count”.

.. code-block:: sql

    	SELECT count(*) FROM t

Global aggregation, two aggregates: “count” and “sum”:

.. code-block:: sql

        SELECT count(*), sum(b) FROM t

Aggregation with three aggregates: “count” and two “sum”s.

.. code-block:: sql

        SELECT a, count(*), sum(b), sum(c) FROM t GROUP BY 1

Aggregation with just one aggregate - “min” - and two grouping keys.

.. code-block:: sql

    	SELECT a, b, min(c) FROM t GROUP BY 1, 2

Typically, aggregations are calculated in two steps: partial aggregation
and final aggregation.

Partial aggregation takes raw data and produces intermediate results. Final
aggregation takes intermediate results and produces the final result. There
are also single and intermediate aggregations that are used in some cases.
Single aggregation is used when data is already partitioned on the grouping
keys and therefore no shuffle is necessary. Intermediate aggregations are used
to combine the results of partial aggregations computed in multiple threads in
parallel to reduce the amount of data sent to the final aggregation stage.

The four types, steps, of aggregation are distinguished solely by the types of
input and output.

========================  ====================    ====================
Step                      Input                   Output
========================  ====================    ====================
**Partial**               Raw Data                Intermediate Results
**Final**                 Intermediate Results    Final Results
**Single**                Raw Data                Final Results
**Intermediate**          Intermediate Results    Intermediate Results
========================  ====================    ====================

In some cases, the calculations performed by partial and final aggregations are
the same. This is the case for the :func:`sum`, :func:`min` and :func:`max`
aggregates. In most cases they are different. For example, partial :func:`count`
aggregate counts incoming values and final :func:`count` aggregate sums up partial
counts to produce a total.

The signature of an aggregate function consists of the type of the raw input data,
the type of the intermediate result and the type of the final result.

Aggregate functions can also be used in a window operator. Here is an example of
computing running total that uses :func:`sum` aggregate function in a window operator.

.. code-block:: sql

        SELECT a, sum(b) OVER (PARTITION by c ORDER BY d DESC) FROM t

Memory Layout
-------------

HashAggregation operator stores data in rows. Each row corresponds to a unique combination of grouping key values. Global aggregations store data in a single row.

Aggregate functions can be classified by the type of their accumulators into three groups:

* Fixed width accumulators:
    * :func:`count`, :func:`sum`, :func:`avg`
    * :func:`min`, :func:`max`, :func:`arbitrary` (for fixed-width types)
* Variable width accumulators with append-only semantics:
    * :func:`array_agg`
    * :func:`map_agg`
* Variable width accumulators which can be modified in any way, not just appended to.
    * :func:`min`, :func:`max` (for strings)
    * :func:`arbitrary` (for variable-width types)
    * :func:`approx_percentile`
    * :func:`approx_distinct`

Fixed-width part of the accumulator is stored in the row. Variable-width
part (if exists) is allocated using :doc:`HashStringAllocator <arena>` and a pointer is
stored in the fixed-width part.

A row is a contiguous byte buffer.  If any of the accumulators has alignment
requirement, the row beginning and accumulator address will be aligned
accordingly.  Data is stored in the following order:

1. Null flags (1 bit per item) for
    1. Keys (only if nullable)
    2. Accumulators
2. Free flag (1 bit)
3. Keys
4. Padding for alignment
5. Accumulators, fixed-width part only
6. Variable size (32 bit)
7. Padding for alignment

.. image:: images/aggregation-layout.png
  :width: 600

Aggregate class
---------------

To add an aggregate function,

* Prepare:
    * Figure out what are the input, intermediate and final types.
    * Figure out what are partial and final calculations.
    * Design the accumulator. Make sure the same accumulator can accept both raw
      inputs and intermediate results.
    * Create a new class that extends velox::exec::Aggregate base class
      (see velox/exec/Aggregate.h) and implement virtual methods.
* Register the new function using exec::registerAggregateFunction(...).
* Add tests.
* Write documentation.

Accumulator size
----------------

The implementation of the velox::exec::Aggregate interface can start with *accumulatorFixedWidthSize()* method.

.. code-block:: c++

      // Returns the fixed number of bytes the accumulator takes on a group
      // row. Variable width accumulators will reference the variable
      // width part of the state from the fixed part.
      virtual int32_t accumulatorFixedWidthSize() const = 0;

The HashAggregation operator uses this method during initialization to calculate the total size of the row and figure out offsets at which different aggregates will be storing their data. The operator then calls velox::exec::Aggregate::setOffsets method for each aggregate to specify the location of the accumulator.

.. code-block:: c++

      // Sets the offset and null indicator position of 'this'.
      // @param offset Offset in bytes from the start of the row of the accumulator
      // @param nullByte Offset in bytes from the start of the row of the null flag
      // @param nullMask The specific bit in the nullByte that stores the null flag
      void setOffsets(int32_t offset, int32_t nullByte, uint8_t nullMask)

The base class implements the setOffsets method by storing the offsets in member variables.

.. code-block:: c++

      // Byte position of null flag in group row.
      int32_t nullByte_;
      uint8_t nullMask_;
      // Offset of fixed length accumulator state in group row.
      int32_t offset_;

Typically, an aggregate function doesn’t use the offsets directly. Instead, it uses helper methods from the base class.

To access the accumulator:

.. code-block:: c++

      template <typename T>
      T* value(char* group) const {
        return reinterpret_cast<T*>(group + offset_);
      }

To manipulate the null flags:

.. code-block:: c++

      bool isNull(char* group) const;

      // Sets null flag for all specified groups to true.
      // For any given group, this method can be called at most once.
      void setAllNulls(char** groups, folly::Range<const vector_size_t*> indices);

      inline bool clearNull(char* group);

Initialization
--------------

Once you have accumulatorFixedWidthSize(), the next method to implement is initializeNewGroups().

.. code-block:: c++

      // Initializes null flags and accumulators for newly encountered groups.
      // @param groups Pointers to the start of the new group rows.
      // @param indices Indices into 'groups' of the new entries.
      virtual void initializeNewGroups(
          char** groups,
          folly::Range<const vector_size_t*> indices) = 0;

This method is called by the HashAggregation operator every time it encounters new combinations of the grouping keys. This method should initialize the accumulators for the new groups. For example, partial “count” and “sum” aggregates would set the accumulators to zero. Many aggregate functions would set null flags to true by calling the exec::Aggregate::setAllNulls(groups, indices) helper method.

GroupBy aggregation
-------------------

At this point you have accumulatorFixedWidthSize() and initializeNewGroups() methods implemented. Now, we can proceed to implementing the end-to-end group-by aggregation. We need the following pieces:

* Logic for adding raw input to the accumulator:
    * addRawInput() method.
* Logic for producing intermediate results from the accumulator:
    * extractAccumulators() method.
* Logic for adding intermediate results to the accumulator:
    * addIntermediateResults() method.
* Logic for producing final results from the accumulator:
    * extractValues() method.
* Logic for adding previously spilled data back to the accumulator:
    * addSingleGroupIntermediateResults() method.

Some methods are only used in a subset of aggregation workflows. The following
tables shows which methods are used in which workflows.

.. list-table::
   :widths: 50 25 25 25 25 25
   :header-rows: 1

   * - Method
     - Partial
     - Final
     - Single
     - Intermediate
     - Streaming
   * - addRawInput
     - Y
     - N
     - Y
     - N
     - Y
   * - extractAccumulators
     - Y
     - Y (used for spilling)
     - Y (used for spilling)
     - Y
     - Y
   * - addIntermediateResults
     - N
     - Y
     - N
     - Y
     - Y
   * - extractValues
     - N
     - Y
     - Y
     - N
     - Y
   * - addSingleGroupIntermediateResults
     - N
     - Y
     - Y
     - N
     - N

We start with the addRawInput() method which receives raw input vectors and adds the data to accumulators.

.. code-block:: c++

      // Updates the accumulator in 'groups' with the values in 'args'.
      // @param groups Pointers to the start of the group rows. These are aligned
      // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th group.
      // The groups may repeat if different rows go into the same group.
      // @param rows Rows of the 'args' to add to the accumulators. These may not be
      // contiguous if the aggregation is configured to drop null grouping keys.
      // This would be the case when aggregation is followed by the join on the
      // grouping keys.
      // @param args Data to add to the accumulators.
      // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
      // The pushdown can happen only if this flag is true and 'args' is a single
      // LazyVector.
      virtual void addRawInput(
          char** groups,
          const SelectivityVector& rows,
          const std::vector<VectorPtr>& args,
          bool mayPushdown = false) = 0;

addRawInput() method would use DecodedVector’s to decode the input data. Then, loop over rows to update the accumulators. It is a good practice to define a member variable of type DecodedVector for each input vector. This allows for reusing the memory needed to decode the inputs between batches of input.

After implementing the addRawInput() method, we proceed to adding logic for extracting intermediate results.

.. code-block:: c++

      // Extracts partial results (used for partial and intermediate aggregations).
      // @param groups Pointers to the start of the group rows.
      // @param numGroups Number of groups to extract results from.
      // @param result The result vector to store the results in.
      virtual void
      extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result) = 0;

Next, we implement the addIntermediateResults() method that receives intermediate results and updates accumulators.

.. code-block:: c++

      virtual void addIntermediateResults(
          char** groups,
          const SelectivityVector& rows,
          const std::vector<VectorPtr>& args,
          bool mayPushdown = false) = 0;

Next, we implement the extractValues() method that extracts final results from the accumulators.

.. code-block:: c++

      // Extracts final results (used for final and single aggregations).
      // @param groups Pointers to the start of the group rows.
      // @param numGroups Number of groups to extract results from.
      // @param result The result vector to store the results in.
      virtual void
      extractValues(char** groups, int32_t numGroups, VectorPtr* result) = 0;

Finally, we implement the addSingleGroupIntermediateResults() method that is used to add previously spilled data back to the accumulator.

.. code-block:: c++

      // Updates the single final accumulator from intermediate results for global
      // aggregation.
      // @param group Pointer to the start of the group row.
      // @param rows Rows of the 'args' to add to the accumulators. These may not
      // be contiguous if the aggregation has mask. 'rows' is guaranteed to have at
      // least one active row.
      // @param args Intermediate results produced by extractAccumulators().
      // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
      // The pushdown can happen only if this flag is true and 'args' is a single
      // LazyVector.
      virtual void addSingleGroupIntermediateResults(
          char* group,
          const SelectivityVector& rows,
          const std::vector<VectorPtr>& args,
          bool mayPushdown) = 0;

GroupBy aggregation code path is done. We proceed to global aggregation.

Global aggregation
------------------

Global aggregation is similar to group-by aggregation, but there is only one
group and one accumulator. After implementing group-by aggregation, the only
thing needed to enable global aggregation is to implement
addSingleGroupRawInput() method (addSingleGroupIntermediateResults() method is
already implemented as it is used for spilling group by).

.. code-block:: c++

      // Updates the single accumulator used for global aggregation.
      // @param group Pointer to the start of the group row.
      // @param allRows A contiguous range of row numbers starting from 0.
      // @param args Data to add to the accumulators.
      // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
      // The pushdown can happen only if this flag is true and 'args' is a single
      // LazyVector.
      virtual void addSingleGroupRawInput(
          char* group,
          const SelectivityVector& allRows,
          const std::vector<VectorPtr>& args,
          bool mayPushdown) = 0;

Spilling is not helpful for global aggregations, hence, it is not supported. The
following table shows which methods are used in different global aggregation
workflows.

.. list-table::
   :widths: 50 25 25 25 25 25
   :header-rows: 1

   * - Method
     - Partial
     - Final
     - Single
     - Intermediate
     - Streaming
   * - addSingleGroupRawInput
     - Y
     - N
     - Y
     - N
     - Y
   * - extractAccumulators
     - Y
     - N
     - N
     - Y
     - Y
   * - addSingleGroupIntermediateResults
     - N
     - Y
     - N
     - Y
     - Y
   * - extractValues
     - N
     - Y
     - Y
     - N
     - Y

Global aggregation is also used by the window operator. For each row, a global
accumulator is cleared (clear + initializeNewGroups), then all rows in a window
frame are added (addSingleGroupRawInput), then result is extracted
(extractValues).

When computing running totals, i.e. when window frame is BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW, window operator re-uses the accumulator for multiple
rows without resetting. For each row, window operator adds that row to the
accumulator, then extracts results. The aggregate function sees a repeated
sequence of addSingleGroupRawInput + extractValues calls and needs to handle
these correctly.

Factory function
----------------

We can now write a factory function that creates an instance of the new
aggregation function and register it by calling exec::registerAggregateFunction
(...) and specifying function name and signatures.

HashAggregation operator uses this function to create an instance of the
aggregate function. A new instance is created for each thread of execution. When
partial aggregation runs on 5 threads, it uses 5 instances of each aggregate
function.

Factory function takes core::AggregationNode::Step
(partial/final/intermediate/single) which tells what type of input to expect,
input type and result type.

.. code-block:: c++

        bool registerApproxPercentile(const std::string& name) {
          std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
          ...

          exec::registerAggregateFunction(
              name,
              std::move(signatures),
              [name](
                  core::AggregationNode::Step step,
                  const std::vector<TypePtr>& argTypes,
                  const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
                if (step == core::AggregationNode::Step::kIntermediate) {
                  return std::make_unique<ApproxPercentileAggregate<double>>(
                      false, false, VARBINARY());
                }

                auto hasWeight = argTypes.size() == 3;
                TypePtr type = exec::isRawInput(step) ? argTypes[0] : resultType;

                switch (type->kind()) {
                  case TypeKind::BIGINT:
                    return std::make_unique<ApproxPercentileAggregate<int64_t>>(
                        hasWeight, resultType);
                  case TypeKind::DOUBLE:
                    return std::make_unique<ApproxPercentileAggregate<double>>(
                        hasWeight, resultType);
                  ...
                }
              });
          return true;
        }

        static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
            registerApproxPercentile(kApproxPercentile);

Use FunctionSignatureBuilder to create FunctionSignature instances which
describe supported signatures. Each signature includes zero or more input
types, an intermediate result type and final result type.

FunctionSignatureBuilder and FunctionSignature support Java-like
generics, variable number of arguments and lambdas. See more in
:ref:`function-signature` section of the :doc:`scalar-functions` guide.

Here is an example of signatures for the :func:`approx_percentile` function. This
functions takes value argument of a numeric type, an optional weight argument
of type INTEGER, and a percentage argument of type DOUBLE. The intermediate
type does not depend on the input types and is always VARBINARY. The final
result type is the same as input value type.

.. code-block:: c++

        for (const auto& inputType :
               {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
            // (x, double percentage) -> varbinary -> x
            signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                                     .returnType(inputType)
                                     .intermediateType("varbinary")
                                     .argumentType(inputType)
                                     .argumentType("double")
                                     .build());

            // (x, integer weight, double percentage) -> varbinary -> x
            signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                                     .returnType(inputType)
                                     .intermediateType("varbinary")
                                     .argumentType(inputType)
                                     .argumentType("bigint")
                                     .argumentType("double")
                                     .build());
          }

Testing
-------

It is time to put all the pieces together and test how well the new function
works.

Use AggregationTestBase from velox/aggregates/tests/AggregationTestBase.h as a
base class for the test.

If the new aggregate function is supported by `DuckDB
<https://duckdb.org/docs/sql/aggregates>`_, you can use DuckDB to check
results. In this case you specify input data, grouping keys, a list of
aggregates and a SQL query to run on DuckDB to calculate the expected results
and call helper function testAggregates defined in AggregationTestBase class.
Grouping keys can be empty for global aggregations.

.. code-block:: c++

    // Global aggregation.
    testAggregations(vectors, {}, {"sum(c1)"}, "SELECT sum(c1) FROM tmp");

    // Group by aggregation.
    testAggregations(
        vectors, {"c0"}, {"sum(c1)"}, "SELECT c0, sum(c1) FROM tmp GROUP BY 1");

If the new function is not supported by DuckDB, you need to specify the expected
results manually.

.. code-block:: c++

    // Global aggregation.
    testAggregations(vectors, {}, {"map_union(c1)"}, expectedResult);

    // Group by aggregation.
    testAggregations(vectors, {"c0"}, {"map_union(c1)"}, expectedResult);

Under the covers, testAggregations method generates multiple different but
logically equivalent plans, executes these plans, verifies successful
completion and compares the results with DuckDB or specified expected results.

The following query plans are being tested.

* Partial aggregation followed by final aggregation. Query runs
  single-threaded.
* Single aggregation. Query runs single-threaded.
* Partial aggregation followed by intermediate aggregation followed by final
  aggregation. Query runs single-threaded.
* Partial aggregation followed by local exchange on the grouping keys followed
  by final aggregation. Query runs using 4 threads.
* Local exchange using round-robin repartitioning followed by partial
  aggregation followed by local exchange on the grouping keys followed by
  final aggregation with forced spilling. Query runs using 4 threads.

Query run with forced spilling is enabled only for group-by aggregations and
only if `allowInputShuffle_` flag is enabled by calling allowInputShuffle
() method from the SetUp(). Spill testing requires multiple batches of input.
To split input data into multiple batches we add local exchange with
round-robin repartitioning before the partial aggregation. This changes the order
in which aggregation inputs are processed, hence, query results with spilling
are expected to be the same as without spilling only if aggregate functions used
in the query are not sensitive to the order of inputs. Many functions produce
the same results regardless of the order of inputs, but some functions may return
different results if inputs are provided in a different order. For
example, :func:`arbitrary`, :func:`array_agg`, :func:`map_agg` and
:func:`map_union` functions are sensitive to the order of inputs,
and :func:`min_by` and :func:`max_by` functions are sensitive to the order of
inputs in the presence of ties.

Function names
--------------

Same as scalar functions, aggregate function names are case insensitive. The names
are converted to lower case automatically when the functions are registered and
when they are resolved for a given expression.

Documentation
-------------

Finally, document the new function by adding an entry to velox/docs/functions/presto/aggregate.rst

You can see the documentation for all functions at :doc:`../functions/presto/aggregate` and read about how documentation is generated at https://github.com/facebookincubator/velox/tree/main/velox/docs#velox-documentation

Accumulator
-----------

In Velox, efficient use of memory is a priority. This includes both optimizing
the total amount of memory used as well as the number of memory allocations.
Note that runtime statistics reported by Velox include both peak memory usage
(in bytes) and number of memory allocations for each operator.

Aggregate functions use memory to store intermediate results in the
accumulators. They allocate memory from an arena (:doc:`HashStringAllocator <arena>` class).

array_agg and ValueList
~~~~~~~~~~~~~~~~~~~~~~~

StlAllocator is an STL-compatible allocator backed by HashStringAllocator that
can be used with STL containers. For example, one can define an std::vector
that allocates memory from the arena like so:

.. code-block:: c++

	std::vector<int64_t, StlAllocator<int64_t>>

This is used, for example, in 3-arg versions of :func:`min_by` and :func:`max_by` with
fixed-width type inputs (e.g. integers).

There is also an AlignedStlAllocator that provides aligned allocations from the
arena and can be used with `F14 <https://engineering.fb.com/2019/04/25/developer-tools/f14/>`_
containers which require 16-byte alignment. One can define an F14FastMap that
allocates memory from the arena like so:


.. code-block:: c++

   folly::F14FastMap<
         int64_t,
         double,
         std::hash<int64_t>,
         std::equal_to<int64_t>,
         AlignedStlAllocator<std::pair<const int64_t, double>, 16>>

You can find an example usage in :func:`histogram` aggregation function.

An :func:`array_agg` function on primitive types could be implemented using
std::vector<T>, but it would not be efficient. Why is that? If one doesn’t
use ‘reserve’ method to provide a hint to std::vector about how many entries will be
added, the default behavior is to allocate memory in powers of 2, e.g. first
allocate 1 entry, then 2, then 4, 8, 16, etc. Every time new allocation is
made the data is copied into the new memory buffer and the old buffer is
released. One can see this by instrumenting StlAllocator::allocate and
deallocate methods to add logging and run a simple loop to add elements to a
vector:

.. code-block:: c++

   std::vector<double, StlAllocator<double>> data(
      0, StlAllocator<double>(allocator_.get()));


   for (auto i = 0; i < 100; ++i) {
    data.push_back(i);
   }


.. code-block:: text

   E20230714 14:57:33.717708 975289 HashStringAllocator.h:497] allocate 1
   E20230714 14:57:33.734280 975289 HashStringAllocator.h:497] allocate 2
   E20230714 14:57:33.734321 975289 HashStringAllocator.h:506] free 1
   E20230714 14:57:33.734352 975289 HashStringAllocator.h:497] allocate 4
   E20230714 14:57:33.734381 975289 HashStringAllocator.h:506] free 2
   E20230714 14:57:33.734416 975289 HashStringAllocator.h:497] allocate 8
   E20230714 14:57:33.734445 975289 HashStringAllocator.h:506] free 4
   E20230714 14:57:33.734481 975289 HashStringAllocator.h:497] allocate 16
   E20230714 14:57:33.734513 975289 HashStringAllocator.h:506] free 8
   E20230714 14:57:33.734544 975289 HashStringAllocator.h:497] allocate 32
   E20230714 14:57:33.734575 975289 HashStringAllocator.h:506] free 16
   E20230714 14:57:33.734606 975289 HashStringAllocator.h:497] allocate 64
   E20230714 14:57:33.734637 975289 HashStringAllocator.h:506] free 32
   E20230714 14:57:33.734668 975289 HashStringAllocator.h:497] allocate 128
   E20230714 14:57:33.734699 975289 HashStringAllocator.h:506] free 64
   E20230714 14:57:33.734731 975289 HashStringAllocator.h:506] free 128


Reallocating memory and copying data is not cheap. To avoid this overhead we
introduced ValueList primitive and used it to implement array_agg.

ValueList is an append-only data structure that allows appending values from any
Velox Vector and reading values back into a Velox Vector. ValueList doesn’t
require a contiguous chunk of memory and therefore doesn’t need to re-allocate
and copy when it runs out of space. It just allocates another chunk and starts
filling that up.

ValueList is designed to work with data that comes from Velox Vectors, hence,
its API is different from std::vector. You append values from a DecodedVector
and read values back into a flat vector. Here is an example of usage:

.. code-block:: c++

   DecodedVector decoded(*data);

   // Store data.
   ValueList values;
   for (auto i = 0; i < 100; ++i) {
    values.appendValue(decoded, i, allocator());
   }


   // Read data back.
   auto copy = BaseVector::create(DOUBLE(), 100, pool());
   aggregate::ValueListReader reader(values);
   for (auto i = 0; i < 100; ++i) {
    reader.next(*copy, i);
   }

ValueList supports all types, so you can use it to append fixed-width values as
well as strings, arrays, maps and structs.

When storing complex types, ValueList serializes the values using
ContainerRowSerde.

ValueList preserves the null flags as well, so you can store a list of nullable
values in it.

The array_agg is implemented using ValueList for the accumulator.

ValueList needs a pointer to the arena for appending data. It doesn’t take an
arena in the constructor and doesn’t store it, because that would require 8
bytes of memory per group in the aggregation operator. Instead,
ValueList::appendValue method takes a pointer to the arena as an argument.
Consequently, ValueList’s destructor cannot release the memory back to the
arena and requires the user to explicitly call the free
(HashStringAllocator*) method.

min, max, and SingleValueAccumulator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`min` and :func:`max` functions store a single value in the accumulator
(the current min or max value). They use SingleValueAccumulator to store
strings, arrays, maps and structs. When processing a new value, we compare
it with the stored value and replace the stored value if necessary.

Similar to ValueList, SingleValueAccumulator serializes the values using
ContainerRowSerde. SingleValueAccumulator provides a compare method to compare
stored value with a row of a DecodedVector.

This accumulator is also used in the implementation of the :func:`arbitrary`
aggregate function which stores the first value in the accumulator.

set_agg, set_union, Strings and AddressableNonNullValueList
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`set_agg` function accumulates a set of unique values into an F14FastSet
configured to allocate memory from the arena via AlignedStlAllocator.
Fixed-width values are stored directly in F14FastSet. Memory allocation pattern
for F14 data structures is similar to std::vector. F14 allocates memory in
powers on 2, copies data and frees previously allocated memory. Hence, we do
not store strings directly in the F14 set. Instead, Velox writes strings into
the arena and stores a StringView pointing to the arena in the set.

In general, when writing to the arena, one is not guaranteed a contiguous write.
However, for StringViews to work we must ensure that strings written into the
arena are contiguous. Strings helper class provides this functionality. Its
append method takes a StringView and a pointer to the arena, copies the string
into the arena and returns a StringView pointing to the copy.

.. code-block:: c++

   /// Copies the string into contiguous memory allocated via
   /// HashStringAllocator. Returns StringView over the copy.
   StringView append(StringView value, HashStringAllocator& allocator);

Strings class provides a free method to release memory back to the arena.

.. code-block:: c++

   /// Frees memory used by the strings. StringViews returned from 'append'
   /// become invalid after this call.
   void free(HashStringAllocator& allocator);

When aggregating complex types (arrays, maps or structs), we use
AddressableNonNullValueList which writes values to the arena and returns
a “pointer” to the written value which we store in the F14 set.
AddressableNonNullValueList provides methods to compute a hash of a value and
compare two values. AddressableNonNullValueList uses ContainerRowSerde for
serializing data and comparing serialized values.


.. code-block:: c++

   /// A set of pointers to values stored in AddressableNonNullValueList.
   SetAccumulator<
      HashStringAllocator::Position,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

AddressableNonNullValueList allows to append a value and erase the last value.
This functionality is sufficient for set_agg and set_union. When processing a
new value, we append it to the list, get a “pointer”, insert that “pointer”
into F14 set and if the “pointer” points to a duplicate value we remove it from
the list.

Like all other arena-based accumulators, AddressableNonNullValueList provides a
free method to return memory back to the arena.

Note: AddressableNonNullValueList is different from ValueList in that it
provides access to individual values (hence, the “Addressable” prefix in the
name) while ValueList does not. With ValueList one can append values, then copy
all the values into a Vector. Adhoc access to individual elements is not
available in ValueList.

SetAccumulator<T> template implements a simple interface to accumulate unique
values. It is implemented using F14FastSet, Strings and
AddressableNonNullValueList. T can be a fixed-width type like int32_t or
int64_t, StringView or ComplexType.

addValue and addValues method allow to add one or multiple values from a vector.

.. code-block:: c++

   /// Adds value if new. No-op if the value was added before.
   void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator)/// Adds new values from an array.

   void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator)

size() method returns the number of unique values.

.. code-block:: c++

   /// Returns number of unique values including null.
   size_t size() const

extractValues method allows to extract unique values into a vector.

.. code-block:: c++

   /// Copies the unique values and null into the specified vector starting at
   /// the specified offset.
   vector_size_t extractValues(FlatVector<T>& values, vector_size_t offset)

   /// For complex types.
   vector_size_t extractValues(BaseVector& values, vector_size_t offset)

Both :func:`set_agg` and :func:`set_union` functions are implemented using
SetAccumulator.

map_agg, map_union, and MapAccumulator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`map_agg` function accumulates keys and values into a map. It discards
duplicate keys and keeps only one value for each unique key. Map_agg uses
MapAccumulator<T> template to accumulate the values. Similar to SetAccumulator,
MapAccumulator is built using F14FastMap, AlignedStlAllocator, Strings and
AddressableNonNullValueList.

insert() method adds a pair of (key, value) to the map discarding the value if matching key already exists.

.. code-block:: c++

   /// Adds key-value pair if entry with that key doesn't exist yet.
   void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator)

size() method returns the number of unique values.

extract() method copies the keys and the values into vectors, which can be combined to form a MapVector.

.. code-block:: c++

   void extract(
      const VectorPtr& mapKeys,
      const VectorPtr& mapValues,
      vector_size_t offset)

Both :func:`map_agg` and :func:`map_union` functions are implemented using
MapAccumulator.

When implementing new aggregate functions, consider using ValueList,
SingleValueAccumulator, Strings, AddressableNonNullValueList and F14
containers to put together an accumulator that uses memory efficiently.

Tracking Memory Usage
~~~~~~~~~~~~~~~~~~~~~

Aggregation operator needs to know how much memory is used for each group.
For example, this information is used to decide how many and which rows to
spill when memory is tight.

Aggregation functions should use RowSizeTracker to help track memory usage
per group. Aggregate base class provides helper method trackRowSize(group),
which can be used like so:

.. code-block:: c++

    rows.applyToSelected([&](vector_size_t row) {
        auto group = groups[row];
        auto tracker = trackRowSize(group);
        accumulator->append(...);
    });

The 'trackRowSize' method returns an instance of RowSizeTracker initialized
with a reference to the arena and a counter to increment on destruction. When
object returned by trackRowSize goes out of scope, the counter is updated to
add memory allocated since object's creation.

End-to-End Testing
------------------

To confirm that aggregate function works end to end as part of query, update testAggregations() test in TestHiveAggregationQueries.java in presto_cpp repo to add a query that uses the new function.

.. code-block:: java

    assertQuery("SELECT orderkey, array_agg(linenumber) FROM lineitem GROUP BY 1");

Overwrite Intermediate Type in Presto
-------------------------------------

Sometimes we need to change the intermediate type of aggregation function in
Presto, due to the differences in implementation or in the type information
worker node receives.  This is done in Presto class
``com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager``.  When
``FeaturesConfig.isUseAlternativeFunctionSignatures()`` is enabled, we can
register a different set of function signatures used specifically by Velox.  An
example of how to create such alternative function signatures from scratch can
be found in
``com.facebook.presto.operator.aggregation.AlternativeApproxPercentile``.  An
example pull request can be found at
https://github.com/prestodb/presto/pull/18386.
