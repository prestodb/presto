********************
March 2022 Update
********************

Documentation
-------------

* Document :doc:`printPlanWithStats <../develop/debugging/print-plan-with-stats>` debugging tool.

Core Library
------------

* Add support for UNNEST WITH ORDINALITY for single array or map.
* Add support for filter to INNER and LEFT merge joins.
* Add support for FULL OUTER hash join.
* Add Substrait-to-Velox plan conversion.
* Add exec::resolveAggregateFunction API to resolve aggregate function final and intermediate types based on function name and input types.
* Add KLL sketch implementation to be used to implement :func:`approx_percentile` Presto function with accuracy parameter. :pr:`1247`
* Add GTest as a submodule and upgrade it to 1.11.0 (from 1.10.0).
* Reduce dependencies for VELOX_BUILD_MINIMAL build by removing GTest, GMock and compression libraries. :pr:`1292`
* Simplify simple functions API to allow to provide void call method for functions that never return null for non-null inputs.
* Optimize LocalMerge operator. :pr:`1264`
* Optimize PartitionedOutput and Exchange operators by removing unnecessary data copy. :pr:`1127`
* Optimize BaseVector::compare() for a pair of flat or a pair of constant vectors or scalar types. :pr:`1317`
* Optimize BaseVector::copy() for flat vectors of scalar type. :pr:`1316`
* Optimize aggregation by enabling pushdown into table scan in more cases. :pr:`1188`
* Optimize simple functions that never return null for non-null input for 70% perf boost. :pr:`1152`
* Fix bucket-by-bucket (grouped) execution of join queries.
* Fix crashes and correctness bugs in joins and aggregations.

Presto Functions
----------------

* Add support for CAST(x as JSON).
* Add :func:`arrays_overlap` function.
* Extend :func:`hour` function to support TIMESTAMP WITH TIME ZONE inputs.
* Fix :func:`parse_datetime` and :func:`to_unixtime` semantics. :pr:`1277`
* Fix :func:`approx_distinct` to return 0 instead of null with all inputs are null.

Performance and Correctness Testing
-----------------------------------

* Add linux-benchmarks-basic CircleCI job to run micro benchmarks on each PR. Initial set of benchmarks covers SelectivityVector, DecodedVector, simple comparisons and conjuncts.
* Add TPC-H benchmark with support for q1, q6 and q18.
* Add support for approximate verification of REAL and DOUBLE values returned by test queries.
* Add support for ORDER BY SQL clauses to PlanBuilder::localMerge() and PlanBuilder::orderBy().

Debugging Experience
--------------------

* Improve error messages in expression evaluation by including the expression being evaluated. :pr:`1138`
* Improve error messages in CAST expression by including the to and from types. :pr:`1150`
* Add printPlanWithStats function to print query plan with runtime statistics.
* Add SelectivityVector::toString() method.
* Improve ConstantExpr::toString() to include constant value.
* Add runtime statistic "loadedToValueHook" to track number of values processed via aggregation pushdown into scan.

Credits
-------

Aditi Pandit, Amit Dutta, Amlan Nayak, Chad Austin, Chao Chen, David Greenberg,
Deepak Majeti, Dimitri Bouche, Gilson Takaasi Gil, Hanqi Wu, Huameng Jiang,
Jialiang Tan, im Meyering, Jimmy Lu, Karteek Murthy Samba Murthy, Kevin
Wilfong, Krishna Pai, Laith Sakka, Liang Tao, MJ Deng, Masha Basmanova, Orri
Erling, Paula Lahera, Pedro Eugenio Rocha Pedreira, Pradeep Garigipati, Richard
Barnes, Rui Mo, Sagar Mittal, Sergey Pershin, Simon Marlow, Siva Muthusamy,
Sridhar Anumandla, Victor Zverovich, Wei He, Wenlei Xie, Yoav Helfman, Yuan
Chao Chou, Zhenyuan Zhao, tanjialiang


********************
Feature Of The Month
********************


Using vector readers/writers to simplify dealing with Velox vectors.
--------------------------------------------------------------------


Although vector readers and writers were created originally as part of the simple function's interface, they are highly
convenient tools that can be used in isolation in aggregate and vector functions implementations, and in general
anywhere we want to read or write vectors. Using those constructs reduces code size and simplifies it, without adding
performance overhead.

In this note, I will explain how such constructs can be used to read or write vectors in a simple convenient way.

Using vector readers and vector writers has several benefits:

* Hides the complexity of decoding and significantly reduces code size, especially for nested complex types.
* Provides the user with STL-like objects that represent elements of maps, arrays, and tuples, making it easier to focus on the logic. E.g. convert an ArrayVector to a sequence of ArrayViews that have std::vector interface.
* Reduce duplicate code and bugs, especially for engineers without a lot of experience in Velox.
* VectorReaders and VectorWriters are efficient, lazy, and should always be preferred.

Vector reader
^^^^^^^^^^^^^
Consider a vector  of type Array<Map<int, int>>. The code below reads the vector and iterates over its content.
For every row, the code reads an array of maps stored at that row.

.. code-block:: c++

    // Decode the vector for rows of interest.
    DecodedVector decoded;
    decoded.decode(vector, rows);

    // Define a vector reader for an Array<Map<int, int>>.
    exec::VectorReader<Array<Map<int64_t, int64_t>>> reader(decoded);

    rows.applyToSelected([&](vector_size_t row) {
        // Check if the row is null.
        if(!reader.isSet(row)) {
            return;
        }

        // Read the row as ArrayView. ArrayView has std::vector<std::optional<V>> interface.
        auto arrayView = reader[row];

        // Elements of the array have std::map<int, std::optional<int>> interface.
        for(const auto&[key, val]:  arrayView.value()) {
          ...
        }
    });

The general workflow is to:

#. Decode the vector for the rows of interest.
#. Define vectorReader<T> where T is the type of the vector being read, T is expressed in the simple function type system.
#. To read a row, call reader[row] and it will return a STL-like object that represents the elements at the row.
#. The code above can be extended to any type supported in the simple function interface. The type returned by reader[row] will be the same input type in the call function in the simple function interface for that type. (e.g: bool, int, StringView, ArrayView ..etc).

Vector writer
^^^^^^^^^^^^^
Now consider a function that generates Array<int64_t> as an output. The result vector can be written as the following.

.. code-block:: c++

    VectorPtr result;
    // Here type is ArrayType(BIGINT()).
    BaseVector::ensureWritable(rows, type, pool_, &result);

    // Define a vector writer. ArrayWriterT is a temp holder. Eventually, Array will be used
    // once old writers are deprecated.
    exec::VectorWriter<ArrayWriterT<int64_t>> vectorWriter;

    // Initialize the writer to write to result vector.
    vectorWriter.init(*result->as<ArrayVector>());

    rows.applyToSelected([&](vector_size_t row) {
        // Specify the row to write.
        vectorWriter.setOffset(row);

        // Get writer to the selected offset.
        auto& arrayWriter = vectorWriter.current();
        arrayWriter.push_back(1);
        arrayWriter.push_back(100);
        ..etc

        // Indicate writing for the row is done.
        vectorWriter.commit();
    });
    // Indicate writing result vector is done.
    vectorWriter.finish();

The general workflow is to:

#. Make sure the vector is writable for the rows of interest (ensureWritable).
#. Define vectorWriter<T> where T is the type expressed in the simple function type system.
#. Call init() to initialize the vectorWriter with the vector to write.
#. To write to a specific row call setOffset(row) followed by current() to get the writer at that row.
#. After finishing writing the row call commit(), or commit(false) to write a null.
#. After finishing writing all rows call finish().

Note the following:

* The type returned by current() is the writer type which is the same type that represents the output in the simple function interface. E.g: bool&, int32_t&, StringWriter, ArrayWriter, ..etc.
* More details about writers of complex types will be added to the documentation.
* The VectorWriter allows out-of-order writing to rows. E.g, writing row 0 after row 10. However, it does not allow writing to multiple rows in parallel.

Reading optional-free container
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If the user knew that a vector does not have null data, there is an option to read an optional-free container using
readNullFree(). For the example above, it will return a container similar to std::vector<std::map<int, int>> instead of
std::vector<std::optional<std::map<int, std::optional<int>>>.

The type returned by readNullFree is the same input type passed to the callNullFree function in the simple function
interface. The code below shows an example:

.. code-block:: c++

    // Decode the vector for rows of interest.
    DecodedVector decoded;
    decoded.decode(vector, rows);

    // Define a vector reader for an Array<Map<int, int>>.
    exec::VectorReader<Array<Map<int64_t, int64_t>>> reader(decoded);

    // Make sure there is no null data.
    assert(!decoded.mayHaveNullsRecursive());

    rows.applyToSelected([&](vector_size_t row) {
      // Read the row as NullFreeArrayView with interface similar to std::vector<V>.
      auto arrayView = reader.readNullFree(row);

      // Elements of the array have std::map<int, int> interface.
      for(const auto&[key, val]:  arrayView){
          ...
      }
    });