==========================================
Aggregation and Window Fuzzer
==========================================

Aggregation Fuzzer
------------------

Velox allows users to define UDAFs (user-defined aggregate functions) and provides
an Aggregation Fuzzer and a Window Fuzzer tools to test the engine and UDAFs thoroughly.
These tools are being used to test builtin Presto and Spark functions and have discovered
numerous bugs caused by corner cases that are difficult to cover in unit tests.

The Aggregation Fuzzer tests the HashAggregation operator, the StreamingAggregation
operator and UDAFs by generating random aggregations and evaluating these on
random input vectors.

The Aggregation Fuzzer tests global aggregations (no grouping keys), group-by
aggregations (one or more grouping keys), distinct aggregations(no aggregates),
aggregations with and without masks, aggregations over sorted and distinct inputs.

The Aggregation Fuzzer includes testing of spilling and abandoning partial
aggregation.

The results of aggregations using functions supported by DuckDB are compared
with DuckDB results.

For each aggregation, Fuzzer generates multiple logically equivalent plans and
verifies that results match. These plans are:

- Single aggregation (raw input, final result).
- Partial -> Final aggregation.
- Partial -> Intermediate -> Final aggregation.
- Partial -> LocalExchange(grouping keys) -> Final aggregation.
- All of the above using flattened input vectors.

In addition, to test StreamingAggregation operator, Fuzzer generates plans
using OrderBy and StreamingAggregation.

- OrderBy(grouping keys) -> Single streaming aggregation (raw input, final result).
- OrderBy(grouping keys) -> Partial streaming -> Final streaming aggregation.
- OrderBy(grouping keys) -> Partial streaming -> Intermediate streaming
  -> Final streaming aggregation.
- OrderBy(grouping keys) -> Partial streaming -> LocalMerge(grouping keys)
  -> Final streaming aggregation.
- All of the above using flattened input vectors.

Fuzzer iterations alternate between generating plans using Values or TableScan
nodes.

Many functions work well with random input data. However, some functions have
restrictions on the input values and random data tend to violate these causing
failures and preventing the fuzzer from exercising the aggregation beyond the
initial sanity checks.

For example, “min” function has 2 signatures:

.. code-block::

    min(x) → [same as x]
    Returns the minimum value of all input values.

    min(x, n) → array<[same as x]>
    Returns n smallest values of all input values of x. n must be a positive integer and not exceed 10,000.

The second signature, let's call it min_n, has 2 arguments. The first argument
is the value and the second is a constant number of minimum values to return.
Most of the time, randomly generated value for the second argument doesn’t fall
into [1, 10’000] range and aggregation fails:

.. code-block::

    VeloxUserError
    Error Source: USER
    Error Code: INVALID_ARGUMENT
    Reason: (3069436511015786487 vs. 10000) second argument of max/min must be less than or equal to 10000
    Retriable: False
    Expression: newN <= 10'000
    Function: checkAndSetN
    File: /Users/mbasmanova/cpp/velox-1/velox/functions/prestosql/aggregates/MinMaxAggregates.cpp
    Line: 574

Similarly, approx_distinct function has a signature that allows to specify max
standard error in the range of [0.0040625, 0.26000]. Random values for 'e' have
near zero chance to fall into this range.

To enable effective testing of these functions, Aggregation Fuzzer allows
registering custom input generators for individual functions.

When testing aggregate functions whose results depend on the order of inputs
(e.g. map_agg, map_union, arbitrary, etc.), the Fuzzer verifies that all plans
succeed or fail with compatible user exceptions. When plans succeed, the Fuzzer
verifies that number of result rows is the same across all plans.

Additionally, Fuzzer tests order-sensitive functions using aggregations over
sorted inputs. When inputs are sorted, the results are deterministic and therefore
can be verified.

Fuzzer also supports specifying custom result verifiers. For example, array_agg
results can be verified by first sorting the result arrays. Similarly, map_agg
results can be partially verified by transforming result maps into sorted arrays
of map keys. approx_distinct can be verified by comparing the results with
count(distinct).

A custom verifier may work by comparing results of executing two logically
equivalent Velox plans or results of executing Velox plan and equivalent query
in Reference DB. These verifiers using transform the results to make them
deterministic, then compare. This is used to verify array_agg, set_agg,
set_union, map_agg, and similar functions.

A custom verifier may also work by analyzing the results of single execution
of a Velox plan. For example, approx_distinct verifies the results by
computing count(distinct) on input data and checking whether the results
of approx_distinct are within expected error bound. Verifier for approx_percentile
works similarly.

At the end of the run, Fuzzer prints out statistics that show what has been
tested:

.. code-block::

    ==============================> Done with iteration 5683
    Total functions tested: 31
    Total masked aggregations: 1011 (17.79%)
    Total global aggregations: 500 (8.80%)
    Total group-by aggregations: 4665 (82.07%)
    Total distinct aggregations: 519 (9.13%)
    Total aggregations verified against DuckDB: 2537 (44.63%)
    Total failed aggregations: 1061 (18.67%)

.. _window-fuzzer:

Window Fuzzer
-------------

The Window fuzzer tests the Window operator with window and aggregation
functions by generating random window queries and evaluating them on
random input vectors. Results of the window queries can be compared to
Presto as the source of truth.

For each window operation, fuzzer generates multiple logically equivalent
plans and verifies that results match. These plans include

- Values -> Window
- TableScan -> PartitionBy -> Window
- Values -> OrderBy -> Window (streaming)
- TableScan -> OrderBy -> Window (streaming)

Window fuzzer currently doesn't use any custom result verifiers. Functions
that require custom result verifiers are left unverified.

How to integrate
---------------------------------------

To integrate with the Aggregation Fuzzer, create a test, register all
aggregate functions supported by the engine, and call
``AggregationFuzzerRunner::run()`` defined in `AggregationFuzzerRunner.h`_. See
`AggregationFuzzerTest.cpp`_.

.. _AggregationFuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/exec/fuzzer/AggregationFuzzer.h

.. _AggregationFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/fuzzer/AggregationFuzzerTest.cpp

Aggregation Fuzzer allows to indicate functions whose results depend on the
order of inputs and optionally provide custom result verifiers. The Fuzzer
also allows to provide custom input generators for individual functions.

Integration with the Window Fuzzer is similar to Aggregation Fuzzer. See
`WindowFuzzerRunner.h`_ and `WindowFuzzerTest.cpp`_.

.. _WindowFuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/exec/fuzzer/WindowFuzzer.h

.. _WindowFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/fuzzer/WindowFuzzerTest.cpp

How to run
----------------------------

All fuzzers support a number of powerful command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and evaluates one expression or aggregation. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps`` and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v=1``: Verbose logging (from `Google Logging Library <https://github.com/google/glog#setting-flags>`_).

* ``–-only``: A comma-separated list of functions to use in generated expressions.

* ``--num_batches``: The number of input vectors of size `--batch_size` to generate. Default is 10.

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--null_ratio``: Chance of adding a null constant to the plan, or null value in a vector (expressed as double from 0 to 1). Default is 0.1.

* ``--max_num_varargs``: The maximum number of variadic arguments fuzzer will generate for functions that accept variadic arguments. Fuzzer will generate up to max_num_varargs arguments for the variadic list in addition to the required arguments by the function. Default is 10.

* ``--presto_url``: Presto coordinator URI along with port.

* ``--req_timeout_ms``: Timeout in milliseconds for HTTP requests made to the reference DB, such as Presto.

In addition, Window Fuzzer supports verifying window query results against reference DB:

* ``--enable_window_reference_verification``: When true, the results of the window aggregation are compared to reference DB results. Default is false.

`WindowFuzzerTest.cpp`_ and `AggregationFuzzerTest.cpp`_ allow results to be
verified against Presto. To setup Presto as a reference DB, please follow these
`instructions`_. The following flags control the connection to the presto
cluster; ``--presto_url`` which is the http server url along with its port number
and ``--req_timeout_ms`` which sets the request timeout in milliseconds. The
timeout is set to 1000 ms by default but can be increased if this time is
insufficient for certain queries. Example command:

::

    velox/functions/prestosql/fuzzer:velox_window_fuzzer_test --enable_window_reference_verification --presto_url="http://127.0.0.1:8080" --req_timeout_ms=2000 --duration_sec=60 --logtostderr=1 --minloglevel=0

.. _instructions: https://github.com/facebookincubator/velox/issues/8111

How to reproduce failures
-------------------------------------

Similar to :doc:`Expression Fuzzer </develop/testing/expression-fuzzer>`, developers
can use ``--seed`` with the Aggregation Fuzzer and Window Fuzzer to reproduce a failed
iteration.

With the Aggregation Fuzzer, developers can also use ``--repro_persist_path``
when running the fuzzer to save the input vectors and the aggregation
query plans to files. The developers can then use velox/exec/tests/velox_aggregation_runner_test
to rerun the saved query plans with save input vectors.
