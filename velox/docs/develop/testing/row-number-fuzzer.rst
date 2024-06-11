================
RowNumber Fuzzer
================

The RowNumberFuzzer is a testing tool that automatically generate equivalent query plans and then executes these plans
to validate the consistency of the results. It works as follows:

1. Data Generation: It starts by generating a random set of input data, also known as a vector. This data can
   have a variety of encodings and data layouts to ensure thorough testing.
2. Plan Generation: Generate two equivalent query plans, one is row-number over ValuesNode as the base plan.
   and the other is over TableScanNode as the alter plan.
3. Query Execution: Executes those equivalent query plans using the generated data and asserts that the results are
   consistent across different plans.
  i. Execute the base plan, compare the result with the reference (DuckDB or Presto) and use it as the expected result.
  #. Execute the alter plan multiple times with and without spill, and compare each result with the
     expected result.
4. Iteration: This process is repeated multiple times to ensure reliability and robustness.

How to run
----------

Use velox_row_number_fuzzer_test binary to run rowNumber fuzzer:

::

    velox/exec/tests/velox_row_number_fuzzer_test --seed 123 --duration_sec 60

By default, the fuzzer will go through 10 iterations. Use --steps
or --duration-sec flag to run fuzzer for longer. Use --seed to
reproduce fuzzer failures.

Here is a full list of supported command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and
  evaluates one expression or aggregation. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps``
  and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v=1``: Verbose logging (from `Google Logging Library <https://github.com/google/glog#setting-flags>`_).

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--num_batches``: The number of input vectors of size `--batch_size` to
  generate. Default is 5.

* ``--enable_spill``: Whether to test with spilling or not. Default is true.

* ``--presto_url`` The PrestoQueryRunner url along with its port number.

* ``--req_timeout_ms`` Timeout in milliseconds of an HTTP request to the PrestoQueryRunner.

* ``--arbitrator_capacity``: Arbitrator capacity in bytes. Default is 6L << 30.

* ``--allocator_capacity``: Allocator capacity in bytes. Default is 8L << 30.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
