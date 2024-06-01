=========================
MemoryArbitration Fuzzer
=========================

The MemoryArbitrationFuzzer is a test tool designed to automatically generate and execute multiple query plans
in parallel with tight total memory budget. It aims to stress the memory arbitration processing, and validate there is
no crash or hanging in a concurrent query execution mode. The query either succeeds or failed with expected errors.
It works as follows:

1. Data Generation: It starts by generating a random set of input data, also known as a vector. This data can
   have a variety of encodings and data layouts to ensure thorough testing.
2. Plan Generation: Generate multiple plans with different query shapes. Currently, it supports HashJoin and
   HashAggregation plans.
3. Query Execution: Create multiple threads, each thread randomly picks a plan with spill enabled or not, and repeatedly
   running this process until ${iteration_duration_sec} seconds. The query thread expects query to succeed or fail with
   query OOM or abort errors, otherwise it throws.
4. Iteration: This process is repeated multiple times to ensure reliability and robustness.

How to run
----------

Use velox_memory_arbitration_fuzzer_test binary to run this fuzzer:

::

    velox/exec/tests/velox_memory_arbitration_fuzzer_test --seed 123 --duration_sec 60

By default, the fuzzer will go through 10 iterations. Use --steps
or --duration-sec flag to run fuzzer for longer. Use --seed to
reproduce fuzzer failures.

Here is a full list of supported command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and
  evaluates one expression or aggregation. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps``
  and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v``: Verbose logging (from `Google Logging Library <https://github.com/google/glog#setting-flags>`_).

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--num_batches``: The number of input vectors of size `--batch_size` to
  generate. Default is 5.

* ``--iteration_duration_sec``: For how long it should run (in seconds) per iteration.

* ``--arbitrator_capacity``: Arbitrator capacity in bytes.

* ``--allocator_capacity``: Allocator capacity in bytes.

* ``--num_threads``: Number of threads running queries in parallel per iteration.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
