===========
Join Fuzzer
===========

Join fuzzer tests all types of hash and merge joins with up to 5 join keys and
up to 3 payload columns with and without spilling.

At each iteration, fuzzer randomly picks a join type (INNER, LEFT, FULL, LEFT
SEMI FILTER, LEFT SEMI PROJECT, ANTI), null-aware flag (if supported by the
join type), number and types of join keys and payload columns. The fuzzer then
generates probe-side and build-size inputs and constructs a Values -> HashJoin
query plan. When generating build-side data, the fuzzer generates either an
empty dataset or a 10% sample (with replacement) of the probe-side keys
combined with randomly generated payload. When generating the join plan node,
fuzzer shuffles join keys and output columns and randomly drops some columns
from the output.

The fuzzer runs the query plan and compares the results with DuckDB.

The fuzzer then generates a set of different but logically equivalent plans,
runs them and verifies that results are the same. Each plan runs twice: with
and without spilling.

The logically equivalent plans are generated as follows:

- Flatten possibly encoded join inputs.
- Flip join sides:
    - LEFT(a, b) => RIGHT(b, a)
    - LEFT SEMI FILTER(a, b) => RIGHT SEMI FILTER(b, a)
    - LEFT SEMI PROJECT(a, b) => RIGHT SEMI PROJECT(b, a)
- Introduce round-robin local exchange before the join:
  Values -> LocalExchange(ROUND_ROBIN) -> HashJoin
- Replace HashJoin with OrderBy(join keys) + MergeJoin for supported join
  types (INNER, LEFT).

How to run
----------

Use velox_join_fuzzer_test binary to run join fuzzer:

::

    velox/exec/tests/velox_join_fuzzer_test

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

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
