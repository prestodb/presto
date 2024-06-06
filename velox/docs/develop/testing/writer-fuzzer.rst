=============
Writer Fuzzer
=============

Writer fuzzer tests table write plan with up to 5 regular columns, up to
3 partition keys and up to 3 bucket columns.

At each iteration, fuzzer randomly generate a table write plan with different
table properties including un-partitioned and partitioned, non-bucketed and bucketed.

The fuzzer then generates inputs and runs the query plan and compares the
results with PrestoDB.
As of now, we compare:
1. How many rows were written.
2. Output directories have the same directory layout and hierarchy.
3. Same data were written by velox and prestoDB.

How to run
----------

Use velox_writer_fuzzer_test binary to run join fuzzer:

::

    velox/exec/tests/velox_writer_fuzzer_test

By default, the fuzzer will go through 10 interations. Use --steps
or --duration-sec flag to run fuzzer for longer. Use --seed to
reproduce fuzzer failures.

Here is a full list of supported command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and
  evaluates one tale writer plan. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps``
  and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--num_batches``: The number of input vectors of size `--batch_size` to
  generate. Default is 5.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
