=================
Expression Fuzzer
=================

Velox allows users to define UDFs and provides an expression fuzzer tool to
test expression evaluation engine and UDFs thoroughly. It is being used to
test builtin Presto and Spark UDFs and has discovered numerous bugs caused
by corner cases that are difficult to cover in unit tests.

The expression fuzzer generates random expressions and evaluates these on
random input vectors. Each generated expression may contain multiple sub-
expressions and each input vector can have random and potentially nested
encodings.

To ensure that evaluation engine and UDFs handle vector encodings correctly,
the expression fuzzer evaluates an expression twice and asserts the results to
be the same: using regular evaluation path and using simplified evaluation that
flattens all input vectors before evaluating an expression.

How to integrate with expression fuzzer
---------------------------------------

Integrating with the expression fuzzer is simple. All you need to do is to
create a test, register all the custom UDFs and the Velox built-in UDFs that
the engine is going to support, and call ``FuzzerRunner::run()`` defined in
`FuzzerRunner.h`_. An example can be found at
`ExpressionFuzzerTest.cpp`_.

.. _FuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/expression/tests/ExpressionFuzzer.h

.. _ExpressionFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/expression/tests/ExpressionFuzzerTest.cpp


A skip function list of UDF names is allowed to specify UDFs not expected to be
tested. UDFs in this list will not appear in the randomly generated
expressions.

How to run expression fuzzer
----------------------------

Expression fuzzer allows some powerful command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and evaluates one expression. Default is 10.

* ``–-duration_sec``: How many seconds to run. If both ``-–steps`` and ``-–duration_sec`` are specified, –duration_sec takes effect.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v=1``: Verbose logging.

* ``–-only``: A comma-separated list of functions to use in generated expressions.

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

If you are running from CLion IDE, add ``--logtostderr=1`` to see the full
output.

How to reproduce fuzzer test failures
-------------------------------------

When a fuzzer test fails, a seed number and the evaluated expression are
printed to the log. An example is given below. Developers can use ``--seed``
with this seed number to rerun the exact same expression with the same inputs,
and use a debugger to investigate the issue. For the example below, the command
to reproduce the error would be ``velox/expression/tests/velox_expression_fuzzer_test --seed 1188545576``.

::

    I0819 18:37:52.249965 1954756 ExpressionFuzzer.cpp:685] ==============================> Started iteration 38
    (seed: 1188545576)
    I0819 18:37:52.250263 1954756 ExpressionFuzzer.cpp:578]
    Executing expression: in("c0",10 elements starting at 0 {120, 19, -71, null, 27, ...})
    I0819 18:37:52.250350 1954756 ExpressionFuzzer.cpp:581] 1 vectors as input:
    I0819 18:37:52.250401 1954756 ExpressionFuzzer.cpp:583] 	[FLAT TINYINT: 100 elements, 6 nulls]
    E0819 18:37:52.252044 1954756 Exceptions.h:68] Line: velox/expression/tests/ExpressionFuzzer.cpp:153, Function:compareVectors, Expression: vec1->equalValueAt(vec2.get(), i, i)Different results at idx '78': 'null' vs. '1', Source: RUNTIME, ErrorCode: INVALID_STATE
    terminate called after throwing an instance of 'facebook::velox::VeloxRuntimeError'
    ...

Note that changes to the set of all UDFs to test with invalidates this
reproduction, which can be affected by the skip function list, the ``--only``
argument, or the base commit, etc. This is because the chosen UDFs in the
expression are determined by both the seed and the pool of all UDFs to choose
from. So make sure you use the same configuration when reproducing a failure.

Accurate on-disk reproduction
-----------------------------

Sometimes developers may want to capture an issue and investigate later,
possibly by someone else using a different machine. Using ``--seed`` is not
sufficient to accurately reproduce the failure in this scenario. This could be
cased by different behaviors of random generator on different platforms,
additions/removals of UDFs from the list, and etc. To have an accurate
reproduction of a fuzzer failure regardless of environments you can record the
input vector and expression to files and replay these later.

1. Run Fuzzer using ``--seed`` and ``--repro_persist_path`` flags to save the input vector and expression to files in the specified directory.

2. Run Expression Runner using generated files.

``--repro_persist_path <path/to/directory>`` flag tells the Fuzzer to save
input vector and expression SQL to files in the specified directory and print
out the exact paths. Fuzzer uses :doc:`VectorSaver <vector-saver>` for storing vectors on disk
while preserving encodings.

ExpressionRunner takes a path to input vector, path to expression SQL and
"mode" and evaluates the specified expression on the specified data.
ExpressionRunner supports the following flags:

* ``--input_path`` path to input vector that was created by the Fuzzer

* ``--sql_path`` path to expression SQL that was created by the Fuzzer

* ``--result_path`` optional path to result vector that was created by the Fuzzer. Result vector is used to reproduce cases where Fuzzer passes dirty vectors to expression evaluation as a result buffer. This ensures that functions are implemented correctly, taking into consideration dirty result buffer.

* ``--mode`` run mode. One of "verify", "common" (default), "simplified".

    - ``verify`` evaluates the expression using common and simplified paths and compares the results. This is identical to a fuzzer run.

    - ``common`` evaluates the expression using common path and prints the results to stdout.

    - ``simplified`` evaluates the expression using simplified path and prints the results to stdout.

* ``--num_rows`` optional number of rows to process in common and simplified modes. Default: 10. 0 means all rows. This flag is ignored in 'verify' mode.

Example command:

::

    velox/expression/tests:velox_expression_runner_test --input_path "/path/to/input" --sql_path "/path/to/sql" --result_path "/path/to/result"

To assist debugging workload, ExpressionRunner supports ``--sql`` to specify
SQL expression on the command line. ``--sql`` option can be used standalone to
evaluate constant expression or together with ``--input_path`` to evaluate
expression on a vector. ``--sql`` and ``--sql_path`` flags are mutually
exclusive. If both are specified, ``--sql`` is used while ``--sql_path`` is
ignored. ``--sql`` option allow to specify multiple comma-separated SQL
expressions.

::

    $ velox/expression/tests:velox_expression_runner_test --sql "pow(2, 3), ceil(1.3)"

    I1101 11:32:51.955689 2306506 ExpressionRunner.cpp:127] Evaluating SQL expression(s): pow(2, 3), ceil(1.3)
    Result: ROW<_col0:DOUBLE,_col1:DOUBLE>
    8 | 2

    $ velox/expression/tests:velox_expression_runner_test --sql "pow(2, 3)"

    Evaluating SQL expression(s): pow(2, 3)
    Result: ROW<_col0:DOUBLE>
    8

    $ velox/expression/tests:velox_expression_runner_test --sql "array_sort(array[3,6,1,null,2])"
    Building: finished in 0.3 sec (100%) 817/3213 jobs, 0/3213 updated

    Evaluating SQL expression(s): array_sort(array[3,6,1,null,2])
    Result: ROW<_col0:ARRAY<INTEGER>>
    [1,2,3,6,null]

    $ velox/expression/tests:velox_expression_runner_test --sql "array_sort(array[3,6,1,null,2]), filter(array[1, 2, 3, 4], x -> (x % 2 == 0))"

    Evaluating SQL expression(s): array_sort(array[3,6,1,null,2]), filter(array[1, 2, 3, 4], x -> (x % 2 == 0))
    Result: ROW<_col0:ARRAY<INTEGER>,_col1:ARRAY<INTEGER>>
    [1,2,3,6,null] | [2,4]
