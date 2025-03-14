=================
Expression Fuzzer
=================

Velox allows users to define UDFs (user-defined functions) and provides an
Expression Fuzzer tool to test the engine and UDFs thoroughly. This tool is
being used to test builtin Presto and Spark functions and have discovered
numerous bugs caused by corner cases that are difficult to cover in unit tests.

The Expression Fuzzer tests the expression evaluation engine and UDFs by
generating random expressions and evaluating these on random input vectors.
Each generated expression may contain multiple sub-expressions and each input
vector can have random and potentially nested encodings.

To ensure that evaluation engine and UDFs handle vector encodings correctly, the
expression fuzzer evaluates each expression twice and asserts the results to be
the same: using regular evaluation path and using simplified evaluation that
flattens all input vectors before evaluating an expression.

How to integrate
---------------------------------------

To integrate with the Expression Fuzzer, create a test, register all scalar
functions supported by the engine, and call ``FuzzerRunner::run()`` defined in
`FuzzerRunner.h`_. See `ExpressionFuzzerTest.cpp`_.

.. _FuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/expression/fuzzer/ExpressionFuzzer.h

.. _ExpressionFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/expression/fuzzer/ExpressionFuzzerTest.cpp

Functions with known bugs can be excluded from testing using a skip-list.

Custom Input Generators
----------------------------

Custom input generators for expression fuzzer are needed when:

* A function requires input values to satisfy specific constraints (e.g., valid JSON or URL).

* The default random input generation does not provide sufficient coverage of edge cases.

A custom input generator can be defined with the following steps.

Step 1: Define the generator class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The developer can implement a custom input generator class by
inheriting from ``ArgValuesGenerator`` and overriding the ``generate()``
method. An example is the `JsonParseArgValuesGenerator`_ class.

.. _JsonParseArgValuesGenerator: https://github.com/facebookincubator/velox/blob/d25685bb6fddb76e467ee740d221455b1fac8f96/velox/expression/fuzzer/ArgValuesGenerators.h#L22

.. code-block:: c++

    class JsonParseArgValuesGenerator : public ArgValuesGenerator {
     public:
      ~JsonParseArgValuesGenerator() override = default;

      std::vector<core::TypedExprPtr> generate(
        const CallableSignature& signature,
        const VectorFuzzer::Options& options,
        FuzzerGenerator& rng,
        ExpressionFuzzerState& state) override;
    };


Step 2: Implement the generate() method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When implementing the ``generate()`` method, the developer first call
``populateInputTypesAndNames(signature, state)`` to append argument types
of ``signature`` and the corresponding input column names to ``state``.
Next, for every argument of ``signature`` in order, if it requires a custom
input generator, the developer emplace back an std::shared_ptr of a subclass
of ``AbstractInputGenerator`` into ``state.customInputGenerators_``. The
developer also emplace back a ``FieldAccessTypedExprPtr`` created with this
argument type and the corresponding input column name into ``inputExpressions``.
Every subclass of ``AbstractInputGenerator`` allows generating input values
satisfying certain constraint. E.g., ``RangeConstrainedGenerator`` allows
generating values in a specified range; ``JsonInputGenerator`` allows generating
valid JSON strings that represent a specified data type.

If the argument doesn't require a custom input generator, the developer emplace
a nullptr back into both ``state.customInputGenerators_`` and ``inputExpressions``.
Finally, this ``generate()`` method returns ``inputExpressions``.

.. code-block:: c++

    std::vector<core::TypedExprPtr> JsonParseArgValuesGenerator::generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) {
      VELOX_CHECK_EQ(signature.args.size(), 1);
      populateInputTypesAndNames(signature, state);

      const auto representedType = facebook::velox::randType(rng, 3);
      const auto seed = rand<uint32_t>(rng);
      const auto nullRatio = options.nullRatio;
      state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::JsonInputGenerator>(
            seed,
            signature.args[0],
            nullRatio,
            fuzzer::getRandomInputGenerator(seed, representedType, nullRatio),
            true));

      std::vector<core::TypedExprPtr> inputExpressions{
        signature.args.size(), nullptr};
      inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
        signature.args[0], state.inputRowNames_.back());
      return inputExpressions;
    };


Step 3: Register the custom input generator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once the custom input generator is implemented, the developer register it in
the ``main`` function in ExpressionFuzzerTest.cpp as follows.

.. code-block:: c++

    std::unordered_map<std::string, std::shared_ptr<ArgValuesGenerator>>
      argValuesGenerators = {
          {"json_parse", std::make_shared<JsonParseArgValuesGenerator>()},
          {"function_name", std::make_shared<CustomInputGeneratorClassName>()},
          ...};





How to run
----------------------------

Expression Fuzzer supports a number of powerful command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and evaluates one expression or aggregation. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps`` and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v=1``: Verbose logging (from `Google Logging Library <https://github.com/google/glog#setting-flags>`_).

* ``–-only``: A comma-separated list of functions to use in generated expressions.

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--null_ratio``: Chance of adding a null constant to the plan, or null value in a vector (expressed as double from 0 to 1). Default is 0.1.

* ``--max_num_varargs``: The maximum number of variadic arguments fuzzer will generate for functions that accept variadic arguments. Fuzzer will generate up to max_num_varargs arguments for the variadic list in addition to the required arguments by the function. Default is 10.

* ``--retry_with_try``: Retry failed expressions by wrapping it using a try() statement. Default is false.

* ``--enable_variadic_signatures``: Enable testing of function signatures with variadic arguments. Default is false.

* ``--special_forms``: Enable testing of specified special forms, including `and`, `or`, `cast`, `coalesce`, `if`, and `switch`. Every fuzzer test specifies the enabled special forms of its own. velox_expression_fuzzer_test has all the aforementioned special forms enabled by default.

* ``--enable_dereference``: Enable testing of the field-reference from structs and row_constructor functions. Default is false.

* ``--velox_fuzzer_enable_complex_types``: Enable testing of function signatures with complex argument or return types. Default is false.

* ``--velox_fuzzer_enable_decimal_type``: Enable testing of function signatures with decimal argument or return type. Default is false.

* ``--lazy_vector_generation_ratio``: Specifies the probability with which columns in the input row vector will be selected to be wrapped in lazy encoding (expressed as double from 0 to 1). Default is 0.0.

* ``--velox_fuzzer_enable_column_reuse``: Enable generation of expressions where one input column can be used by multiple subexpressions. Default is false.

* ``--velox_fuzzer_enable_expression_reuse``: Enable generation of expressions that re-uses already generated subexpressions. Default is false.

* ``--assign_function_tickets``: Comma separated list of function names and their tickets in the format <function_name>=<tickets>. Every ticket represents an opportunity for a function to be chosen from a pool of candidates. By default, every function has one ticket, and the likelihood of a function being picked can be increased by allotting it more tickets. Note that in practice, increasing the number of tickets does not proportionally increase the likelihood of selection, as the selection process involves filtering the pool of candidates by a required return type so not all functions may compete against the same number of functions at every instance. Number of tickets must be a positive integer. Example: eq=3,floor=5.

* ``--max_expression_trees_per_step``: This sets an upper limit on the number of expression trees to generate per step. These trees would be executed in the same ExprSet and can re-use already generated columns and subexpressions (if re-use is enabled). Default is 1.

* ``--velox_fuzzer_max_level_of_nesting``: Max levels of expression nesting. Default is 10 and minimum is 1.

If you would like to run Expression Fuzzer with Presto as the source of truth, two command line arguments can be used to specify the url and timeout of Presto:

* ``--presto_url``: Presto coordinator URI along with port.

* ``--req_timeout_ms``: Timeout in milliseconds for HTTP requests made to the reference DB, such as Presto.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.

An example set of arguments to run the expression fuzzer with all features enabled is as follows:
``--duration_sec 60
--enable_variadic_signatures
--lazy_vector_generation_ratio 0.2
--velox_fuzzer_enable_complex_types
--velox_fuzzer_enable_expression_reuse
--velox_fuzzer_enable_column_reuse
--retry_with_try
--enable_dereference
--special_forms="and,or,cast,coalesce,if,switch"
--max_expression_trees_per_step=2
--repro_persist_path=<a_valid_local_path>
--logtostderr=1``

Expression fuzzer with Presto as the source of truth currently only supports a subset of features:
``--duration_sec 60
--presto_url=http://127.0.0.1:8080
--req_timeout_ms 10000
--enable_variadic_signatures
--velox_fuzzer_enable_complex_types
--special_forms="cast,coalesce,if,switch"
--lazy_vector_generation_ratio 0.2
--velox_fuzzer_enable_column_reuse
--velox_fuzzer_enable_expression_reuse
--max_expression_trees_per_step 2
--logtostderr=1``

How to reproduce failures
-------------------------------------

When Fuzzer test fails, a seed number and the evaluated expression are
printed to the log. An example is given below. Developers can use ``--seed``
with this seed number to rerun the exact same expression with the same inputs,
and use a debugger to investigate the issue. For the example below, the command
to reproduce the error would be ``velox/expression/fuzzer/velox_expression_fuzzer_test --seed 1188545576``.

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

1. Run Fuzzer using ``--seed`` and ``--repro_persist_path`` flags to save the input vector and expression to files in the specified directory. Add "--persist_and_run_once" if the issue is not an exception failure but a crash failure.

2. Run Expression Runner using generated files.

``--repro_persist_path <path/to/directory>`` flag tells the Fuzzer to save the
input vector, initial result vector, expression SQL, and other relevant data to files in a new directory saved within
the specified directory. It also prints out the exact paths for these. Fuzzer uses :doc:`VectorSaver <../debugging/vector-saver>`
for storing vectors on disk while preserving encodings.

If an iteration crashes the process before data can be persisted, run the fuzzer
with the seed used for that iteration and use the following flag:

``--persist_and_run_once`` Persist repro info before evaluation and only run one iteration.
This is to rerun with the seed number and persist repro info upon a crash failure.
Only effective if repro_persist_path is set.

ExpressionRunner needs at the very least a path to input vector and path to expression SQL to run.
However, you might need more files to reproduce the issue. All of which will be present in the directory
that the fuzzer test generated. You can directly point the ExpressionRunner to that directory using --fuzzer_repro_path
where it will pick up all the files automatically or you can specify each explicitly using other startup flags.
ExpressionRunner supports the following flags:

* ``--fuzzer_repro_path`` directory path where all input files (required to reproduce a failure) that are generated by the Fuzzer are expected to reside. ExpressionRunner will automatically pick up all the files from this folder unless they are explicitly specified via their respective startup flag.

* ``--input_path`` path to input vector that was created by the Fuzzer

* ``--sql_path`` path to expression SQL that was created by the Fuzzer

* ``--registry`` function registry to use for evaluating expression. One of "presto" (default) or "spark".

* ``--complex_constant_path`` optional path to complex constants that aren't accurately expressable in SQL (Array, Map, Structs, ...). This is used with SQL file to reproduce the exact expression, not needed when the expression doesn't contain complex constants.

* ``--input_row_metadata_path`` optional path for the file stored on-disk which contains a struct containing input row metadata. This includes columns in the input row vector to be wrapped in a lazy vector and/or dictionary encoded. It may also contain a dictionary peel for columns requiring dictionary encoding. This is used when the failing test included input columns that were lazy vectors and/or had columns wrapped with a common dictionary wrap.

* ``--result_path`` optional path to result vector that was created by the Fuzzer. Result vector is used to reproduce cases where Fuzzer passes dirty vectors to expression evaluation as a result buffer. This ensures that functions are implemented correctly, taking into consideration dirty result buffer.

* ``--mode`` run mode. One of "verify", "common" (default), "simplified".

    - ``verify`` evaluates the expression using common and simplified paths and compares the results. This is identical to a fuzzer run.

    - ``common`` evaluates the expression using common path and prints the results to stdout.

    - ``simplified`` evaluates the expression using simplified path and prints the results to stdout.

    - ``query`` evaluate SQL query specified in --sql or --sql_path and print out results. If --input_path is specified, the query may reference it as table 't'.

* ``--num_rows`` optional number of rows to process in common and simplified modes. Default: 10. 0 means all rows. This flag is ignored in 'verify' mode.

* ``--store_result_path`` optional directory path for storing the results of evaluating SQL expression or query in 'common', 'simplified' or 'query' modes.

* ``--findMinimalSubExpression`` optional Whether to find minimum failing subexpression on result mismatch. Set to false by default.

* ``--useSeperatePoolForInput`` optional If true (default), expression evaluator and input vectors use different memory pools. This helps trigger code-paths that can depend on vectors having different pools. For eg, when copying a flat string vector copies of the strings stored in the string buffers need to be created. If however, the pools were the same between the vectors then the buffers can simply be shared between them instead.

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
