==================
printExprWithStats
==================

Velox collects a number of statistics during query execution that allow us to
reason about the execution dynamic and troubleshoot performance issues.
Initially these statistics were collected at the operator level and accessed
via Task::taskStats(). Recently we started collecting runtime statistics for
individual expressions and introduced a helper function printExprWithStats to
print the expression tree annotated with runtime stats. This allows us to
identify bottlenecks in the individual functions.

printExprWithStats
------------------

This is a helper method that can be used to print out the expression tree
annotated with runtime statistics for individual expressions. The stats include
CPU time and number of processed rows.

For example, here is what the output of printExprWithStats may look like for an
expression (c0 + 3) * c1 evaluated on 1024 rows represented using flat
vectors.

.. code-block::

    multiply [cpu time: 52.77us, rows: 1024] -> BIGINT [#1]
       plus [cpu time: 83.77us, rows: 1024] -> BIGINT [#2]
          c0 [cpu time: 0ns, rows: 0] -> BIGINT [#3]
          3:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT [#4]
       c1 [cpu time: 0ns, rows: 0] -> BIGINT [#5]

If the same expression was evaluated on dictionary-encoded data, the stats would
show that evaluation processed fewer rows (i.e. number of rows with unique
values) and used less CPU time:

.. code-block::

    multiply [cpu time: 21.46us, rows: 205] -> BIGINT [#1]
       plus [cpu time: 32.34us, rows: 205] -> BIGINT [#2]
          c0 [cpu time: 0ns, rows: 0] -> BIGINT [#3]
          3:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT [#4]
       c1 [cpu time: 0ns, rows: 0] -> BIGINT [#5]

printExprWithStats can be used to explore various optimizations in the
expression evaluation and investigate whether these applied in a particular
run.

For example, we can demonstrate common sub-expression elimination using an
expression set with two expressions: (c0 + c1) % 5, (c0 + c1) % 3. Here,
(c0 + c1) is a common sub-expression that is evaluated only once. The output of
printExprWithStats shows that the second instance is a duplicate. It is
annotated with [CSE #2] which allows us to identify the original expression.
Runtime stats for duplicate expressions are not displayed.

.. code-block::

    mod [cpu time: 49.98us, rows: 1024] -> BIGINT [#1]
       plus [cpu time: 53.75us, rows: 1024] -> BIGINT [#2]
          c0 [cpu time: 0ns, rows: 0] -> BIGINT [#3]
          c1 [cpu time: 0ns, rows: 0] -> BIGINT [#4]
       5:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT [#5]

    mod [cpu time: 53.46us, rows: 1024] -> BIGINT [#6]
       plus(c0, c1) -> BIGINT [CSE #2]
       3:BIGINT [cpu time: 0ns, rows: 0] -> BIGINT [#7]

Another example is dictionary memoization. If we evaluate an expression over
multiple batches of data of the column using the same base dictionary, the
evaluation remembers the results for the dictionary rows it has seen and
evaluates only the new rows.

For example, we evaluated log2(c0) * 1.5 expressions on 3 batches of data. Each
batch had 1024 rows dictionary encoded using the same dictionary. The
dictionary also has 1024 rows. First batch referenced first ⅕ of the dictionary
rows repeating each 5 times. Second batch referenced the first  ⅓ of the
dictionary rows repeating each 3 times. Third batch referenced the first 1/10
of the dictionary rows repeating each 10 times.

After evaluating the first batch printExprWithStats showed that expression was
evaluated on 205 rows ( ⅕ of 1024).

.. code-block::

    multiply [cpu time: 29.63us, rows: 205] -> DOUBLE [#1]
       log2 [cpu time: 45.80us, rows: 205] -> DOUBLE [#2]
          c0 [cpu time: 0ns, rows: 0] -> DOUBLE [#3]
       1.5:DOUBLE [cpu time: 0ns, rows: 0] -> DOUBLE [#4]

After evaluating the second batch printExprWithStats showed that expression was
evaluated on 137 additional rows (137 = ⅓ - ⅕ of 1024). This is expected as we
have already computed the results for the first 205 rows and only need to
compute the results for the remaining 137 rows.

.. code-block::

    multiply [cpu time: 46.30us, rows: 342] -> DOUBLE [#1]
       log2 [cpu time: 68.59us, rows: 342] -> DOUBLE [#2]
          c0 [cpu time: 0ns, rows: 0] -> DOUBLE [#3]
       1.5:DOUBLE [cpu time: 0ns, rows: 0] -> DOUBLE [#4]

After evaluating the third batch printExprWithStats shows that expression was
not evaluated on any additional rows. This is expected as we have already
computed the results on all the necessary rows.

.. code-block::

    multiply [cpu time: 46.30us, rows: 342] -> DOUBLE [#1]
       log2 [cpu time: 68.59us, rows: 342] -> DOUBLE [#2]
          c0 [cpu time: 0ns, rows: 0] -> DOUBLE [#3]
       1.5:DOUBLE [cpu time: 0ns, rows: 0] -> DOUBLE [#4]

ExprSetListener
---------------

One can register a listener that is invoked on destruction of the ExprSet and
receives the aggregated runtime statistics for the whole expression tree. The
statistics of the individual expressions as combined based on the expression
name, i.e. function name or built-in expression name such as AND, OR, IF,
SWITCH, etc. The listener can be used to log the runtime stats for offline
analysis.

.. code-block:: c++

    /// Listener invoked on ExprSet destruction.
    class ExprSetListener {
     public:
      virtual ~ExprSetListener() = default;

      /// Called on ExprSet destruction. Provides runtime statistics about
      /// expression evaluation.
      /// @param uuid Universally unique identifier of the set of expressions.
      /// @param event Runtime stats.
      virtual void onCompletion(
          const std::string& uuid,
          const ExprSetCompletionEvent& event) = 0;
    };

    /// Register a listener to be invoked on ExprSet destruction. Returns true if
    /// listener was successfully registered, false if listener is already
    /// registered.
    bool registerExprSetListener(std::shared_ptr<ExprSetListener> listener);
