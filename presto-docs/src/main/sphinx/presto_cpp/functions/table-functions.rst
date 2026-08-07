**************************
Presto C++ Table Functions
**************************

Table functions return tables and are invoked in the ``FROM`` clause of a
query. For the SQL syntax and user-facing semantics, see :doc:`/functions/table`.
For the Java developer API, including the semantics of scalar, descriptor, and
table arguments, see :doc:`/develop/table-functions`.

This page focuses on the native development workflow for table functions in
Presto C++. Today, that work is centered on the native execution stack rather
than on the Java SPI classes described in the Java developer guide.

Where the work lives
====================

When adding or modifying a table function for Presto C++, expect the work to
span the native worker and the Velox engine it uses underneath:

* The Presto C++ worker lives in the ``presto-native-execution`` tree.
* The native developer setup uses a Velox checkout together with the worker
  build, and native function work is commonly carried in that Velox checkout.
* The :doc:`/presto_cpp/plugin/function_plugin` page documents dynamic loading
  for scalar UDFs. Use that page as a reference for native build and deployment
  patterns, but do not assume that the scalar-UDF registration flow applies
  unchanged to table functions.

Recommended workflow
====================

1. Start from the SQL contract

   Define the fully qualified function name, the arguments, and the output
   schema first. Reuse the general table function semantics from
   :doc:`/develop/table-functions` for:

   * scalar, descriptor, and table arguments
   * partitioning and ordering on table arguments
   * pass-through columns
   * ``PRUNE WHEN EMPTY`` and ``KEEP WHEN EMPTY``

2. Implement the native execution behavior

   Keep the table function logic in the native execution stack used by Presto
   C++. In practice, this usually means implementing the behavior in the Velox
   layer used by the worker and then wiring the function into the native worker
   build.

   A useful design split is:

   * source-style table functions, which produce rows without consuming input
     tables
   * table-processing functions, which consume one or more input relations and
     produce transformed output

3. Register the function with the native worker

   Native function registration in Presto C++ is sensitive to naming. Follow
   the ``catalog.schema.function`` pattern used by the native worker, and check
   :doc:`/presto_cpp/properties` for the ``presto.default-namespace`` setting
   when you decide how the function name should be exposed.

   If your deployment uses a Presto C++ sidecar, restart the sidecar together
   with the worker so the coordinator can observe updated native function
   metadata. See :doc:`/presto_cpp/sidecar`.

4. Validate from SQL

   Run the function through the normal SQL entry point:

   .. code-block:: sql

      SELECT *
      FROM TABLE(catalog.schema.my_table_function(...));

   Validate both the happy path and the table-function-specific edge cases:

   * named arguments and defaults
   * empty input handling
   * partitioning and ordering behavior
   * output column shape, including pass-through columns when applicable

Testing guidance
================

Prefer tests close to the layer you changed, then confirm the end-to-end SQL
behavior from Presto:

* unit tests for the native implementation
* integration tests for worker startup and registration
* query tests that exercise ``FROM TABLE(...)`` with representative inputs

Because table functions can be sensitive to partitioning and row-shape
semantics, include tests for both regular output rows and edge cases such as
empty inputs or input tables with null values.

Troubleshooting
===============

If a table function works in development but not in a running Presto C++
cluster, check the following first:

* The function name matches the native worker's expected
  ``catalog.schema.function`` naming pattern.
* The worker and any sidecar were restarted after the native code change.
* The worker build and the Velox build are compatible with each other.
* The SQL invocation still matches the general semantics documented in
  :doc:`/functions/table` and :doc:`/develop/table-functions`.

See also
========

* :doc:`/functions/table`
* :doc:`/develop/table-functions`
* :doc:`/presto_cpp/plugin/function_plugin`
* :doc:`/presto_cpp/sidecar`
