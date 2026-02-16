==================
Materialized Views
==================

Introduction
------------

A materialized view stores the results of a query physically, unlike regular views which are virtual.
Queries can read pre-computed results instead of re-executing the underlying query against base tables.

Materialized views improve performance for expensive queries by calculating results once during
refresh rather than on every query. Common use cases include aggregations, joins, and filtered
subsets of large tables.

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.
    Use at your own risk in production environments.

    To enable materialized views, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in your configuration properties.

    Alternatively, you can use ``SET SESSION legacy_materialized_views = false`` to enable them for a session,
    but only if :ref:`admin/properties:\`\`experimental.allow-legacy-materialized-views-toggle\`\`` = ``true``
    is set in the server configuration. The toggle option should only be used in non-production environments
    for testing and migration purposes.

Security Modes
--------------

When ``legacy_materialized_views=false``, materialized views support SQL standard security modes
that control whose permissions are used when querying the view:

**SECURITY DEFINER**
  The materialized view executes with the permissions of the user who created it. This is the
  default mode and matches the behavior of most SQL systems. When using DEFINER mode, column
  masks and row filters on base tables are permitted.

**SECURITY INVOKER**
  The materialized view executes with the permissions of the user querying it. Each user must
  have appropriate permissions on the underlying base tables. When using INVOKER mode and there
  are column masks or row filters on the base tables, the materialized view is treated as stale,
  since the data would vary by user.

The default security mode can be configured using the ``default_view_security_mode`` session
property. When the ``SECURITY`` clause is not specified in ``CREATE MATERIALIZED VIEW``, this
default is used.

.. note::

    The ``REFRESH`` operation always uses DEFINER rights regardless of the view's security mode.

Stale Data Handling
-------------------

Connectors report the freshness state of materialized views to the engine. When a materialized
view is stale (base tables have been modified since the data was last known to be fresh), the
engine determines how to handle the query based on configuration.

Connectors can configure staleness handling per materialized view, including a behavior setting
and staleness tolerance window. See connector-specific documentation for details (for example,
:ref:`Iceberg <iceberg-stale-data-handling>`).

When no per-view configuration is specified, the default behavior is ``USE_VIEW_QUERY`` (Presto
falls back to executing the underlying view query against the base tables). This can be changed
using the ``materialized_view_stale_read_behavior`` session property or the
``materialized-view-stale-read-behavior`` configuration property. Setting it to ``FAIL`` causes
the query to fail with an error when the materialized view is stale.

Required Permissions
--------------------

The following permissions are required for materialized view operations when
``legacy_materialized_views=false``:

**CREATE MATERIALIZED VIEW**
  * ``CREATE TABLE`` permission
  * ``CREATE VIEW`` permission

**REFRESH MATERIALIZED VIEW**
  * ``INSERT`` permission (to write new data)
  * ``DELETE`` permission (to remove old data)

**DROP MATERIALIZED VIEW**
  * ``DROP TABLE`` permission
  * ``DROP VIEW`` permission

**Querying a materialized view**
  * For DEFINER mode: User needs ``SELECT`` permission on the view itself. Additionally, the
    view owner must have ``CREATE_VIEW_WITH_SELECT_COLUMNS`` permission on base tables when
    non-owners query the view to prevent privilege escalation.
  * For INVOKER mode: User needs ``SELECT`` permission on all underlying base tables

Data Consistency Modes
----------------------

Materialized views support three data consistency modes that control how queries are optimized
when the view's data may be stale:

**USE_STITCHING** (default)
  Reads fresh data from storage, recomputes stale data from base tables,
  and combines results via UNION.

**FAIL**
  Fails the query if the materialized view is stale.

**USE_VIEW_QUERY**
  Executes the view query against base tables. Always fresh but highest cost.

Set via session property::

    SET SESSION materialized_view_skip_storage = 'USE_STITCHING';

Predicate Stitching (USE_STITCHING Mode)
----------------------------------------

Overview
^^^^^^^^

Predicate stitching recomputes only stale data rather than the entire view. When base
tables change, Presto identifies which data is affected and generates a UNION query
that combines:

* **Storage scan**: Reads unchanged (fresh) data from the materialized view's storage
* **Recompute branch**: Recomputes changed (stale) data from base tables using the view's
  defining query

This avoids full recomputation when only a subset of data is stale, though there is
overhead from the UNION operation and predicate-based filtering.

How It Works
^^^^^^^^^^^^

**Staleness Detection**

For each base table referenced in the materialized view, a connector may track which data
has changed since the last refresh and return predicates identifying the stale data. The
specific mechanism depends on the connector:

1. At refresh time, metadata is recorded (implementation varies by connector)
2. When the view is queried, the current state is compared with the recorded state
3. Predicates are built that identify exactly which data is stale

See the connector-specific documentation for details on how staleness is tracked.
For Iceberg tables, see :ref:`connector/iceberg:materialized views`.

**Query Rewriting**

When a query uses a materialized view with stale data, the optimizer rewrites the query
to use UNION::

    -- Original query
    SELECT * FROM my_materialized_view WHERE order_date >= '2024-01-01'

    -- Rewritten with predicate stitching (example using partition predicates)
    SELECT * FROM (
        -- Fresh partitions from storage
        SELECT * FROM my_materialized_view_storage
        WHERE order_date >= '2024-01-01'
          AND order_date NOT IN ('2024-01-15', '2024-01-16')  -- Exclude stale
    UNION ALL
        -- Stale partitions recomputed
        SELECT o.order_id, c.customer_name, o.order_date
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
                        AND o.order_date = c.reg_date
        WHERE o.order_date IN ('2024-01-15', '2024-01-16')  -- Stale partition filter
          AND c.reg_date IN ('2024-01-15', '2024-01-16')    -- Propagated via equivalence
          AND o.order_date >= '2024-01-01'  -- Original filter preserved
    )

The partition predicate is propagated to equivalent columns in joined tables (in this case,
``c.reg_date``), allowing partition pruning on the ``customers`` table as well.

Requirements
^^^^^^^^^^^^

For predicate stitching to work effectively, the following requirements must be met:

**Predicate Mapping Requirement**

The connector must be able to express staleness as predicates that can be mapped to the
materialized view's columns. The specific requirements depend on the connector implementation.
For partition-based connectors (like Iceberg), this typically means:

* Base table partition columns must appear in the SELECT list or be equivalent to columns that do
* The materialized view should be partitioned on the same or equivalent columns
* Partition columns must use compatible data types

See connector-specific documentation for details on staleness tracking requirements.

**Unsupported Query Patterns**

Predicate stitching does not work with:

* **Outer joins**: LEFT, RIGHT, and FULL OUTER joins
* **Non-deterministic functions**: ``RANDOM()``, ``NOW()``, ``UUID()``, etc.

**Security Constraints**

For SECURITY INVOKER materialized views, predicate stitching requires that:

* No column masks are defined on base tables (or the view is treated as fully stale)
* No row filters are defined on base tables (or the view is treated as fully stale)

This is because column masks and row filters can vary by user, making it impossible to
determine staleness in a user-independent way.

Column Equivalences and Passthrough Columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Predicate stitching supports **passthrough columns** through **column equivalences**,
which allows tracking staleness even when predicate columns from base tables
are not directly in the materialized view's output.

**Column Equivalence**

When tables are joined with equality predicates, those columns become equivalent for
predicate propagation purposes. This applies to any type of staleness predicate
(partition-based, snapshot-based, etc.). For example with partition predicates::

    CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, order_date VARCHAR)
      WITH (partitioning = ARRAY['order_date']);

    CREATE TABLE customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR)
      WITH (partitioning = ARRAY['reg_date']);

    -- Materialized view with equivalence: order_date = reg_date
    CREATE MATERIALIZED VIEW order_summary
    WITH (partitioning = ARRAY['order_date'])
    AS
      SELECT o.order_id, c.name, o.order_date
      FROM orders o
      JOIN customers c ON o.customer_id = c.customer_id
                      AND o.order_date = c.reg_date;  -- Creates equivalence

In this example:

* ``orders.order_date`` and ``customers.reg_date`` are equivalent due to the equality join condition
* Even though ``reg_date`` is not in the SELECT list, staleness can be tracked through the equivalence to ``order_date``
* When ``customers`` table changes in partition ``reg_date='2024-01-15'``, this maps to ``order_date='2024-01-15'`` for recomputation

**How Passthrough Mapping Works**

1. **Equivalence Extraction**: During materialized view creation, Presto analyzes JOIN conditions to identify
   column equivalences

2. **Staleness Detection**: When a base table changes:

   * The connector detects which data changed in the base table and returns predicates
   * For passthrough columns, predicates are mapped through equivalences
   * Example: ``customers.reg_date='2024-01-15'`` → ``orders.order_date='2024-01-15'``

3. **Predicate Application**: The mapped predicates are used in:

   * Storage scan: Exclude data where equivalent columns match stale values
   * Recompute branch: Filter the stale table using the staleness predicate
   * Joined tables: Propagate the predicate to equivalent columns in joined
     tables, enabling pruning on those tables as well

**Requirements for Passthrough Columns**

* Join must be an INNER JOIN (not LEFT, RIGHT, or FULL OUTER)
* Equality must be direct (``col1 = col2``), not through expressions like ``col1 = col2 + 1``
* At least one column in the equivalence class must be in the materialized view's output
* Data types must be compatible

**Transitive Equivalences**

Multiple equivalences can be chained together. If ``A.x = B.y`` and ``B.y = C.z``, then
``A.x``, ``B.y``, and ``C.z`` are all equivalent for predicate propagation.

Unsupported Patterns
^^^^^^^^^^^^^^^^^^^^

Predicate stitching is **not** applied in the following cases:

* **No staleness predicates available**: If the connector cannot provide staleness predicates
* **Predicate columns not preserved**: If predicate columns are transformed or not mappable to the materialized view's output
* **Outer joins with passthrough**: LEFT, RIGHT, and FULL OUTER joins invalidate passthrough equivalences due to null handling
* **Expression-based equivalences**: ``CAST(col1 AS DATE) = col2`` or ``col1 = col2 + 1``

When predicate stitching cannot be applied, the behavior falls back to the configured consistency mode:

* If ``USE_STITCHING`` is set but stitching is not possible, the query falls back to full
  recompute (equivalent to ``USE_VIEW_QUERY``)
* A warning may be logged indicating why stitching was not possible

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**When Stitching is Most Effective**

* **Large materialized views**: More benefit from avoiding full recomputation
* **Localized changes**: When only a small fraction of data is stale
* **Frequently refreshed**: When most data remains fresh between queries
* **Well-structured data**: When staleness predicates align with data modification patterns

**Cost Trade-offs**

Predicate stitching introduces a UNION operation, which has overhead:

* **Storage scan overhead**: Reading from storage + filtering fresh data
* **Recompute overhead**: Querying base tables + filtering stale data
* **Union overhead**: Combining results from both branches

However, this is typically much cheaper than:

* **Full recompute**: Reading all base table data
* **Stale data**: Returning incorrect results

**Optimization Tips**

1. **Predicate granularity**: For partition-based connectors, choose partition columns that align
   with data modification patterns

   * Too coarse (e.g., partitioning by year): Recomputes too much data
   * Too fine (e.g., partitioning by second): Too many partitions to manage

2. **Refresh frequency**: Balance freshness needs with refresh costs

   * More frequent refreshes: Less recomputation per query, but higher refresh costs
   * Less frequent refreshes: More recomputation per query, but lower refresh costs

3. **Query filters**: Include predicate columns in query filters when possible::

       -- Good: Limits scan to relevant data
       SELECT * FROM mv WHERE order_date >= '2024-01-01'

       -- Less optimal: Scans all data
       SELECT * FROM mv WHERE customer_id = 12345

4. **Monitor metrics**: Track the ratio of storage scan vs recompute:

   * High recompute ratio: Consider more frequent refreshes or better staleness granularity
   * High storage scan ratio: Stitching is working efficiently

See Also
--------

:doc:`/sql/create-materialized-view`, :doc:`/sql/drop-materialized-view`,
:doc:`/sql/refresh-materialized-view`, :doc:`/sql/show-create-materialized-view`