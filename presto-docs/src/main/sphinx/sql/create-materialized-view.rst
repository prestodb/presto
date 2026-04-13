========================
CREATE MATERIALIZED VIEW
========================

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.

    To enable, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in configuration properties.

Synopsis
--------

.. code-block:: none

    CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] view_name
    [ COMMENT 'string' ]
    [ SECURITY { DEFINER | INVOKER } ]
    [ WITH ( property_name = expression [, ...] ) ]
    AS query

Description
-----------

Create a new materialized view of a :doc:`select` query. The materialized view physically stores
the query results, unlike regular views which are virtual. Queries can read pre-computed results
instead of re-executing the underlying query.

The optional ``IF NOT EXISTS`` clause causes the materialized view to be created only if it does
not already exist.

The optional ``COMMENT`` clause stores a description of the materialized view in the metastore.

The optional ``SECURITY`` clause specifies the security mode for the materialized view. When
``legacy_materialized_views=false``:

* ``SECURITY DEFINER``: The view executes with the permissions of the user who created it. This is the default mode if ``SECURITY`` is not specified and matches the behavior of most SQL systems. The view owner must have ``CREATE_VIEW_WITH_SELECT_COLUMNS`` permission on base tables for non-owners to query the view.
* ``SECURITY INVOKER``: The view executes with the permissions of the user querying it. Each user must have appropriate permissions on the underlying base tables.

When ``legacy_materialized_views=true``, the ``SECURITY`` clause is not supported and will
cause an error if used.

The optional ``WITH`` clause specifies connector-specific properties. Connector properties vary by
connector implementation. Consult connector documentation for supported properties.

Examples
--------

Create a materialized view with daily aggregations::

    CREATE MATERIALIZED VIEW daily_sales AS
    SELECT date_trunc('day', order_date) AS day,
           region,
           SUM(amount) AS total_sales,
           COUNT(*) AS order_count
    FROM orders
    GROUP BY date_trunc('day', order_date), region

Create a materialized view with DEFINER security mode::

    CREATE MATERIALIZED VIEW daily_sales
    SECURITY DEFINER
    AS
    SELECT date_trunc('day', order_date) AS day,
           region,
           SUM(amount) AS total_sales
    FROM orders
    GROUP BY date_trunc('day', order_date), region

Create a materialized view with INVOKER security mode::

    CREATE MATERIALIZED VIEW user_specific_sales
    SECURITY INVOKER
    AS
    SELECT date_trunc('day', order_date) AS day,
           SUM(amount) AS total_sales
    FROM orders
    GROUP BY date_trunc('day', order_date)

Create a materialized view with staleness configuration::

    CREATE MATERIALIZED VIEW daily_sales
    WITH (stale_read_behavior = 'FAIL',
    staleness_window = '1h')
    AS
    SELECT date_trunc('day', order_date) AS day,
           region,
           SUM(amount) AS total_sales
    FROM orders
    GROUP BY date_trunc('day', order_date), region


The optional ``staleness_window`` parameter defines how long stale data is acceptable and allows duration values in hours, minutes, or seconds (for example: ``1h``, ``30m``).

Timestamp-Based Staleness Detection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create materialized views from non-Iceberg tables, where snapshot comparison is not feasible, you can enable timestamp-based staleness detection using the ``use_timestamp_based_staleness`` property.

* The ``use_timestamp_based_staleness`` property accepts a boolean value: ``true`` or ``false``
* This mode is automatically enabled when ``cross_catalog_materialized_views_enabled`` is set to ``true``

**Limitations**

* Timestamp-based staleness does not track individual base table modifications, only the time since last refresh

Create a materialized view with timestamp-based staleness detection::

    CREATE MATERIALIZED VIEW daily_sales_timestamp
    WITH (
        use_timestamp_based_staleness = true,
        staleness_window = '2h'
    )
    AS
    SELECT date_trunc('day', order_date) AS day,
           region,
           SUM(amount) AS total_sales
    FROM orders
    GROUP BY date_trunc('day', order_date), region

Create a cross-catalog materialized view with timestamp-based staleness::

    CREATE MATERIALIZED VIEW iceberg.analytics.cross_catalog_sales
    WITH (
        cross_catalog_materialized_views_enabled = true,
        staleness_window = '1h'
    )
    AS
    SELECT date_trunc('day', order_date) AS day,
           SUM(amount) AS total_sales
    FROM mysql.sales.orders
    GROUP BY date_trunc('day', order_date)

Create a materialized view with connector properties::

    CREATE MATERIALIZED VIEW partitioned_sales
    WITH (
        partitioned_by = ARRAY['year', 'month']
    )
    AS
    SELECT year(order_date) AS year,
           month(order_date) AS month,
           SUM(amount) AS total_sales
    FROM orders
    GROUP BY year(order_date), month(order_date)

See Also
--------

:doc:`drop-materialized-view`, :doc:`refresh-materialized-view`,
:doc:`show-create-materialized-view`, :doc:`/admin/materialized-views`