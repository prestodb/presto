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