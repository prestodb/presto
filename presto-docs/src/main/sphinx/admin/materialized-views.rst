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
  * For DEFINER mode: User needs ``SELECT`` permission on the view itself
  * For INVOKER mode: User needs ``SELECT`` permission on all underlying base tables

See Also
--------

:doc:`/sql/create-materialized-view`, :doc:`/sql/drop-materialized-view`,
:doc:`/sql/refresh-materialized-view`, :doc:`/sql/show-create-materialized-view`