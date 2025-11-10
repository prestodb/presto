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
    in your configuration properties, or use ``SET SESSION legacy_materialized_views = false``.

See Also
--------

:doc:`/sql/create-materialized-view`, :doc:`/sql/drop-materialized-view`,
:doc:`/sql/refresh-materialized-view`, :doc:`/sql/show-create-materialized-view`