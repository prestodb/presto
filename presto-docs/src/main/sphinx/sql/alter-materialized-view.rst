=======================
ALTER MATERIALIZED VIEW
=======================

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.

    To enable, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in configuration properties. ``ALTER MATERIALIZED VIEW ... SET PROPERTIES`` is not supported
    when ``legacy_materialized_views=true``.

Synopsis
--------

.. code-block:: none

    ALTER MATERIALIZED VIEW [ IF EXISTS ] name SET PROPERTIES (property_name = value [, ...])

Description
-----------

Change properties on an existing materialized view. The set of properties that can be changed
is connector-specific; some properties (typically those that determine the physical identity of
the storage) are fixed at creation time. See :doc:`/connector` for the
authoritative list of updatable properties.

The optional ``IF EXISTS`` clause causes the error to be suppressed if the materialized view
does not exist.

Setting a property to ``NULL`` is not supported. To change a property, the new value must be
non-null. Properties not mentioned in the ``SET PROPERTIES`` clause are left unchanged.

Examples
--------

Change a property on a materialized view::

    ALTER MATERIALIZED VIEW users SET PROPERTIES (x = 'y');

Use ``IF EXISTS`` to suppress an error when the view may not exist::

    ALTER MATERIALIZED VIEW IF EXISTS users SET PROPERTIES (x = 'y');

For connector-specific examples, see :doc:`/connector`.

See Also
--------

:doc:`create-materialized-view`, :doc:`drop-materialized-view`,
:doc:`refresh-materialized-view`, :doc:`show-create-materialized-view`,
:doc:`/admin/materialized-views`
