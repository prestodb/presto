=====================
DROP MATERIALIZED VIEW
=====================

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.

    To enable, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in configuration properties.

Synopsis
--------

.. code-block:: none

    DROP MATERIALIZED VIEW [ IF EXISTS ] view_name

Description
-----------

Drop an existing materialized view and delete its stored data.

The optional ``IF EXISTS`` clause causes the statement to succeed even if the materialized view
does not exist.

Examples
--------

Drop the materialized view ``daily_sales``::

    DROP MATERIALIZED VIEW daily_sales

Drop the materialized view if it exists::

    DROP MATERIALIZED VIEW IF EXISTS daily_sales

See Also
--------

:doc:`create-materialized-view`, :doc:`refresh-materialized-view`,
:doc:`show-create-materialized-view`, :doc:`/admin/materialized-views`