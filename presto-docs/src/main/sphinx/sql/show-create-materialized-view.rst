============================
SHOW CREATE MATERIALIZED VIEW
============================

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.

    To enable, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in configuration properties.

Synopsis
--------

.. code-block:: none

    SHOW CREATE MATERIALIZED VIEW view_name

Description
-----------

Show the SQL statement that creates the specified materialized view.

Examples
--------

Show the SQL for the ``daily_sales`` materialized view::

    SHOW CREATE MATERIALIZED VIEW daily_sales

See Also
--------

:doc:`create-materialized-view`, :doc:`drop-materialized-view`,
:doc:`refresh-materialized-view`, :doc:`/admin/materialized-views`