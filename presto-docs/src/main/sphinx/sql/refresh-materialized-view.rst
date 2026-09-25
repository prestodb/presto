=========================
REFRESH MATERIALIZED VIEW
=========================

.. warning::

    Materialized views are experimental. The SPI and behavior may change in future releases.

    To enable, set :ref:`admin/properties:\`\`experimental.legacy-materialized-views\`\`` = ``false``
    in configuration properties.

Synopsis
--------

.. code-block:: none

    REFRESH MATERIALIZED VIEW view_name

Description
-----------

Refresh the data stored in a materialized view by re-executing the view query against the base
tables.

Examples
--------

Refresh a materialized view::

    REFRESH MATERIALIZED VIEW daily_sales

.. important::

    - A newly created materialized view has an **empty storage table**.
    - Therefore, it is considered **stale immediately after creation**.
    - You must run a refresh before querying when using strict modes like ``FAIL``.

See Also
--------

:doc:`create-materialized-view`, :doc:`drop-materialized-view`,
:doc:`show-create-materialized-view`, :doc:`/admin/materialized-views`