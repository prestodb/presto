===========
DROP SCHEMA
===========

Synopsis
--------

.. code-block:: none

    DROP SCHEMA [ IF EXISTS ] schema_name

Description
-----------

Drop an existing schema. The schema must be empty.

The optional ``IF EXISTS`` clause causes the error to be suppressed if
the schema does not exist.

Examples
--------

Drop the schema ``web``::

    DROP SCHEMA web

Drop the schema ``sales`` if it exists::

    DROP TABLE IF EXISTS sales

See Also
--------

:doc:`alter-schema`, :doc:`create-schema`
