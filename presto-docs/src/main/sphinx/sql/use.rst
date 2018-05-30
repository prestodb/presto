===
USE
===

Synopsis
--------

.. code-block:: none

    USE catalog.schema
    USE schema

Description
-----------

Update the session to use the specified catalog and schema. If a
catalog is not specified, the schema is resolved relative to the
current catalog.


Examples
--------

.. code-block:: sql

    USE hive.finance;
    USE information_schema;
