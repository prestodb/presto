============
ALTER SCHEMA
============

Synopsis
--------

.. code-block:: none

    ALTER SCHEMA name RENAME TO new_name

Description
-----------

Change the definition of an existing schema.

Examples
--------

Rename schema ``web`` to ``traffic``::

    ALTER TABLE web RENAME TO traffic

See Also
--------

:doc:`create-schema`
