===========
ALTER TABLE
===========

Synopsis
--------

.. code-block:: none

    ALTER TABLE name RENAME TO new_name
    ALTER TABLE name ADD COLUMN column_name data_type
    ALTER TABLE name RENAME COLUMN column_name TO new_column_name

Description
-----------

Change the definition of an existing table.

Examples
--------

Rename table ``users`` to ``people``::

    ALTER TABLE users RENAME TO people;

Add column ``zip`` to the ``users`` table::

    ALTER TABLE users ADD COLUMN zip varchar;

Rename column ``id`` to ``user_id`` in the ``users`` table::

    ALTER TABLE users RENAME COLUMN id TO user_id;

See Also
--------

:doc:`create-table`
