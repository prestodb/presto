===========
ALTER TABLE
===========

Synopsis
--------

.. code-block:: none

    ALTER TABLE name RENAME TO new_name
    ALTER TABLE name RENAME COLUMN column_name TO new_column_name

Description
-----------

Change the definition of an existing table.

Examples
--------

Rename table ``users`` to ``people``::

    ALTER TABLE users RENAME TO people;

Rename column ``id`` to ``user_id`` in the ``users`` table::

    ALTER TABLE users RENAME COLUMN id TO user_id;
