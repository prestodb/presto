===========
ALTER TABLE
===========

Synopsis
--------

.. code-block:: none

    ALTER TABLE [ IF EXISTS ] name RENAME TO new_name
    ALTER TABLE [ IF EXISTS ] name ADD COLUMN [ IF NOT EXISTS ] column_name data_type [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
    ALTER TABLE [ IF EXISTS ] name DROP COLUMN column_name
    ALTER TABLE [ IF EXISTS ] name RENAME COLUMN [ IF EXISTS ] column_name TO new_column_name
    ALTER TABLE [ IF EXISTS ] name ADD [ CONSTRAINT constraint_name ] { PRIMARY KEY | UNIQUE } ( { column_name [, ...] } ) [ { ENABLED | DISABLED } ] [ [ NOT ] RELY ] [ [ NOT ] ENFORCED } ]
    ALTER TABLE [ IF EXISTS ] name DROP CONSTRAINT [ IF EXISTS ] constraint_name
    ALTER TABLE [ IF EXISTS ] name ALTER [ COLUMN ] column_name { SET | DROP } NOT NULL
    ALTER TABLE [ IF EXISTS ] name SET PROPERTIES (property_name=value, [, ...])
    ALTER TABLE [ IF EXISTS ] name DROP BRANCH [ IF EXISTS ] branch_name
    ALTER TABLE [ IF EXISTS ] name DROP TAG [ IF EXISTS ] tag_name

Description
-----------

Change the definition of an existing table.

The optional ``IF EXISTS`` (when used before the table name) clause causes the error to be suppressed if the table does not exists.

The optional ``IF EXISTS`` (when used before the column name) clause causes the error to be suppressed if the column does not exists.

The optional ``IF NOT EXISTS`` clause causes the error to be suppressed if the column already exists.

Examples
--------

Rename table ``users`` to ``people``::

    ALTER TABLE users RENAME TO people;

Rename table ``users`` to ``people`` if table ``users`` exists::

    ALTER TABLE IF EXISTS users RENAME TO people;

Add column ``zip`` to the ``users`` table::

    ALTER TABLE users ADD COLUMN zip varchar;

Add column ``zip`` to the ``users`` table if table ``users`` exists and column ``zip`` not already exists::

    ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip varchar;

Drop column ``zip`` from the ``users`` table::

    ALTER TABLE users DROP COLUMN zip;

Drop column ``zip`` from the ``users`` table if table ``users`` and column ``zip`` exists::

    ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip;

Rename column ``id`` to ``user_id`` in the ``users`` table::

    ALTER TABLE users RENAME COLUMN id TO user_id;

Rename column ``id`` to ``user_id`` in the ``users`` table if table ``users`` and column ``id`` exists::

    ALTER TABLE IF EXISTS users RENAME column IF EXISTS id to user_id;

Add constraint ``pk`` on column ``user_id`` in the ``users`` table::

    ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY (user_id);

Add unnamed, disabled unique constraint on columns ``first_name`` and ``last_name`` in the ``users`` table if table ``users`` exists::

    ALTER TABLE IF EXISTS users ADD UNIQUE (first_name, last_name) DISABLED;

Drop constraint ``pk`` from the ``users`` table::

    ALTER TABLE users DROP CONSTRAINT pk;

Drop constraint ``pk`` from the ``users`` table if table ``users`` exists and constraint ``pk`` exists::

    ALTER TABLE IF EXISTS users DROP CONSTRAINT IF EXISTS pk;

Add not null constraint to column ``zip`` in the ``users`` table::

    ALTER TABLE users ALTER COLUMN zip SET NOT NULL;

Add not null constraint to column ``zip`` in the ``users`` table if  table ``users`` exists::

    ALTER TABLE IF EXISTS users ALTER zip SET NOT NULL;

Drop not null constraint from column ``zip`` in the ``users`` table::

    ALTER TABLE users ALTER COLUMN zip DROP NOT NULL;

Set table property (``x=y``) to table ``users``::

    ALTER TABLE users SET PROPERTIES (x='y');

Drop branch ``branch1`` from the ``users`` table::

    ALTER TABLE users DROP BRANCH 'branch1';

Drop tag ``tag1`` from the ``users`` table::

    ALTER TABLE users DROP TAG 'tag1';

See Also
--------

:doc:`create-table`
