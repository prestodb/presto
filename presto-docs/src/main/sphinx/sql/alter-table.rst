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
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name FOR SYSTEM_VERSION AS OF version
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name FOR SYSTEM_TIME AS OF timestamp
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name FOR SYSTEM_VERSION AS OF version RETAIN retention_period
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] BRANCH [ IF NOT EXISTS ] branch_name FOR SYSTEM_VERSION AS OF version RETAIN retention_period WITH SNAPSHOT RETENTION min_snapshots SNAPSHOTS min_retention_period
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] tag_name
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] tag_name FOR SYSTEM_VERSION AS OF version
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] tag_name FOR SYSTEM_TIME AS OF timestamp
    ALTER TABLE [ IF EXISTS ] name CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] tag_name FOR SYSTEM_VERSION AS OF version RETAIN retention_period

Description
-----------

Change the definition of an existing table.

The optional ``IF EXISTS`` (when used before the table name) clause causes the error to be suppressed if the table does not exists.

The optional ``IF EXISTS`` (when used before the column name) clause causes the error to be suppressed if the column does not exists.

The optional ``IF NOT EXISTS`` clause causes the error to be suppressed if the column already exists.

For ``CREATE BRANCH`` statements:

* The optional ``OR REPLACE`` clause causes the branch to be replaced if it already exists.
* The optional ``IF NOT EXISTS`` clause causes the error to be suppressed if the branch already exists.
* ``OR REPLACE`` and ``IF NOT EXISTS`` cannot be specified together.

For ``CREATE TAG`` statements:

* The optional ``OR REPLACE`` clause causes the tag to be replaced if it already exists.
* The optional ``IF NOT EXISTS`` clause causes the error to be suppressed if the tag already exists.
* ``OR REPLACE`` and ``IF NOT EXISTS`` cannot be specified together.

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

Create branch ``branch1`` from the ``users`` table::

    ALTER TABLE users CREATE BRANCH 'branch1';

Create branch ``branch1`` from the ``users`` table only if it doesn't already exist::

    ALTER TABLE users CREATE BRANCH IF NOT EXISTS 'branch1';

Create or replace branch ``branch1`` from the ``users`` table::

    ALTER TABLE users CREATE OR REPLACE BRANCH 'branch1';

Create branch ``branch1`` from the ``users`` table for system version as of version 5::

    ALTER TABLE users CREATE BRANCH 'branch1' FOR SYSTEM_VERSION AS OF 5;

Create branch ``branch1`` from the ``users`` table for system version as of version 5, only if it doesn't exist::

    ALTER TABLE users CREATE BRANCH IF NOT EXISTS 'branch1' FOR SYSTEM_VERSION AS OF 5;

Create or replace branch ``branch1`` from the ``users`` table for system time as of timestamp '2026-01-02 17:30:35.247 Asia/Kolkata'::

    ALTER TABLE users CREATE OR REPLACE BRANCH 'branch1' FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-02 17:30:35.247 Asia/Kolkata';

Create branch ``branch1`` from the ``users`` table for system version as of version 5 with retention period of 30 days::

    ALTER TABLE users CREATE BRANCH 'branch1' FOR SYSTEM_VERSION AS OF 5 RETAIN INTERVAL 30 DAY;

Create branch ``branch1`` from the ``users`` table for system version as of version 5 with snapshot retention of minimum 3 snapshots and minimum retention period of 7 days::

    ALTER TABLE users CREATE BRANCH 'branch1' FOR SYSTEM_VERSION AS OF 5 RETAIN INTERVAL 7 DAY WITH SNAPSHOT RETENTION 3 SNAPSHOTS INTERVAL 7 DAYS;

Create tag ``tag1`` from the ``users`` table::

    ALTER TABLE users CREATE TAG 'tag1';

Create tag ``tag1`` from the ``users`` table only if it doesn't already exist::

    ALTER TABLE users CREATE TAG IF NOT EXISTS 'tag1';

Create or replace tag ``tag1`` from the ``users`` table::

    ALTER TABLE users CREATE OR REPLACE TAG 'tag1';

Create tag ``tag1`` from the ``users`` table for system version as of version 5::

    ALTER TABLE users CREATE TAG 'tag1' FOR SYSTEM_VERSION AS OF 5;

Create tag ``tag1`` from the ``users`` table for system version as of version 5, only if it doesn't exist::

    ALTER TABLE users CREATE TAG IF NOT EXISTS 'tag1' FOR SYSTEM_VERSION AS OF 5;

Create or replace tag ``tag1`` from the ``users`` table for system time as of timestamp '2026-01-02 17:30:35.247 Asia/Kolkata'::

    ALTER TABLE users CREATE OR REPLACE TAG 'tag1' FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-02 17:30:35.247 Asia/Kolkata';

Create tag ``tag1`` from the ``users`` table for system version as of version 5 with retention period of 30 days::

    ALTER TABLE users CREATE TAG 'tag1' FOR SYSTEM_VERSION AS OF 5 RETAIN INTERVAL 30 DAY;

See Also
--------

:doc:`create-table`
