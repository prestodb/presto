=================
START TRANSACTION
=================

Synopsis
--------

.. code-block:: none

    START TRANSACTION [ mode [, ...] ]

where ``mode`` is one of

.. code-block:: none

    ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
    READ { ONLY | WRITE }

Description
-----------

Start a new transaction for the current session.

Examples
--------

.. code-block:: sql

    START TRANSACTION;
    START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION READ WRITE;
    START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;
    START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE;

See Also
--------

:doc:`commit`, :doc:`rollback`
