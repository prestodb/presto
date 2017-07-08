=============
COMMENT TABLE
=============

Synopsis
--------

.. code-block:: none

    COMMENT ON TABLE name IS 'comments'

Description
-----------

Add a comment to an existing table.

Examples
--------

Add the comment ``master table`` to the ``users`` table::

    COMMENT ON TABLE users IS 'master table';

