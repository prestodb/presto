=======
EXPLAIN
=======

Synopsis
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

    where option can be one of:

        FORMAT { TEXT | GRAPHVIZ }
        TYPE { LOGICAL | DISTRIBUTED }

Description
-----------

Show the logical or distributed execution plan of a statement.
