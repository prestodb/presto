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

Examples
--------

.. code-block:: none

    presto:default> explain select count(*) from airline_data;
                         Query Plan                      
    ---------------------------------------------------------
    - Output[_col0]
        _col0 := count
      - Aggregate => [count:bigint]
          count := count(*)
        - TableScan[hive:default:airline_data,
	    original constraint=true] =>
            itinid := HiveColumnHandle{clientId=hive,
                name=itinid, ordinalPosition=0, hiveType=STRING
