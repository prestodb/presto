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

.. code-block:: none
    
    presto:default> explain (type distributed) select n_regionkey, count(*) from nation group by n_regionkey;
                         Query Plan                      
    ---------------------------------------------------------
    - Output[n_regionkey, _col1]
        n_regionkey := n_2
        _col1 := count
    - Exchange[[1]] => [n_2:bigint, count:bigint]
        - Sink[12] => [n_2:bigint, count:bigint]
            - Aggregate(FINAL)[n_2] => [n_2:bigint, count:bigint]
                    count := "count"("count_3")
                - Exchange[[0]] => [n_2:bigint, count_3:bigint]
                    - Sink[9] => [n_2:bigint, count_3:bigint]
                        - Aggregate(PARTIAL)[n_2] => [n_2:bigint, count_3:bigint]
                                count_3 := "count"(*)
                            - TableScan[hive:hive:default:nation, original constraint=true] => [n_2:bigint]
                                    n_2 := hive:HiveColumnHandle{clientId=hive, name=n_regionkey, ordinalPosition=2, hiveType=INT, hiveColumnIndex=2, partitionKey=false}
