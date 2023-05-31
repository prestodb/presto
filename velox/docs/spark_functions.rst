***********************
Spark Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/spark/math
    functions/spark/bitwise
    functions/spark/comparison
    functions/spark/string
    functions/spark/struct
    functions/spark/datetime
    functions/spark/array
    functions/spark/map
    functions/spark/regexp
    functions/spark/binary
    functions/spark/json
    functions/spark/aggregate
    functions/spark/window

Here is a list of all scalar and aggregate Spark functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/spark/coverage>` functions.

.. raw:: html

    <style>

    table.rows th {
        background-color: lightblue;
        border-style: solid solid solid solid;
        border-width: 1px 1px 1px 1px;
        border-color: #AAAAAA;
        text-align: center;
    }

    table.rows td {
        border-style: solid solid solid solid;
        border-width: 1px 1px 1px 1px;
        border-color: #AAAAAA;
    }

    table.rows tr {
        border-style: solid solid solid solid;
        border-width: 0px 0px 0px 0px;
        border-color: #AAAAAA;
    }

    table.rows td:nth-child(4) {
        background-color: lightblue;
    }
    </style>

.. table::
    :widths: auto
    :class: rows

    ================================  ================================  ================================  ==  ================================  ==  ================================
    Scalar Functions                                                                                          Aggregate Functions                   Window Functions
    ====================================================================================================  ==  ================================  ==  ================================
    :spark:func:`abs`                 :spark:func:`floor`               :spark:func:`rand`                    :spark:func:`bit_xor`                 :spark:func:`nth_value`         
    :spark:func:`acosh`               :spark:func:`get_json_object`     :spark:func:`regexp_extract`          :spark:func:`first`                                                   
    :spark:func:`add`                 :spark:func:`greaterthan`         :spark:func:`remainder`               :spark:func:`first_ignore_null`                                       
    :spark:func:`aggregate`           :spark:func:`greaterthanorequal`  :spark:func:`replace`                 :spark:func:`last`                                                    
    :spark:func:`array`               :spark:func:`greatest`            :spark:func:`rlike`                   :spark:func:`last_ignore_null`                                        
    :spark:func:`array_contains`      :spark:func:`hash`                :spark:func:`round`                                                                                         
    :spark:func:`array_intersect`     :spark:func:`in`                  :spark:func:`rtrim`                                                                                         
    :spark:func:`array_sort`          :spark:func:`instr`               :spark:func:`sec`                                                                                           
    :spark:func:`ascii`               :spark:func:`isnotnull`           :spark:func:`sha1`                                                                                          
    :spark:func:`asinh`               :spark:func:`isnull`              :spark:func:`sha2`                                                                                          
    :spark:func:`atanh`               :spark:func:`least`               :spark:func:`shiftleft`                                                                                     
    :spark:func:`between`             :spark:func:`length`              :spark:func:`shiftright`                                                                                    
    :spark:func:`bitwise_and`         :spark:func:`lessthan`            :spark:func:`size`                                                                                          
    :spark:func:`bitwise_or`          :spark:func:`lessthanorequal`     :spark:func:`sort_array`                                                                                    
    :spark:func:`ceil`                :spark:func:`lower`               :spark:func:`split`                                                                                         
    :spark:func:`chr`                 :spark:func:`ltrim`               :spark:func:`startswith`                                                                                    
    :spark:func:`concat`              :spark:func:`map`                 :spark:func:`substring`                                                                                     
    :spark:func:`contains`            :spark:func:`map_filter`          :spark:func:`subtract`                                                                                      
    :spark:func:`csc`                 :spark:func:`map_from_arrays`     :spark:func:`to_unix_timestamp`                                                                             
    :spark:func:`divide`              :spark:func:`md5`                 :spark:func:`transform`                                                                                     
    :spark:func:`element_at`          :spark:func:`might_contain`       :spark:func:`trim`                                                                                          
    :spark:func:`endswith`            :spark:func:`multiply`            :spark:func:`unaryminus`                                                                                    
    :spark:func:`equalnullsafe`       :spark:func:`not`                 :spark:func:`unix_timestamp`                                                                                
    :spark:func:`equalto`             :spark:func:`notequalto`          :spark:func:`upper`                                                                                         
    :spark:func:`exp`                 :spark:func:`pmod`                :spark:func:`xxhash64`                                                                                      
    :spark:func:`filter`              :spark:func:`power`               :spark:func:`year`                                                                                          
    ================================  ================================  ================================  ==  ================================  ==  ================================
