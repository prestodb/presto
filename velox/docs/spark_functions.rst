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

    ================================  ================================  ================================  ==  ================================
    Scalar Functions                                                                                          Aggregate Functions
    ====================================================================================================  ==  ================================
    :spark:func:`abs`                 :spark:func:`get_json_object`     :spark:func:`rand`                    :spark:func:`last`
    :spark:func:`add`                 :spark:func:`greaterthan`         :spark:func:`regexp_extract`
    :spark:func:`aggregate`           :spark:func:`greaterthanorequal`  :spark:func:`remainder`
    :spark:func:`array`               :spark:func:`greatest`            :spark:func:`replace`
    :spark:func:`array_contains`      :spark:func:`hash`                :spark:func:`rlike`
    :spark:func:`array_intersect`     :spark:func:`in`                  :spark:func:`round`
    :spark:func:`array_sort`          :spark:func:`instr`               :spark:func:`sha1`
    :spark:func:`ascii`               :spark:func:`isnotnull`           :spark:func:`sha2`
    :spark:func:`between`             :spark:func:`isnull`              :spark:func:`shiftleft`
    :spark:func:`bitwise_and`         :spark:func:`least`               :spark:func:`shiftright`
    :spark:func:`bitwise_or`          :spark:func:`length`              :spark:func:`size`
    :spark:func:`ceil`                :spark:func:`lessthan`            :spark:func:`sort_array`
    :spark:func:`chr`                 :spark:func:`lessthanorequal`     :spark:func:`split`
    :spark:func:`concat`              :spark:func:`lower`               :spark:func:`startswith`
    :spark:func:`contains`            :spark:func:`map`                 :spark:func:`substring`
    :spark:func:`divide`              :spark:func:`map_filter`          :spark:func:`subtract`
    :spark:func:`element_at`          :spark:func:`map_from_arrays`     :spark:func:`transform`
    :spark:func:`endswith`            :spark:func:`md5`                 :spark:func:`unaryminus`
    :spark:func:`equalnullsafe`       :spark:func:`multiply`            :spark:func:`upper`
    :spark:func:`equalto`             :spark:func:`not`                 :spark:func:`xxhash64`
    :spark:func:`exp`                 :spark:func:`notequalto`          :spark:func:`year`
    :spark:func:`filter`              :spark:func:`pmod`
    :spark:func:`floor`               :spark:func:`power`
    ================================  ================================  ================================  ==  ================================
