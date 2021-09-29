***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/math
    functions/bitwise
    functions/string
    functions/datetime
    functions/array
    functions/map
    functions/regexp
    functions/binary
    functions/url
    functions/aggregate

Here is a list of all scalar and aggregate Presto functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/coverage>` and :doc:`most used
<functions/most_used_coverage>` functions for broader context.

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

    ===========================  ===========================  ===========================  ==  ===========================
    Scalar Functions                                                                           Aggregate Functions
    =====================================================================================  ==  ===========================
    :func:`abs`                  :func:`exp`                  :func:`pow`                      :func:`approx_distinct`
    :func:`acos`                 :func:`filter`               :func:`power`                    :func:`approx_percentile`
    :func:`array_constructor`    :func:`floor`                :func:`radians`                  :func:`arbitrary`
    :func:`array_distinct`       :func:`from_base64`          :func:`rand`                     :func:`array_agg`
    :func:`array_except`         :func:`from_hex`             :func:`reduce`                   :func:`avg`
    :func:`array_intersect`      :func:`from_unixtime`        :func:`regexp_extract`           :func:`bitwise_and_agg`
    :func:`array_max`            gt                           :func:`regexp_like`              :func:`bitwise_or_agg`
    :func:`array_min`            gte                          :func:`replace`                  :func:`bool_and`
    :func:`asin`                 :func:`in`                   :func:`reverse`                  :func:`bool_or`
    :func:`atan`                 :func:`is_null`              :func:`round`                    :func:`count`
    :func:`atan2`                :func:`json_extract_scalar`  :func:`sin`                      :func:`count_if`
    :func:`between`              :func:`length`               :func:`sqrt`                     :func:`map_agg`
    :func:`bitwise_and`          :func:`ln`                   :func:`strpos`                   :func:`max`
    :func:`bitwise_not`          :func:`lower`                :func:`subscript`                :func:`max_by`
    :func:`bitwise_or`           lt                           :func:`substr`                   :func:`min`
    :func:`bitwise_xor`          lte                          :func:`tan`                      :func:`min_by`
    :func:`cardinality`          :func:`map`                  :func:`tanh`                     :func:`stddev`
    :func:`cbrt`                 :func:`map_concat`           :func:`to_base64`                :func:`stddev_pop`
    :func:`ceil`                 :func:`map_entries`          :func:`to_hex`                   :func:`stddev_samp`
    :func:`ceiling`              :func:`map_filter`           :func:`to_unix_timestamp`        :func:`sum`
    :func:`chr`                  :func:`map_keys`             :func:`to_unixtime`              :func:`var_pop`
    :func:`clamp`                :func:`map_values`           :func:`to_utf8`                  :func:`var_samp`
    :func:`coalesce`             :func:`md5`                  :func:`transform`                :func:`variance`
    :func:`codepoint`            :func:`millisecond`          :func:`trim`
    :func:`concat`               minus                        :func:`upper`
    :func:`contains`             modulus                      :func:`url_decode`
    :func:`cos`                  multiply                     :func:`url_encode`
    :func:`cosh`                 negate                       :func:`width_bucket`
    divide                       neq                          :func:`xxhash64`
    :func:`element_at`           not
    eq                           plus
    ===========================  ===========================  ===========================  ==  ===========================
