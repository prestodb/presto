***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/math
    functions/bitwise
    functions/comparison
    functions/string
    functions/datetime
    functions/array
    functions/map
    functions/regexp
    functions/binary
    functions/url
    functions/aggregate
    functions/hyperloglog

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

    ======================================  ======================================  ======================================  ==  ======================================
    Scalar Functions                                                                                                            Aggregate Functions
    ======================================================================================================================  ==  ======================================
    :func:`abs`                             :func:`dow`                             negate                                      :func:`approx_distinct`
    :func:`acos`                            :func:`doy`                             neq                                         :func:`approx_percentile`
    :func:`array_constructor`               :func:`element_at`                      not                                         :func:`arbitrary`
    :func:`array_distinct`                  eq                                      plus                                        :func:`array_agg`
    :func:`array_except`                    :func:`exp`                             :func:`pow`                                 :func:`avg`
    :func:`array_intersect`                 :func:`filter`                          :func:`power`                               :func:`bitwise_and_agg`
    :func:`array_max`                       :func:`floor`                           :func:`radians`                             :func:`bitwise_or_agg`
    :func:`array_min`                       :func:`from_base64`                     :func:`rand`                                :func:`bool_and`
    :func:`asin`                            :func:`from_hex`                        :func:`reduce`                              :func:`bool_or`
    :func:`atan`                            :func:`from_unixtime`                   :func:`regexp_extract`                      :func:`count`
    :func:`atan2`                           :func:`greatest`                        :func:`regexp_extract_all`                  :func:`count_if`
    :func:`between`                         gt                                      :func:`regexp_like`                         :func:`map_agg`
    :func:`bitwise_and`                     gte                                     :func:`replace`                             :func:`max`
    :func:`bitwise_arithmetic_shift_right`  :func:`hour`                            :func:`reverse`                             :func:`max_by`
    :func:`bitwise_left_shift`              :func:`in`                              :func:`round`                               :func:`min`
    :func:`bitwise_logical_shift_right`     :func:`is_null`                         :func:`rtrim`                               :func:`min_by`
    :func:`bitwise_not`                     :func:`json_extract_scalar`             :func:`second`                              :func:`stddev`
    :func:`bitwise_or`                      :func:`least`                           :func:`sign`                                :func:`stddev_pop`
    :func:`bitwise_right_shift`             :func:`length`                          :func:`sin`                                 :func:`stddev_samp`
    :func:`bitwise_right_shift_arithmetic`  :func:`like`                            :func:`split`                               :func:`sum`
    :func:`bitwise_shift_left`              :func:`ln`                              :func:`sqrt`                                :func:`var_pop`
    :func:`bitwise_xor`                     :func:`log10`                           :func:`strpos`                              :func:`var_samp`
    :func:`cardinality`                     :func:`log2`                            :func:`subscript`                           :func:`variance`
    :func:`cbrt`                            :func:`lower`                           :func:`substr`
    :func:`ceil`                            lt                                      :func:`tan`
    :func:`ceiling`                         lte                                     :func:`tanh`
    :func:`chr`                             :func:`ltrim`                           :func:`to_base64`
    :func:`clamp`                           :func:`map`                             :func:`to_hex`
    :func:`coalesce`                        :func:`map_concat`                      :func:`to_unix_timestamp`
    :func:`codepoint`                       :func:`map_entries`                     :func:`to_unixtime`
    :func:`concat`                          :func:`map_filter`                      :func:`to_utf8`
    :func:`contains`                        :func:`map_keys`                        :func:`transform`
    :func:`cos`                             :func:`map_values`                      :func:`trim`
    :func:`cosh`                            :func:`md5`                             :func:`upper`
    :func:`date_trunc`                      :func:`millisecond`                     :func:`url_decode`
    :func:`day`                             minus                                   :func:`url_encode`
    :func:`day_of_month`                    :func:`minute`                          :func:`width_bucket`
    :func:`day_of_week`                     modulus                                 :func:`xxhash64`
    :func:`day_of_year`                     :func:`month`                           :func:`year`
    divide                                  multiply
    ======================================  ======================================  ======================================  ==  ======================================