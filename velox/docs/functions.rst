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
    :func:`abs`                             :func:`floor`                           :func:`radians`                             :func:`approx_distinct`
    :func:`acos`                            :func:`from_base64`                     :func:`rand`                                :func:`approx_percentile`
    :func:`array_constructor`               :func:`from_hex`                        :func:`reduce`                              :func:`approx_set`
    :func:`array_distinct`                  :func:`from_unixtime`                   :func:`regexp_extract`                      :func:`arbitrary`
    :func:`array_duplicates`                :func:`greatest`                        :func:`regexp_extract_all`                  :func:`array_agg`
    :func:`array_except`                    gt                                      :func:`regexp_like`                         :func:`avg`
    :func:`array_intersect`                 gte                                     :func:`replace`                             :func:`bitwise_and_agg`
    :func:`array_max`                       :func:`hour`                            :func:`reverse`                             :func:`bitwise_or_agg`
    :func:`array_min`                       :func:`in`                              :func:`round`                               :func:`bool_and`
    :func:`asin`                            :func:`infinity`                        :func:`rpad`                                :func:`bool_or`
    :func:`atan`                            :func:`is_finite`                       :func:`rtrim`                               :func:`count`
    :func:`atan2`                           :func:`is_infinite`                     :func:`second`                              :func:`count_if`
    :func:`between`                         :func:`is_nan`                          :func:`sign`                                :func:`map_agg`
    :func:`bitwise_and`                     :func:`is_null`                         :func:`sin`                                 :func:`max`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_extract_scalar`             :func:`slice`                               :func:`max_by`
    :func:`bitwise_left_shift`              :func:`least`                           :func:`split`                               :func:`merge`
    :func:`bitwise_logical_shift_right`     :func:`length`                          :func:`split_part`                          :func:`min`
    :func:`bitwise_not`                     :func:`like`                            :func:`sqrt`                                :func:`min_by`
    :func:`bitwise_or`                      :func:`ln`                              :func:`strpos`                              :func:`stddev`
    :func:`bitwise_right_shift`             :func:`log10`                           :func:`subscript`                           :func:`stddev_pop`
    :func:`bitwise_right_shift_arithmetic`  :func:`log2`                            :func:`substr`                              :func:`stddev_samp`
    :func:`bitwise_shift_left`              :func:`lower`                           :func:`tan`                                 :func:`sum`
    :func:`bitwise_xor`                     :func:`lpad`                            :func:`tanh`                                :func:`var_pop`
    :func:`cardinality`                     lt                                      :func:`to_base64`                           :func:`var_samp`
    :func:`cbrt`                            lte                                     :func:`to_hex`                              :func:`variance`
    :func:`ceil`                            :func:`ltrim`                           :func:`to_unix_timestamp`
    :func:`ceiling`                         :func:`map`                             :func:`to_unixtime`
    :func:`chr`                             :func:`map_concat`                      :func:`to_utf8`
    :func:`clamp`                           :func:`map_entries`                     :func:`transform`
    :func:`coalesce`                        :func:`map_filter`                      :func:`trim`
    :func:`codepoint`                       :func:`map_keys`                        :func:`upper`
    :func:`concat`                          :func:`map_values`                      :func:`url_decode`
    :func:`contains`                        :func:`md5`                             :func:`url_encode`
    :func:`cos`                             :func:`millisecond`                     :func:`url_extract_fragment`
    :func:`cosh`                            minus                                   :func:`url_extract_host`
    :func:`date_trunc`                      :func:`minute`                          :func:`url_extract_parameter`
    :func:`day`                             modulus                                 :func:`url_extract_path`
    :func:`day_of_month`                    :func:`month`                           :func:`url_extract_port`
    :func:`day_of_week`                     multiply                                :func:`url_extract_protocol`
    :func:`day_of_year`                     :func:`nan`                             :func:`url_extract_query`
    divide                                  negate                                  :func:`width_bucket`
    :func:`dow`                             neq                                     :func:`xxhash64`
    :func:`doy`                             not                                     :func:`year`
    :func:`element_at`                      :func:`parse_datetime`                  :func:`year_of_week`
    :func:`empty_approx_set`                plus                                    :func:`yow`
    eq                                      :func:`pow`                             :func:`zip`
    :func:`exp`                             :func:`power`
    :func:`filter`                          :func:`quarter`
    ======================================  ======================================  ======================================  ==  ======================================
