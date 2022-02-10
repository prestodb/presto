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
    functions/json
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
    :func:`abs`                             :func:`floor`                           :func:`quarter`                             :func:`approx_distinct`
    :func:`acos`                            :func:`from_base`                       :func:`radians`                             :func:`approx_percentile`
    :func:`array_constructor`               :func:`from_base64`                     :func:`rand`                                :func:`approx_set`
    :func:`array_distinct`                  :func:`from_hex`                        :func:`random`                              :func:`arbitrary`
    :func:`array_duplicates`                :func:`from_unixtime`                   :func:`reduce`                              :func:`array_agg`
    :func:`array_except`                    :func:`greatest`                        :func:`regexp_extract`                      :func:`avg`
    :func:`array_intersect`                 :func:`gt`                              :func:`regexp_extract_all`                  :func:`bitwise_and_agg`
    :func:`array_join`                      :func:`gte`                             :func:`regexp_like`                         :func:`bitwise_or_agg`
    :func:`array_max`                       :func:`hour`                            :func:`regexp_replace`                      :func:`bool_and`
    :func:`array_min`                       in                                      :func:`replace`                             :func:`bool_or`
    :func:`array_position`                  :func:`infinity`                        :func:`reverse`                             :func:`checksum`
    :func:`asin`                            :func:`is_finite`                       :func:`round`                               :func:`corr`
    :func:`atan`                            :func:`is_infinite`                     :func:`rpad`                                :func:`count`
    :func:`atan2`                           :func:`is_nan`                          :func:`rtrim`                               :func:`count_if`
    :func:`between`                         :func:`is_null`                         :func:`second`                              :func:`covar_pop`
    :func:`bitwise_and`                     :func:`json_extract_scalar`             :func:`sign`                                :func:`covar_samp`
    :func:`bitwise_arithmetic_shift_right`  :func:`least`                           :func:`sin`                                 :func:`every`
    :func:`bitwise_left_shift`              :func:`length`                          :func:`slice`                               :func:`map_agg`
    :func:`bitwise_logical_shift_right`     :func:`like`                            :func:`split`                               :func:`max`
    :func:`bitwise_not`                     :func:`ln`                              :func:`split_part`                          :func:`max_by`
    :func:`bitwise_or`                      :func:`log10`                           :func:`sqrt`                                :func:`merge`
    :func:`bitwise_right_shift`             :func:`log2`                            :func:`strpos`                              :func:`min`
    :func:`bitwise_right_shift_arithmetic`  :func:`lower`                           :func:`subscript`                           :func:`min_by`
    :func:`bitwise_shift_left`              :func:`lpad`                            :func:`substr`                              :func:`stddev`
    :func:`bitwise_xor`                     :func:`lt`                              :func:`tan`                                 :func:`stddev_pop`
    :func:`cardinality`                     :func:`lte`                             :func:`tanh`                                :func:`stddev_samp`
    :func:`cbrt`                            :func:`ltrim`                           :func:`to_base`                             :func:`sum`
    :func:`ceil`                            :func:`map`                             :func:`to_base64`                           :func:`var_pop`
    :func:`ceiling`                         :func:`map_concat`                      :func:`to_hex`                              :func:`var_samp`
    :func:`chr`                             :func:`map_concat_empty_nulls`          :func:`to_unixtime`                         :func:`variance`
    :func:`clamp`                           :func:`map_entries`                     :func:`to_utf8`
    :func:`coalesce`                        :func:`map_filter`                      :func:`transform`
    :func:`codepoint`                       :func:`map_keys`                        :func:`trim`
    :func:`concat`                          :func:`map_values`                      :func:`upper`
    :func:`contains`                        :func:`md5`                             :func:`url_decode`
    :func:`cos`                             :func:`millisecond`                     :func:`url_encode`
    :func:`cosh`                            :func:`minus`                           :func:`url_extract_fragment`
    :func:`date_trunc`                      :func:`minute`                          :func:`url_extract_host`
    :func:`day`                             :func:`mod`                             :func:`url_extract_parameter`
    :func:`day_of_month`                    :func:`month`                           :func:`url_extract_path`
    :func:`day_of_week`                     :func:`multiply`                        :func:`url_extract_port`
    :func:`day_of_year`                     :func:`nan`                             :func:`url_extract_protocol`
    :func:`divide`                          :func:`negate`                          :func:`url_extract_query`
    :func:`dow`                             :func:`neq`                             :func:`width_bucket`
    :func:`doy`                             not                                     :func:`xxhash64`
    :func:`element_at`                      :func:`parse_datetime`                  :func:`year`
    :func:`empty_approx_set`                :func:`pi`                              :func:`year_of_week`
    :func:`eq`                              :func:`plus`                            :func:`yow`
    :func:`exp`                             :func:`pow`                             :func:`zip`
    :func:`filter`                          :func:`power`
    ======================================  ======================================  ======================================  ==  ======================================

