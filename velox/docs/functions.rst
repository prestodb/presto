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
    :func:`acos`                            :func:`from_base64`                     :func:`radians`                             :func:`approx_percentile`
    :func:`array_constructor`               :func:`from_hex`                        :func:`rand`                                :func:`approx_set`
    :func:`array_distinct`                  :func:`from_unixtime`                   :func:`random`                              :func:`arbitrary`
    :func:`array_duplicates`                :func:`greatest`                        :func:`reduce`                              :func:`array_agg`
    :func:`array_except`                    :func:`gt`                              :func:`regexp_extract`                      :func:`avg`
    :func:`array_intersect`                 :func:`gte`                             :func:`regexp_extract_all`                  :func:`bitwise_and_agg`
    :func:`array_max`                       :func:`hour`                            :func:`regexp_like`                         :func:`bitwise_or_agg`
    :func:`array_min`                       in                                      :func:`replace`                             :func:`bool_and`
    :func:`asin`                            :func:`infinity`                        :func:`reverse`                             :func:`bool_or`
    :func:`atan`                            :func:`is_finite`                       :func:`round`                               :func:`corr`
    :func:`atan2`                           :func:`is_infinite`                     :func:`rpad`                                :func:`count`
    :func:`between`                         :func:`is_nan`                          :func:`rtrim`                               :func:`count_if`
    :func:`bitwise_and`                     :func:`is_null`                         :func:`second`                              :func:`covar_pop`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_extract_scalar`             :func:`sign`                                :func:`covar_samp`
    :func:`bitwise_left_shift`              :func:`least`                           :func:`sin`                                 :func:`every`
    :func:`bitwise_logical_shift_right`     :func:`length`                          :func:`slice`                               :func:`map_agg`
    :func:`bitwise_not`                     :func:`like`                            :func:`split`                               :func:`max`
    :func:`bitwise_or`                      :func:`ln`                              :func:`split_part`                          :func:`max_by`
    :func:`bitwise_right_shift`             :func:`log10`                           :func:`sqrt`                                :func:`merge`
    :func:`bitwise_right_shift_arithmetic`  :func:`log2`                            :func:`strpos`                              :func:`min`
    :func:`bitwise_shift_left`              :func:`lower`                           :func:`subscript`                           :func:`min_by`
    :func:`bitwise_xor`                     :func:`lpad`                            :func:`substr`                              :func:`stddev`
    :func:`cardinality`                     :func:`lt`                              :func:`tan`                                 :func:`stddev_pop`
    :func:`cbrt`                            :func:`lte`                             :func:`tanh`                                :func:`stddev_samp`
    :func:`ceil`                            :func:`ltrim`                           :func:`to_base64`                           :func:`sum`
    :func:`ceiling`                         :func:`map`                             :func:`to_hex`                              :func:`var_pop`
    :func:`chr`                             :func:`map_concat`                      :func:`to_unixtime`                         :func:`var_samp`
    :func:`clamp`                           :func:`map_concat_empty_nulls`          :func:`to_utf8`                             :func:`variance`
    :func:`coalesce`                        :func:`map_entries`                     :func:`transform`
    :func:`codepoint`                       :func:`map_filter`                      :func:`trim`
    :func:`concat`                          :func:`map_keys`                        :func:`upper`
    :func:`contains`                        :func:`map_values`                      :func:`url_decode`
    :func:`cos`                             :func:`md5`                             :func:`url_encode`
    :func:`cosh`                            :func:`millisecond`                     :func:`url_extract_fragment`
    :func:`date_trunc`                      :func:`minus`                           :func:`url_extract_host`
    :func:`day`                             :func:`minute`                          :func:`url_extract_parameter`
    :func:`day_of_month`                    modulus                                 :func:`url_extract_path`
    :func:`day_of_week`                     :func:`month`                           :func:`url_extract_port`
    :func:`day_of_year`                     :func:`multiply`                        :func:`url_extract_protocol`
    :func:`divide`                          :func:`nan`                             :func:`url_extract_query`
    :func:`dow`                             :func:`negate`                          :func:`width_bucket`
    :func:`doy`                             :func:`neq`                             :func:`xxhash64`
    :func:`element_at`                      not                                     :func:`year`
    :func:`empty_approx_set`                :func:`parse_datetime`                  :func:`year_of_week`
    :func:`eq`                              :func:`plus`                            :func:`yow`
    :func:`exp`                             :func:`pow`                             :func:`zip`
    :func:`filter`                          :func:`power`
    ======================================  ======================================  ======================================  ==  ======================================

