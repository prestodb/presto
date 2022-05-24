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
    :func:`abs`                             :func:`empty_approx_set`                :func:`pow`                                 :func:`approx_distinct`
    :func:`acos`                            :func:`eq`                              :func:`power`                               :func:`approx_percentile`
    :func:`array_constructor`               :func:`exp`                             :func:`quarter`                             :func:`approx_set`
    :func:`array_distinct`                  :func:`filter`                          :func:`radians`                             :func:`arbitrary`
    :func:`array_duplicates`                :func:`floor`                           :func:`rand`                                :func:`array_agg`
    :func:`array_except`                    :func:`from_base`                       :func:`random`                              :func:`avg`
    :func:`array_intersect`                 :func:`from_base64`                     :func:`reduce`                              :func:`bitwise_and_agg`
    :func:`array_join`                      :func:`from_hex`                        :func:`regexp_extract`                      :func:`bitwise_or_agg`
    :func:`array_max`                       :func:`from_unixtime`                   :func:`regexp_extract_all`                  :func:`bool_and`
    :func:`array_min`                       :func:`greatest`                        :func:`regexp_like`                         :func:`bool_or`
    :func:`array_position`                  :func:`gt`                              :func:`regexp_replace`                      :func:`checksum`
    :func:`arrays_overlap`                  :func:`gte`                             :func:`replace`                             :func:`corr`
    :func:`asin`                            :func:`hour`                            :func:`reverse`                             :func:`count`
    :func:`atan`                            in                                      :func:`round`                               :func:`count_if`
    :func:`atan2`                           :func:`infinity`                        :func:`rpad`                                :func:`covar_pop`
    :func:`between`                         :func:`is_finite`                       :func:`rtrim`                               :func:`covar_samp`
    :func:`bit_count`                       :func:`is_infinite`                     :func:`second`                              :func:`every`
    :func:`bitwise_and`                     :func:`is_nan`                          :func:`sha256`                              :func:`histogram`
    :func:`bitwise_arithmetic_shift_right`  :func:`is_null`                         :func:`sign`                                :func:`map_agg`
    :func:`bitwise_left_shift`              :func:`json_extract_scalar`             :func:`sin`                                 :func:`max`
    :func:`bitwise_logical_shift_right`     :func:`least`                           :func:`slice`                               :func:`max_by`
    :func:`bitwise_not`                     :func:`length`                          :func:`split`                               :func:`merge`
    :func:`bitwise_or`                      :func:`like`                            :func:`split_part`                          :func:`min`
    :func:`bitwise_right_shift`             :func:`ln`                              :func:`sqrt`                                :func:`min_by`
    :func:`bitwise_right_shift_arithmetic`  :func:`log10`                           :func:`strpos`                              :func:`stddev`
    :func:`bitwise_shift_left`              :func:`log2`                            :func:`subscript`                           :func:`stddev_pop`
    :func:`bitwise_xor`                     :func:`lower`                           :func:`substr`                              :func:`stddev_samp`
    :func:`cardinality`                     :func:`lpad`                            :func:`tan`                                 :func:`sum`
    :func:`cbrt`                            :func:`lt`                              :func:`tanh`                                :func:`var_pop`
    :func:`ceil`                            :func:`lte`                             :func:`to_base`                             :func:`var_samp`
    :func:`ceiling`                         :func:`ltrim`                           :func:`to_base64`                           :func:`variance`
    :func:`chr`                             :func:`map`                             :func:`to_hex`
    :func:`clamp`                           :func:`map_concat`                      :func:`to_unixtime`
    :func:`codepoint`                       :func:`map_concat_empty_nulls`          :func:`to_utf8`
    :func:`concat`                          :func:`map_entries`                     :func:`transform`
    :func:`contains`                        :func:`map_filter`                      :func:`trim`
    :func:`cos`                             :func:`map_keys`                        :func:`upper`
    :func:`cosh`                            :func:`map_values`                      :func:`url_decode`
    :func:`date_add`                        :func:`md5`                             :func:`url_encode`
    :func:`date_diff`                       :func:`millisecond`                     :func:`url_extract_fragment`
    :func:`date_format`                     :func:`minus`                           :func:`url_extract_host`
    :func:`date_parse`                      :func:`minute`                          :func:`url_extract_parameter`
    :func:`date_trunc`                      :func:`mod`                             :func:`url_extract_path`
    :func:`day`                             :func:`month`                           :func:`url_extract_port`
    :func:`day_of_month`                    :func:`multiply`                        :func:`url_extract_protocol`
    :func:`day_of_week`                     :func:`nan`                             :func:`url_extract_query`
    :func:`day_of_year`                     :func:`negate`                          :func:`width_bucket`
    :func:`distinct_from`                   :func:`neq`                             :func:`xxhash64`
    :func:`divide`                          not                                     :func:`year`
    :func:`dow`                             :func:`parse_datetime`                  :func:`year_of_week`
    :func:`doy`                             :func:`pi`                              :func:`yow`
    :func:`element_at`                      :func:`plus`                            :func:`zip`
    ======================================  ======================================  ======================================  ==  ======================================
