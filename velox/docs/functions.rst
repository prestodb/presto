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
    :func:`abs`                             :func:`eq`                              :func:`power`                               :func:`approx_distinct`
    :func:`acos`                            :func:`exp`                             :func:`quarter`                             :func:`approx_percentile`
    :func:`array_constructor`               :func:`filter`                          :func:`radians`                             :func:`approx_set`
    :func:`array_distinct`                  :func:`floor`                           :func:`rand`                                :func:`arbitrary`
    :func:`array_duplicates`                :func:`from_base`                       :func:`random`                              :func:`array_agg`
    :func:`array_except`                    :func:`from_base64`                     :func:`reduce`                              :func:`avg`
    :func:`array_intersect`                 :func:`from_hex`                        :func:`regexp_extract`                      :func:`bitwise_and_agg`
    :func:`array_join`                      :func:`from_unixtime`                   :func:`regexp_extract_all`                  :func:`bitwise_or_agg`
    :func:`array_max`                       :func:`greatest`                        :func:`regexp_like`                         :func:`bool_and`
    :func:`array_min`                       :func:`gt`                              :func:`regexp_replace`                      :func:`bool_or`
    :func:`array_position`                  :func:`gte`                             :func:`replace`                             :func:`checksum`
    :func:`arrays_overlap`                  :func:`hour`                            :func:`reverse`                             :func:`corr`
    :func:`asin`                            in                                      :func:`round`                               :func:`count`
    :func:`atan`                            :func:`infinity`                        :func:`rpad`                                :func:`count_if`
    :func:`atan2`                           :func:`is_finite`                       :func:`rtrim`                               :func:`covar_pop`
    :func:`between`                         :func:`is_infinite`                     :func:`second`                              :func:`covar_samp`
    :func:`bit_count`                       :func:`is_nan`                          :func:`sha256`                              :func:`every`
    :func:`bitwise_and`                     :func:`is_null`                         :func:`sign`                                :func:`map_agg`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_extract_scalar`             :func:`sin`                                 :func:`max`
    :func:`bitwise_left_shift`              :func:`least`                           :func:`slice`                               :func:`max_by`
    :func:`bitwise_logical_shift_right`     :func:`length`                          :func:`split`                               :func:`merge`
    :func:`bitwise_not`                     :func:`like`                            :func:`split_part`                          :func:`min`
    :func:`bitwise_or`                      :func:`ln`                              :func:`sqrt`                                :func:`min_by`
    :func:`bitwise_right_shift`             :func:`log10`                           :func:`strpos`                              :func:`stddev`
    :func:`bitwise_right_shift_arithmetic`  :func:`log2`                            :func:`subscript`                           :func:`stddev_pop`
    :func:`bitwise_shift_left`              :func:`lower`                           :func:`substr`                              :func:`stddev_samp`
    :func:`bitwise_xor`                     :func:`lpad`                            :func:`tan`                                 :func:`sum`
    :func:`cardinality`                     :func:`lt`                              :func:`tanh`                                :func:`var_pop`
    :func:`cbrt`                            :func:`lte`                             :func:`to_base`                             :func:`var_samp`
    :func:`ceil`                            :func:`ltrim`                           :func:`to_base64`                           :func:`variance`
    :func:`ceiling`                         :func:`map`                             :func:`to_hex`
    :func:`chr`                             :func:`map_concat`                      :func:`to_unixtime`
    :func:`clamp`                           :func:`map_concat_empty_nulls`          :func:`to_utf8`
    :func:`coalesce`                        :func:`map_entries`                     :func:`transform`
    :func:`codepoint`                       :func:`map_filter`                      :func:`trim`
    :func:`concat`                          :func:`map_keys`                        :func:`upper`
    :func:`contains`                        :func:`map_values`                      :func:`url_decode`
    :func:`cos`                             :func:`md5`                             :func:`url_encode`
    :func:`cosh`                            :func:`millisecond`                     :func:`url_extract_fragment`
    :func:`date_add`                        :func:`minus`                           :func:`url_extract_host`
    :func:`date_diff`                       :func:`minute`                          :func:`url_extract_parameter`
    :func:`date_format`                     :func:`mod`                             :func:`url_extract_path`
    :func:`date_trunc`                      :func:`month`                           :func:`url_extract_port`
    :func:`day`                             :func:`multiply`                        :func:`url_extract_protocol`
    :func:`day_of_month`                    :func:`nan`                             :func:`url_extract_query`
    :func:`day_of_week`                     :func:`negate`                          :func:`width_bucket`
    :func:`day_of_year`                     :func:`neq`                             :func:`xxhash64`
    :func:`divide`                          not                                     :func:`year`
    :func:`dow`                             :func:`parse_datetime`                  :func:`year_of_week`
    :func:`doy`                             :func:`pi`                              :func:`yow`
    :func:`element_at`                      :func:`plus`                            :func:`zip`
    :func:`empty_approx_set`                :func:`pow`
    ======================================  ======================================  ======================================  ==  ======================================
