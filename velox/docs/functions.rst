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
    functions/window
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
    :func:`acos`                            :func:`eq`                              :func:`power`                               :func:`approx_most_frequent`
    :func:`array_constructor`               :func:`exp`                             :func:`quarter`                             :func:`approx_percentile`
    :func:`array_distinct`                  :func:`filter`                          :func:`radians`                             :func:`approx_set`
    :func:`array_duplicates`                :func:`floor`                           :func:`rand`                                :func:`arbitrary`
    :func:`array_except`                    :func:`format_datetime`                 :func:`random`                              :func:`array_agg`
    :func:`array_intersect`                 :func:`from_base`                       :func:`reduce`                              :func:`avg`
    :func:`array_join`                      :func:`from_base64`                     :func:`regexp_extract`                      :func:`bitwise_and_agg`
    :func:`array_max`                       :func:`from_hex`                        :func:`regexp_extract_all`                  :func:`bitwise_or_agg`
    :func:`array_min`                       :func:`from_unixtime`                   :func:`regexp_like`                         :func:`bool_and`
    :func:`array_position`                  :func:`greatest`                        :func:`regexp_replace`                      :func:`bool_or`
    :func:`array_sort`                      :func:`gt`                              :func:`replace`                             :func:`checksum`
    :func:`array_sum`                       :func:`gte`                             :func:`reverse`                             :func:`corr`
    :func:`arrays_overlap`                  :func:`hour`                            :func:`round`                               :func:`count`
    :func:`asin`                            in                                      :func:`rpad`                                :func:`count_if`
    :func:`atan`                            :func:`infinity`                        :func:`rtrim`                               :func:`covar_pop`
    :func:`atan2`                           :func:`is_finite`                       :func:`second`                              :func:`covar_samp`
    :func:`between`                         :func:`is_infinite`                     :func:`sha256`                              :func:`every`
    :func:`bit_count`                       :func:`is_json_scalar`                  :func:`sha512`                              :func:`histogram`
    :func:`bitwise_and`                     :func:`is_nan`                          :func:`sign`                                :func:`map_agg`
    :func:`bitwise_arithmetic_shift_right`  :func:`is_null`                         :func:`sin`                                 :func:`map_union`
    :func:`bitwise_left_shift`              :func:`json_array_contains`             :func:`slice`                               :func:`max`
    :func:`bitwise_logical_shift_right`     :func:`json_array_length`               :func:`split`                               :func:`max_by`
    :func:`bitwise_not`                     :func:`json_extract_scalar`             :func:`split_part`                          :func:`max_data_size_for_stats`
    :func:`bitwise_or`                      :func:`least`                           :func:`sqrt`                                :func:`merge`
    :func:`bitwise_right_shift`             :func:`length`                          :func:`strpos`                              :func:`min`
    :func:`bitwise_right_shift_arithmetic`  :func:`like`                            :func:`subscript`                           :func:`min_by`
    :func:`bitwise_shift_left`              :func:`ln`                              :func:`substr`                              :func:`stddev`
    :func:`bitwise_xor`                     :func:`log10`                           :func:`tan`                                 :func:`stddev_pop`
    :func:`cardinality`                     :func:`log2`                            :func:`tanh`                                :func:`stddev_samp`
    :func:`cbrt`                            :func:`lower`                           :func:`to_base`                             :func:`sum`
    :func:`ceil`                            :func:`lpad`                            :func:`to_base64`                           :func:`var_pop`
    :func:`ceiling`                         :func:`lt`                              :func:`to_hex`                              :func:`var_samp`
    :func:`chr`                             :func:`lte`                             :func:`to_unixtime`                         :func:`variance`
    :func:`clamp`                           :func:`ltrim`                           :func:`to_utf8`
    :func:`codepoint`                       :func:`map`                             :func:`transform`
    :func:`combinations`                    :func:`map_concat`                      :func:`transform_keys`
    :func:`concat`                          :func:`map_concat_empty_nulls`          :func:`transform_values`
    :func:`contains`                        :func:`map_entries`                     :func:`trim`
    :func:`cos`                             :func:`map_filter`                      :func:`truncate`
    :func:`cosh`                            :func:`map_keys`                        :func:`upper`
    :func:`date_add`                        :func:`map_values`                      :func:`url_decode`
    :func:`date_diff`                       :func:`map_zip_with`                    :func:`url_encode`
    :func:`date_format`                     :func:`md5`                             :func:`url_extract_fragment`
    :func:`date_parse`                      :func:`millisecond`                     :func:`url_extract_host`
    :func:`date_trunc`                      :func:`minus`                           :func:`url_extract_parameter`
    :func:`day`                             :func:`minute`                          :func:`url_extract_path`
    :func:`day_of_month`                    :func:`mod`                             :func:`url_extract_port`
    :func:`day_of_week`                     :func:`month`                           :func:`url_extract_protocol`
    :func:`day_of_year`                     :func:`multiply`                        :func:`url_extract_query`
    :func:`degrees`                         :func:`nan`                             :func:`width_bucket`
    :func:`distinct_from`                   :func:`negate`                          :func:`xxhash64`
    :func:`divide`                          :func:`neq`                             :func:`year`
    :func:`dow`                             not                                     :func:`year_of_week`
    :func:`doy`                             :func:`parse_datetime`                  :func:`yow`
    :func:`e`                               :func:`pi`                              :func:`zip`
    :func:`element_at`                      :func:`plus`                            :func:`zip_with`
    ======================================  ======================================  ======================================  ==  ======================================
