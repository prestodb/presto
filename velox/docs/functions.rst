***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/presto/math
    functions/presto/bitwise
    functions/presto/comparison
    functions/presto/string
    functions/presto/datetime
    functions/presto/array
    functions/presto/map
    functions/presto/regexp
    functions/presto/binary
    functions/presto/json
    functions/presto/url
    functions/presto/aggregate
    functions/presto/window
    functions/presto/hyperloglog

Here is a list of all scalar and aggregate Presto functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/presto/coverage>` and :doc:`most used
<functions/presto/most_used_coverage>` functions for broader context.

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

    table.rows td:nth-child(4), td:nth-child(6) {
        background-color: lightblue;
    }
    </style>

.. table::
    :widths: auto
    :class: rows

    ======================================================  ======================================================  ======================================================  ==  ======================================================  ==  ======================================================
    Scalar Functions                                                                                                                                                            Aggregate Functions                                         Window Functions
    ======================================================================================================================================================================  ==  ======================================================  ==  ======================================================
    :func:`abs`                                             :func:`day_of_year`                                     :func:`pi`                                                  :func:`approx_distinct`                                     :func:`cume_dist`
    :func:`acos`                                            :func:`degrees`                                         :func:`plus`                                                :func:`approx_distinct_merge`                               :func:`dense_rank`
    :func:`all_match`                                       :func:`distinct_from`                                   :func:`pow`                                                 :func:`approx_distinct_merge_extract_bigint`                :func:`first_value`
    :func:`any_match`                                       :func:`divide`                                          :func:`power`                                               :func:`approx_distinct_partial`                             :func:`lag`
    :func:`approx_distinct_extract_bigint`                  :func:`dow`                                             :func:`quarter`                                             :func:`approx_most_frequent`                                :func:`last_value`
    :func:`approx_percentile_extract_array_bigint`          :func:`doy`                                             :func:`radians`                                             :func:`approx_percentile`                                   :func:`lead`
    :func:`approx_percentile_extract_array_double`          :func:`e`                                               :func:`rand`                                                :func:`approx_percentile_merge`                             :func:`nth_value`
    :func:`approx_percentile_extract_array_integer`         :func:`element_at`                                      :func:`random`                                              :func:`approx_percentile_merge_extract_array_bigint`        :func:`ntile`
    :func:`approx_percentile_extract_array_real`            :func:`empty_approx_set`                                :func:`reduce`                                              :func:`approx_percentile_merge_extract_array_double`        :func:`percent_rank`
    :func:`approx_percentile_extract_array_smallint`        :func:`eq`                                              :func:`regexp_extract`                                      :func:`approx_percentile_merge_extract_array_integer`       :func:`rank`
    :func:`approx_percentile_extract_array_tinyint`         :func:`exp`                                             :func:`regexp_extract_all`                                  :func:`approx_percentile_merge_extract_array_real`          :func:`row_number`
    :func:`approx_percentile_extract_bigint`                :func:`filter`                                          :func:`regexp_like`                                         :func:`approx_percentile_merge_extract_array_smallint`
    :func:`approx_percentile_extract_double`                :func:`floor`                                           :func:`regexp_replace`                                      :func:`approx_percentile_merge_extract_array_tinyint`
    :func:`approx_percentile_extract_integer`               :func:`format_datetime`                                 :func:`repeat`                                              :func:`approx_percentile_merge_extract_bigint`
    :func:`approx_percentile_extract_real`                  :func:`from_base`                                       :func:`replace`                                             :func:`approx_percentile_merge_extract_double`
    :func:`approx_percentile_extract_smallint`              :func:`from_base64`                                     :func:`reverse`                                             :func:`approx_percentile_merge_extract_integer`
    :func:`approx_percentile_extract_tinyint`               :func:`from_base64url`                                  :func:`round`                                               :func:`approx_percentile_merge_extract_real`
    :func:`approx_set_extract_hyperloglog`                  :func:`from_big_endian_32`                              :func:`rpad`                                                :func:`approx_percentile_merge_extract_smallint`
    :func:`array_average`                                   :func:`from_big_endian_64`                              :func:`rtrim`                                               :func:`approx_percentile_merge_extract_tinyint`
    :func:`array_constructor`                               :func:`from_hex`                                        :func:`second`                                              :func:`approx_percentile_partial`
    :func:`array_distinct`                                  :func:`from_unixtime`                                   :func:`sequence`                                            :func:`approx_set`
    :func:`array_duplicates`                                :func:`from_utf8`                                       :func:`sha1`                                                :func:`approx_set_merge`
    :func:`array_except`                                    :func:`greatest`                                        :func:`sha256`                                              :func:`approx_set_merge_extract_hyperloglog`
    :func:`array_frequency`                                 :func:`gt`                                              :func:`sha512`                                              :func:`approx_set_partial`
    :func:`array_has_duplicates`                            :func:`gte`                                             :func:`shuffle`                                             :func:`arbitrary`
    :func:`array_intersect`                                 :func:`hmac_md5`                                        :func:`sign`                                                :func:`array_agg`
    :func:`array_join`                                      :func:`hmac_sha1`                                       :func:`sin`                                                 :func:`avg`
    :func:`array_max`                                       :func:`hmac_sha256`                                     :func:`slice`                                               :func:`avg_merge`
    :func:`array_min`                                       :func:`hmac_sha512`                                     :func:`split`                                               :func:`avg_merge_extract_double`
    :func:`array_normalize`                                 :func:`hour`                                            :func:`split_part`                                          :func:`avg_merge_extract_real`
    :func:`array_position`                                  in                                                      :func:`spooky_hash_v2_32`                                   :func:`avg_partial`
    :func:`array_sort`                                      :func:`infinity`                                        :func:`spooky_hash_v2_64`                                   :func:`bitwise_and_agg`
    :func:`array_sum`                                       :func:`is_finite`                                       :func:`sqrt`                                                :func:`bitwise_or_agg`
    :func:`arrays_overlap`                                  :func:`is_infinite`                                     :func:`strpos`                                              :func:`bool_and`
    :func:`asin`                                            :func:`is_json_scalar`                                  :func:`strrpos`                                             :func:`bool_or`
    :func:`atan`                                            :func:`is_nan`                                          :func:`subscript`                                           :func:`checksum`
    :func:`atan2`                                           :func:`is_null`                                         :func:`substr`                                              :func:`corr`
    :func:`avg_extract_double`                              :func:`json_array_contains`                             :func:`tan`                                                 :func:`count`
    :func:`avg_extract_real`                                :func:`json_array_length`                               :func:`tanh`                                                :func:`count_if`
    :func:`beta_cdf`                                        :func:`json_extract_scalar`                             :func:`timezone_hour`                                       :func:`covar_pop`
    :func:`between`                                         :func:`json_format`                                     :func:`timezone_minute`                                     :func:`covar_samp`
    :func:`binomial_cdf`                                    :func:`json_parse`                                      :func:`to_base`                                             :func:`every`
    :func:`bit_count`                                       :func:`json_size`                                       :func:`to_base64`                                           :func:`histogram`
    :func:`bitwise_and`                                     :func:`least`                                           :func:`to_base64url`                                        :func:`map_agg`
    :func:`bitwise_arithmetic_shift_right`                  :func:`length`                                          :func:`to_big_endian_32`                                    :func:`map_union`
    :func:`bitwise_left_shift`                              :func:`like`                                            :func:`to_big_endian_64`                                    :func:`map_union_sum`
    :func:`bitwise_logical_shift_right`                     :func:`ln`                                              :func:`to_hex`                                              :func:`max`
    :func:`bitwise_not`                                     :func:`log10`                                           :func:`to_ieee754_64`                                       :func:`max_by`
    :func:`bitwise_or`                                      :func:`log2`                                            :func:`to_unixtime`                                         :func:`max_data_size_for_stats`
    :func:`bitwise_right_shift`                             :func:`lower`                                           :func:`to_utf8`                                             :func:`merge`
    :func:`bitwise_right_shift_arithmetic`                  :func:`lpad`                                            :func:`transform`                                           :func:`merge_merge`
    :func:`bitwise_shift_left`                              :func:`lt`                                              :func:`transform_keys`                                      :func:`merge_merge_extract`
    :func:`bitwise_xor`                                     :func:`lte`                                             :func:`transform_values`                                    :func:`merge_partial`
    :func:`cardinality`                                     :func:`ltrim`                                           :func:`trim`                                                :func:`min`
    :func:`cbrt`                                            :func:`map`                                             :func:`truncate`                                            :func:`min_by`
    :func:`ceil`                                            :func:`map_concat`                                      :func:`upper`                                               :func:`regr_intercept`
    :func:`ceiling`                                         :func:`map_entries`                                     :func:`url_decode`                                          :func:`regr_slope`
    :func:`chr`                                             :func:`map_filter`                                      :func:`url_encode`                                          :func:`stddev`
    :func:`clamp`                                           :func:`map_keys`                                        :func:`url_extract_fragment`                                :func:`stddev_pop`
    :func:`codepoint`                                       :func:`map_values`                                      :func:`url_extract_host`                                    :func:`stddev_samp`
    :func:`combinations`                                    :func:`map_zip_with`                                    :func:`url_extract_parameter`                               :func:`sum`
    :func:`concat`                                          :func:`md5`                                             :func:`url_extract_path`                                    :func:`var_pop`
    :func:`contains`                                        :func:`merge_extract`                                   :func:`url_extract_port`                                    :func:`var_samp`
    :func:`cos`                                             :func:`millisecond`                                     :func:`url_extract_protocol`                                :func:`variance`
    :func:`cosh`                                            :func:`minus`                                           :func:`url_extract_query`
    :func:`crc32`                                           :func:`minute`                                          :func:`week`
    :func:`current_date`                                    :func:`mod`                                             :func:`week_of_year`
    :func:`date`                                            :func:`month`                                           :func:`width_bucket`
    :func:`date_add`                                        :func:`multiply`                                        :func:`xxhash64`
    :func:`date_diff`                                       :func:`nan`                                             :func:`year`
    :func:`date_format`                                     :func:`negate`                                          :func:`year_of_week`
    :func:`date_parse`                                      :func:`neq`                                             :func:`yow`
    :func:`date_trunc`                                      :func:`none_match`                                      :func:`zip`
    :func:`day`                                             :func:`normal_cdf`                                      :func:`zip_with`
    :func:`day_of_month`                                    not
    :func:`day_of_week`                                     :func:`parse_datetime`
    ======================================================  ======================================================  ======================================================  ==  ======================================================  ==  ======================================================
