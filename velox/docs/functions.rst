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

    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
    Scalar Functions                                                                                                            Aggregate Functions                         Window Functions
    ======================================================================================================================  ==  ======================================  ==  ======================================
    :func:`abs`                             :func:`filter`                          :func:`radians`                             :func:`approx_distinct`                     :func:`cume_dist`
    :func:`acos`                            :func:`flatten`                         :func:`rand`                                :func:`approx_most_frequent`                :func:`dense_rank`
    :func:`all_match`                       :func:`floor`                           :func:`random`                              :func:`approx_percentile`                   :func:`first_value`
    :func:`any_match`                       :func:`format_datetime`                 :func:`reduce`                              :func:`approx_set`                          :func:`lag`
    :func:`array_average`                   :func:`from_base`                       :func:`regexp_extract`                      :func:`arbitrary`                           :func:`last_value`
    :func:`array_constructor`               :func:`from_base64`                     :func:`regexp_extract_all`                  :func:`array_agg`                           :func:`lead`
    :func:`array_distinct`                  :func:`from_base64url`                  :func:`regexp_like`                         :func:`avg`                                 :func:`nth_value`
    :func:`array_duplicates`                :func:`from_big_endian_32`              :func:`regexp_replace`                      :func:`bitwise_and_agg`                     :func:`ntile`
    :func:`array_except`                    :func:`from_big_endian_64`              :func:`repeat`                              :func:`bitwise_or_agg`                      :func:`percent_rank`
    :func:`array_frequency`                 :func:`from_hex`                        :func:`replace`                             :func:`bool_and`                            :func:`rank`
    :func:`array_has_duplicates`            :func:`from_unixtime`                   :func:`reverse`                             :func:`bool_or`                             :func:`row_number`
    :func:`array_intersect`                 :func:`from_utf8`                       :func:`round`                               :func:`checksum`
    :func:`array_join`                      :func:`greatest`                        :func:`rpad`                                :func:`corr`
    :func:`array_max`                       :func:`gt`                              :func:`rtrim`                               :func:`count`
    :func:`array_min`                       :func:`gte`                             :func:`second`                              :func:`count_if`
    :func:`array_normalize`                 :func:`hmac_md5`                        :func:`sequence`                            :func:`covar_pop`
    :func:`array_position`                  :func:`hmac_sha1`                       :func:`sha1`                                :func:`covar_samp`
    :func:`array_sort`                      :func:`hmac_sha256`                     :func:`sha256`                              :func:`every`
    :func:`array_sort_desc`                 :func:`hmac_sha512`                     :func:`sha512`                              :func:`histogram`
    :func:`array_sum`                       :func:`hour`                            :func:`shuffle`                             :func:`kurtosis`
    :func:`arrays_overlap`                  in                                      :func:`sign`                                :func:`map_agg`
    :func:`asin`                            :func:`infinity`                        :func:`sin`                                 :func:`map_union`
    :func:`atan`                            :func:`is_finite`                       :func:`slice`                               :func:`map_union_sum`
    :func:`atan2`                           :func:`is_infinite`                     :func:`split`                               :func:`max`
    :func:`beta_cdf`                        :func:`is_json_scalar`                  :func:`split_part`                          :func:`max_by`
    :func:`between`                         :func:`is_nan`                          :func:`spooky_hash_v2_32`                   :func:`max_data_size_for_stats`
    :func:`binomial_cdf`                    :func:`is_null`                         :func:`spooky_hash_v2_64`                   :func:`merge`
    :func:`bit_count`                       :func:`json_array_contains`             :func:`sqrt`                                :func:`min`
    :func:`bitwise_and`                     :func:`json_array_length`               :func:`strpos`                              :func:`min_by`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_extract`                    :func:`strrpos`                             :func:`regr_intercept`
    :func:`bitwise_left_shift`              :func:`json_extract_scalar`             :func:`subscript`                           :func:`regr_slope`
    :func:`bitwise_logical_shift_right`     :func:`json_format`                     :func:`substr`                              :func:`set_agg`
    :func:`bitwise_not`                     :func:`json_parse`                      :func:`tan`                                 :func:`set_union`
    :func:`bitwise_or`                      :func:`json_size`                       :func:`tanh`                                :func:`skewness`
    :func:`bitwise_right_shift`             :func:`least`                           :func:`timezone_hour`                       :func:`stddev`
    :func:`bitwise_right_shift_arithmetic`  :func:`length`                          :func:`timezone_minute`                     :func:`stddev_pop`
    :func:`bitwise_shift_left`              :func:`like`                            :func:`to_base`                             :func:`stddev_samp`
    :func:`bitwise_xor`                     :func:`ln`                              :func:`to_base64`                           :func:`sum`
    :func:`cardinality`                     :func:`log10`                           :func:`to_base64url`                        :func:`sum_data_size_for_stats`
    :func:`cbrt`                            :func:`log2`                            :func:`to_big_endian_32`                    :func:`var_pop`
    :func:`ceil`                            :func:`lower`                           :func:`to_big_endian_64`                    :func:`var_samp`
    :func:`ceiling`                         :func:`lpad`                            :func:`to_hex`                              :func:`variance`
    :func:`chr`                             :func:`lt`                              :func:`to_ieee754_64`
    :func:`clamp`                           :func:`lte`                             :func:`to_unixtime`
    :func:`codepoint`                       :func:`ltrim`                           :func:`to_utf8`
    :func:`combinations`                    :func:`map`                             :func:`transform`
    :func:`concat`                          :func:`map_concat`                      :func:`transform_keys`
    :func:`contains`                        :func:`map_entries`                     :func:`transform_values`
    :func:`cos`                             :func:`map_filter`                      :func:`trim`
    :func:`cosh`                            :func:`map_from_entries`                :func:`trim_array`
    :func:`crc32`                           :func:`map_keys`                        :func:`truncate`
    :func:`current_date`                    :func:`map_values`                      :func:`upper`
    :func:`date`                            :func:`map_zip_with`                    :func:`url_decode`
    :func:`date_add`                        :func:`md5`                             :func:`url_encode`
    :func:`date_diff`                       :func:`millisecond`                     :func:`url_extract_fragment`
    :func:`date_format`                     :func:`minus`                           :func:`url_extract_host`
    :func:`date_parse`                      :func:`minute`                          :func:`url_extract_parameter`
    :func:`date_trunc`                      :func:`mod`                             :func:`url_extract_path`
    :func:`day`                             :func:`month`                           :func:`url_extract_port`
    :func:`day_of_month`                    :func:`multiply`                        :func:`url_extract_protocol`
    :func:`day_of_week`                     :func:`nan`                             :func:`url_extract_query`
    :func:`day_of_year`                     :func:`negate`                          :func:`week`
    :func:`degrees`                         :func:`neq`                             :func:`week_of_year`
    :func:`distinct_from`                   :func:`none_match`                      :func:`width_bucket`
    :func:`divide`                          :func:`normal_cdf`                      :func:`xxhash64`
    :func:`dow`                             not                                     :func:`year`
    :func:`doy`                             :func:`parse_datetime`                  :func:`year_of_week`
    :func:`e`                               :func:`pi`                              :func:`yow`
    :func:`element_at`                      :func:`plus`                            :func:`zip`
    :func:`empty_approx_set`                :func:`pow`                             :func:`zip_with`
    :func:`eq`                              :func:`power`
    :func:`exp`                             :func:`quarter`
    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
