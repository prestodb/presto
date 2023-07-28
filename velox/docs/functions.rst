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
    :func:`abs`                             :func:`eq`                              :func:`power`                               :func:`approx_distinct`                     :func:`cume_dist`
    :func:`acos`                            :func:`exp`                             :func:`quarter`                             :func:`approx_most_frequent`                :func:`dense_rank`
    :func:`all_match`                       :func:`filter`                          :func:`radians`                             :func:`approx_percentile`                   :func:`first_value`
    :func:`any_match`                       :func:`flatten`                         :func:`rand`                                :func:`approx_set`                          :func:`lag`
    :func:`array_average`                   :func:`floor`                           :func:`random`                              :func:`arbitrary`                           :func:`last_value`
    :func:`array_constructor`               :func:`format_datetime`                 :func:`reduce`                              :func:`array_agg`                           :func:`lead`
    :func:`array_distinct`                  :func:`from_base`                       :func:`regexp_extract`                      :func:`avg`                                 :func:`nth_value`
    :func:`array_duplicates`                :func:`from_base64`                     :func:`regexp_extract_all`                  :func:`bitwise_and_agg`                     :func:`ntile`
    :func:`array_except`                    :func:`from_base64url`                  :func:`regexp_like`                         :func:`bitwise_or_agg`                      :func:`percent_rank`
    :func:`array_frequency`                 :func:`from_big_endian_32`              :func:`regexp_replace`                      :func:`bool_and`                            :func:`rank`
    :func:`array_has_duplicates`            :func:`from_big_endian_64`              :func:`repeat`                              :func:`bool_or`                             :func:`row_number`
    :func:`array_intersect`                 :func:`from_hex`                        :func:`replace`                             :func:`checksum`
    :func:`array_join`                      :func:`from_unixtime`                   :func:`reverse`                             :func:`corr`
    :func:`array_max`                       :func:`from_utf8`                       :func:`round`                               :func:`count`
    :func:`array_min`                       :func:`greatest`                        :func:`rpad`                                :func:`count_if`
    :func:`array_normalize`                 :func:`gt`                              :func:`rtrim`                               :func:`covar_pop`
    :func:`array_position`                  :func:`gte`                             :func:`second`                              :func:`covar_samp`
    :func:`array_sort`                      :func:`hmac_md5`                        :func:`sequence`                            :func:`entropy`
    :func:`array_sort_desc`                 :func:`hmac_sha1`                       :func:`sha1`                                :func:`every`
    :func:`array_sum`                       :func:`hmac_sha256`                     :func:`sha256`                              :func:`histogram`
    :func:`array_union`                     :func:`hmac_sha512`                     :func:`sha512`                              :func:`kurtosis`
    :func:`arrays_overlap`                  :func:`hour`                            :func:`shuffle`                             :func:`map_agg`
    :func:`asin`                            in                                      :func:`sign`                                :func:`map_union`
    :func:`atan`                            :func:`infinity`                        :func:`sin`                                 :func:`map_union_sum`
    :func:`atan2`                           :func:`inverse_beta_cdf`                :func:`slice`                               :func:`max`
    :func:`beta_cdf`                        :func:`is_finite`                       :func:`split`                               :func:`max_by`
    :func:`between`                         :func:`is_infinite`                     :func:`split_part`                          :func:`max_data_size_for_stats`
    :func:`binomial_cdf`                    :func:`is_json_scalar`                  :func:`spooky_hash_v2_32`                   :func:`merge`
    :func:`bit_count`                       :func:`is_nan`                          :func:`spooky_hash_v2_64`                   :func:`min`
    :func:`bitwise_and`                     :func:`is_null`                         :func:`sqrt`                                :func:`min_by`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_array_contains`             :func:`strpos`                              :func:`regr_intercept`
    :func:`bitwise_left_shift`              :func:`json_array_length`               :func:`strrpos`                             :func:`regr_slope`
    :func:`bitwise_logical_shift_right`     :func:`json_extract`                    :func:`subscript`                           :func:`set_agg`
    :func:`bitwise_not`                     :func:`json_extract_scalar`             :func:`substr`                              :func:`set_union`
    :func:`bitwise_or`                      :func:`json_format`                     :func:`tan`                                 :func:`skewness`
    :func:`bitwise_right_shift`             :func:`json_parse`                      :func:`tanh`                                :func:`stddev`
    :func:`bitwise_right_shift_arithmetic`  :func:`json_size`                       :func:`timezone_hour`                       :func:`stddev_pop`
    :func:`bitwise_shift_left`              :func:`least`                           :func:`timezone_minute`                     :func:`stddev_samp`
    :func:`bitwise_xor`                     :func:`length`                          :func:`to_base`                             :func:`sum`
    :func:`cardinality`                     :func:`like`                            :func:`to_base64`                           :func:`sum_data_size_for_stats`
    :func:`cauchy_cdf`                      :func:`ln`                              :func:`to_base64url`                        :func:`var_pop`
    :func:`cbrt`                            :func:`log10`                           :func:`to_big_endian_32`                    :func:`var_samp`
    :func:`ceil`                            :func:`log2`                            :func:`to_big_endian_64`                    :func:`variance`
    :func:`ceiling`                         :func:`lower`                           :func:`to_hex`
    :func:`chi_squared_cdf`                 :func:`lpad`                            :func:`to_ieee754_64`
    :func:`chr`                             :func:`lt`                              :func:`to_unixtime`
    :func:`clamp`                           :func:`lte`                             :func:`to_utf8`
    :func:`codepoint`                       :func:`ltrim`                           :func:`transform`
    :func:`combinations`                    :func:`map`                             :func:`transform_keys`
    :func:`concat`                          :func:`map_concat`                      :func:`transform_values`
    :func:`contains`                        :func:`map_entries`                     :func:`trim`
    :func:`cos`                             :func:`map_filter`                      :func:`trim_array`
    :func:`cosh`                            :func:`map_from_entries`                :func:`truncate`
    :func:`crc32`                           :func:`map_keys`                        :func:`upper`
    :func:`current_date`                    :func:`map_values`                      :func:`url_decode`
    :func:`date`                            :func:`map_zip_with`                    :func:`url_encode`
    :func:`date_add`                        :func:`md5`                             :func:`url_extract_fragment`
    :func:`date_diff`                       :func:`millisecond`                     :func:`url_extract_host`
    :func:`date_format`                     :func:`minus`                           :func:`url_extract_parameter`
    :func:`date_parse`                      :func:`minute`                          :func:`url_extract_path`
    :func:`date_trunc`                      :func:`mod`                             :func:`url_extract_port`
    :func:`day`                             :func:`month`                           :func:`url_extract_protocol`
    :func:`day_of_month`                    :func:`multiply`                        :func:`url_extract_query`
    :func:`day_of_week`                     :func:`nan`                             :func:`week`
    :func:`day_of_year`                     :func:`negate`                          :func:`week_of_year`
    :func:`degrees`                         :func:`neq`                             :func:`width_bucket`
    :func:`distinct_from`                   :func:`none_match`                      :func:`xxhash64`
    :func:`divide`                          :func:`normal_cdf`                      :func:`year`
    :func:`dow`                             not                                     :func:`year_of_week`
    :func:`doy`                             :func:`parse_datetime`                  :func:`yow`
    :func:`e`                               :func:`pi`                              :func:`zip`
    :func:`element_at`                      :func:`plus`                            :func:`zip_with`
    :func:`empty_approx_set`                :func:`pow`
    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
