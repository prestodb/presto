***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/presto/math
    functions/presto/decimal
    functions/presto/bitwise
    functions/presto/comparison
    functions/presto/string
    functions/presto/datetime
    functions/presto/array
    functions/presto/map
    functions/presto/regexp
    functions/presto/binary
    functions/presto/json
    functions/presto/conversion
    functions/presto/url
    functions/presto/aggregate
    functions/presto/window
    functions/presto/hyperloglog
    functions/presto/tdigest
    functions/presto/qdigest
    functions/presto/geospatial
    functions/presto/ipaddress
    functions/presto/uuid
    functions/presto/misc

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

    ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
    Scalar Functions                                                                                                                  Aggregate Functions                           Window Functions
    ============================================================================================================================  ==  ========================================  ==  ========================================
    :func:`$internal$split_to_map`            :func:`format_datetime`                   :func:`plus`                                  :func:`any_value`                             :func:`cume_dist`
    :func:`abs`                               :func:`from_base`                         :func:`poisson_cdf`                           :func:`approx_distinct`                       :func:`dense_rank`
    :func:`acos`                              :func:`from_base64`                       :func:`pow`                                   :func:`approx_most_frequent`                  :func:`first_value`
    :func:`all_keys_match`                    :func:`from_base64url`                    :func:`power`                                 :func:`approx_percentile`                     :func:`lag`
    :func:`all_match`                         :func:`from_big_endian_32`                :func:`quarter`                               :func:`approx_set`                            :func:`last_value`
    :func:`any_keys_match`                    :func:`from_big_endian_64`                :func:`radians`                               :func:`arbitrary`                             :func:`lead`
    :func:`any_match`                         :func:`from_hex`                          :func:`rand`                                  :func:`array_agg`                             :func:`nth_value`
    :func:`any_values_match`                  :func:`from_ieee754_32`                   :func:`random`                                :func:`avg`                                   :func:`ntile`
    :func:`array_average`                     :func:`from_ieee754_64`                   :func:`reduce`                                :func:`bitwise_and_agg`                       :func:`percent_rank`
    :func:`array_constructor`                 :func:`from_iso8601_date`                 :func:`regexp_extract`                        :func:`bitwise_or_agg`                        :func:`rank`
    :func:`array_cum_sum`                     :func:`from_iso8601_timestamp`            :func:`regexp_extract_all`                    :func:`bitwise_xor_agg`                       :func:`row_number`
    :func:`array_distinct`                    :func:`from_unixtime`                     :func:`regexp_like`                           :func:`bool_and`
    :func:`array_duplicates`                  :func:`from_utf8`                         :func:`regexp_replace`                        :func:`bool_or`
    :func:`array_except`                      :func:`gamma_cdf`                         :func:`regexp_split`                          :func:`checksum`
    :func:`array_frequency`                   :func:`greatest`                          :func:`remove_nulls`                          :func:`classification_fall_out`
    :func:`array_has_duplicates`              :func:`gt`                                :func:`repeat`                                :func:`classification_miss_rate`
    :func:`array_intersect`                   :func:`gte`                               :func:`replace`                               :func:`classification_precision`
    :func:`array_join`                        :func:`hamming_distance`                  :func:`replace_first`                         :func:`classification_recall`
    :func:`array_max`                         :func:`hmac_md5`                          :func:`reverse`                               :func:`classification_thresholds`
    :func:`array_min`                         :func:`hmac_sha1`                         :func:`round`                                 :func:`corr`
    :func:`array_normalize`                   :func:`hmac_sha256`                       :func:`rpad`                                  :func:`count`
    :func:`array_position`                    :func:`hmac_sha512`                       :func:`rtrim`                                 :func:`count_if`
    :func:`array_remove`                      :func:`hour`                              :func:`second`                                :func:`covar_pop`
    :func:`array_sort`                        in                                        :func:`secure_rand`                           :func:`covar_samp`
    :func:`array_sort_desc`                   :func:`infinity`                          :func:`secure_random`                         :func:`entropy`
    :func:`array_sum`                         :func:`inverse_beta_cdf`                  :func:`sequence`                              :func:`every`
    :func:`array_sum_propagate_element_null`  :func:`inverse_cauchy_cdf`                :func:`sha1`                                  :func:`geometric_mean`
    :func:`array_union`                       :func:`inverse_laplace_cdf`               :func:`sha256`                                :func:`histogram`
    :func:`arrays_overlap`                    :func:`inverse_normal_cdf`                :func:`sha512`                                :func:`kurtosis`
    :func:`asin`                              :func:`inverse_weibull_cdf`               :func:`shuffle`                               :func:`map_agg`
    :func:`at_timezone`                       :func:`ip_prefix`                         :func:`sign`                                  :func:`map_union`
    :func:`atan`                              :func:`is_finite`                         :func:`sin`                                   :func:`map_union_sum`
    :func:`atan2`                             :func:`is_infinite`                       :func:`slice`                                 :func:`max`
    :func:`beta_cdf`                          :func:`is_json_scalar`                    :func:`split`                                 :func:`max_by`
    :func:`between`                           :func:`is_nan`                            :func:`split_part`                            :func:`max_data_size_for_stats`
    :func:`binomial_cdf`                      :func:`is_null`                           :func:`split_to_map`                          :func:`merge`
    :func:`bit_count`                         :func:`json_array_contains`               :func:`spooky_hash_v2_32`                     :func:`min`
    :func:`bitwise_and`                       :func:`json_array_get`                    :func:`spooky_hash_v2_64`                     :func:`min_by`
    :func:`bitwise_arithmetic_shift_right`    :func:`json_array_length`                 :func:`sqrt`                                  :func:`multimap_agg`
    :func:`bitwise_left_shift`                :func:`json_extract`                      :func:`starts_with`                           :func:`reduce_agg`
    :func:`bitwise_logical_shift_right`       :func:`json_extract_scalar`               :func:`strpos`                                :func:`regr_avgx`
    :func:`bitwise_not`                       :func:`json_format`                       :func:`strrpos`                               :func:`regr_avgy`
    :func:`bitwise_or`                        :func:`json_parse`                        :func:`subscript`                             :func:`regr_count`
    :func:`bitwise_right_shift`               :func:`json_size`                         :func:`substr`                                :func:`regr_intercept`
    :func:`bitwise_right_shift_arithmetic`    :func:`laplace_cdf`                       :func:`tan`                                   :func:`regr_r2`
    :func:`bitwise_shift_left`                :func:`last_day_of_month`                 :func:`tanh`                                  :func:`regr_slope`
    :func:`bitwise_xor`                       :func:`least`                             :func:`timezone_hour`                         :func:`regr_sxx`
    :func:`cardinality`                       :func:`length`                            :func:`timezone_minute`                       :func:`regr_sxy`
    :func:`cauchy_cdf`                        :func:`levenshtein_distance`              :func:`to_base`                               :func:`regr_syy`
    :func:`cbrt`                              :func:`like`                              :func:`to_base64`                             :func:`set_agg`
    :func:`ceil`                              :func:`ln`                                :func:`to_base64url`                          :func:`set_union`
    :func:`ceiling`                           :func:`log10`                             :func:`to_big_endian_32`                      :func:`skewness`
    :func:`chi_squared_cdf`                   :func:`log2`                              :func:`to_big_endian_64`                      :func:`stddev`
    :func:`chr`                               :func:`lower`                             :func:`to_hex`                                :func:`stddev_pop`
    :func:`clamp`                             :func:`lpad`                              :func:`to_ieee754_32`                         :func:`stddev_samp`
    :func:`codepoint`                         :func:`lt`                                :func:`to_ieee754_64`                         :func:`sum`
    :func:`combinations`                      :func:`lte`                               :func:`to_iso8601`                            :func:`sum_data_size_for_stats`
    :func:`concat`                            :func:`ltrim`                             :func:`to_milliseconds`                       :func:`var_pop`
    :func:`contains`                          :func:`map`                               :func:`to_unixtime`                           :func:`var_samp`
    :func:`cos`                               :func:`map_concat`                        :func:`to_utf8`                               :func:`variance`
    :func:`cosh`                              :func:`map_entries`                       :func:`trail`
    :func:`cosine_similarity`                 :func:`map_filter`                        :func:`transform`
    :func:`crc32`                             :func:`map_from_entries`                  :func:`transform_keys`
    :func:`current_date`                      :func:`map_key_exists`                    :func:`transform_values`
    :func:`date`                              :func:`map_keys`                          :func:`trim`
    :func:`date_add`                          :func:`map_normalize`                     :func:`trim_array`
    :func:`date_diff`                         :func:`map_remove_null_values`            :func:`truncate`
    :func:`date_format`                       :func:`map_subset`                        :func:`typeof`
    :func:`date_parse`                        :func:`map_top_n`                         :func:`upper`
    :func:`date_trunc`                        :func:`map_top_n_keys`                    :func:`url_decode`
    :func:`day`                               :func:`map_values`                        :func:`url_encode`
    :func:`day_of_month`                      :func:`map_zip_with`                      :func:`url_extract_fragment`
    :func:`day_of_week`                       :func:`md5`                               :func:`url_extract_host`
    :func:`day_of_year`                       :func:`millisecond`                       :func:`url_extract_parameter`
    :func:`degrees`                           :func:`minus`                             :func:`url_extract_path`
    :func:`distinct_from`                     :func:`minute`                            :func:`url_extract_port`
    :func:`divide`                            :func:`mod`                               :func:`url_extract_protocol`
    :func:`dow`                               :func:`month`                             :func:`url_extract_query`
    :func:`doy`                               :func:`multimap_from_entries`             :func:`uuid`
    :func:`e`                                 :func:`multiply`                          :func:`week`
    :func:`element_at`                        :func:`nan`                               :func:`week_of_year`
    :func:`empty_approx_set`                  :func:`negate`                            :func:`weibull_cdf`
    :func:`ends_with`                         :func:`neq`                               :func:`width_bucket`
    :func:`eq`                                :func:`ngrams`                            :func:`wilson_interval_lower`
    :func:`exp`                               :func:`no_keys_match`                     :func:`wilson_interval_upper`
    :func:`f_cdf`                             :func:`no_values_match`                   :func:`word_stem`
    :func:`fail`                              :func:`none_match`                        :func:`xxhash64`
    :func:`filter`                            :func:`normal_cdf`                        :func:`year`
    :func:`find_first`                        :func:`normalize`                         :func:`year_of_week`
    :func:`find_first_index`                  not                                       :func:`yow`
    :func:`flatten`                           :func:`parse_datetime`                    :func:`zip`
    :func:`floor`                             :func:`pi`                                :func:`zip_with`
    ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
