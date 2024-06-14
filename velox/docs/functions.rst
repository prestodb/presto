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

    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
    Scalar Functions                                                                                                            Aggregate Functions                         Window Functions
    ======================================================================================================================  ==  ======================================  ==  ======================================
    :func:`abs`                             :func:`find_first_index`                :func:`plus`                                :func:`any_value`                           :func:`cume_dist`
    :func:`acos`                            :func:`flatten`                         :func:`poisson_cdf`                         :func:`approx_distinct`                     :func:`dense_rank`
    :func:`all_keys_match`                  :func:`floor`                           :func:`pow`                                 :func:`approx_most_frequent`                :func:`first_value`
    :func:`all_match`                       :func:`format_datetime`                 :func:`power`                               :func:`approx_percentile`                   :func:`lag`
    :func:`any_keys_match`                  :func:`from_base`                       :func:`quarter`                             :func:`approx_set`                          :func:`last_value`
    :func:`any_match`                       :func:`from_base64`                     :func:`radians`                             :func:`arbitrary`                           :func:`lead`
    :func:`any_values_match`                :func:`from_base64url`                  :func:`rand`                                :func:`array_agg`                           :func:`nth_value`
    :func:`array_average`                   :func:`from_big_endian_32`              :func:`random`                              :func:`avg`                                 :func:`ntile`
    :func:`array_constructor`               :func:`from_big_endian_64`              :func:`reduce`                              :func:`bitwise_and_agg`                     :func:`percent_rank`
    :func:`array_distinct`                  :func:`from_hex`                        :func:`regexp_extract`                      :func:`bitwise_or_agg`                      :func:`rank`
    :func:`array_duplicates`                :func:`from_ieee754_32`                 :func:`regexp_extract_all`                  :func:`bitwise_xor_agg`                     :func:`row_number`
    :func:`array_except`                    :func:`from_ieee754_64`                 :func:`regexp_like`                         :func:`bool_and`
    :func:`array_frequency`                 :func:`from_iso8601_date`               :func:`regexp_replace`                      :func:`bool_or`
    :func:`array_has_duplicates`            :func:`from_unixtime`                   :func:`remove_nulls`                        :func:`checksum`
    :func:`array_intersect`                 :func:`from_utf8`                       :func:`repeat`                              :func:`corr`
    :func:`array_join`                      :func:`gamma_cdf`                       :func:`replace`                             :func:`count`
    :func:`array_max`                       :func:`greatest`                        :func:`reverse`                             :func:`count_if`
    :func:`array_min`                       :func:`gt`                              :func:`round`                               :func:`covar_pop`
    :func:`array_normalize`                 :func:`gte`                             :func:`rpad`                                :func:`covar_samp`
    :func:`array_position`                  :func:`hamming_distance`                :func:`rtrim`                               :func:`entropy`
    :func:`array_remove`                    :func:`hmac_md5`                        :func:`second`                              :func:`every`
    :func:`array_sort`                      :func:`hmac_sha1`                       :func:`sequence`                            :func:`geometric_mean`
    :func:`array_sort_desc`                 :func:`hmac_sha256`                     :func:`sha1`                                :func:`histogram`
    :func:`array_sum`                       :func:`hmac_sha512`                     :func:`sha256`                              :func:`kurtosis`
    :func:`array_union`                     :func:`hour`                            :func:`sha512`                              :func:`map_agg`
    :func:`arrays_overlap`                  in                                      :func:`shuffle`                             :func:`map_union`
    :func:`asin`                            :func:`infinity`                        :func:`sign`                                :func:`map_union_sum`
    :func:`atan`                            :func:`inverse_beta_cdf`                :func:`sin`                                 :func:`max`
    :func:`atan2`                           :func:`is_finite`                       :func:`slice`                               :func:`max_by`
    :func:`beta_cdf`                        :func:`is_infinite`                     :func:`split`                               :func:`max_data_size_for_stats`
    :func:`between`                         :func:`is_json_scalar`                  :func:`split_part`                          :func:`merge`
    :func:`binomial_cdf`                    :func:`is_nan`                          :func:`split_to_map`                        :func:`min`
    :func:`bit_count`                       :func:`is_null`                         :func:`spooky_hash_v2_32`                   :func:`min_by`
    :func:`bitwise_and`                     :func:`json_array_contains`             :func:`spooky_hash_v2_64`                   :func:`multimap_agg`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_array_length`               :func:`sqrt`                                :func:`reduce_agg`
    :func:`bitwise_left_shift`              :func:`json_extract`                    :func:`starts_with`                         :func:`regr_avgx`
    :func:`bitwise_logical_shift_right`     :func:`json_extract_scalar`             :func:`strpos`                              :func:`regr_avgy`
    :func:`bitwise_not`                     :func:`json_format`                     :func:`strrpos`                             :func:`regr_count`
    :func:`bitwise_or`                      :func:`json_parse`                      :func:`subscript`                           :func:`regr_intercept`
    :func:`bitwise_right_shift`             :func:`json_size`                       :func:`substr`                              :func:`regr_r2`
    :func:`bitwise_right_shift_arithmetic`  :func:`laplace_cdf`                     :func:`tan`                                 :func:`regr_slope`
    :func:`bitwise_shift_left`              :func:`last_day_of_month`               :func:`tanh`                                :func:`regr_sxx`
    :func:`bitwise_xor`                     :func:`least`                           :func:`timezone_hour`                       :func:`regr_sxy`
    :func:`cardinality`                     :func:`length`                          :func:`timezone_minute`                     :func:`regr_syy`
    :func:`cauchy_cdf`                      :func:`levenshtein_distance`            :func:`to_base`                             :func:`set_agg`
    :func:`cbrt`                            :func:`like`                            :func:`to_base64`                           :func:`set_union`
    :func:`ceil`                            :func:`ln`                              :func:`to_base64url`                        :func:`skewness`
    :func:`ceiling`                         :func:`log10`                           :func:`to_big_endian_32`                    :func:`stddev`
    :func:`chi_squared_cdf`                 :func:`log2`                            :func:`to_big_endian_64`                    :func:`stddev_pop`
    :func:`chr`                             :func:`lower`                           :func:`to_hex`                              :func:`stddev_samp`
    :func:`clamp`                           :func:`lpad`                            :func:`to_ieee754_32`                       :func:`sum`
    :func:`codepoint`                       :func:`lt`                              :func:`to_ieee754_64`                       :func:`sum_data_size_for_stats`
    :func:`combinations`                    :func:`lte`                             :func:`to_unixtime`                         :func:`var_pop`
    :func:`concat`                          :func:`ltrim`                           :func:`to_utf8`                             :func:`var_samp`
    :func:`contains`                        :func:`map`                             :func:`transform`                           :func:`variance`
    :func:`cos`                             :func:`map_concat`                      :func:`transform_keys`
    :func:`cosh`                            :func:`map_entries`                     :func:`transform_values`
    :func:`cosine_similarity`               :func:`map_filter`                      :func:`trim`
    :func:`crc32`                           :func:`map_from_entries`                :func:`trim_array`
    :func:`current_date`                    :func:`map_keys`                        :func:`truncate`
    :func:`date`                            :func:`map_normalize`                   :func:`typeof`
    :func:`date_add`                        :func:`map_subset`                      :func:`upper`
    :func:`date_diff`                       :func:`map_top_n`                       :func:`url_decode`
    :func:`date_format`                     :func:`map_values`                      :func:`url_encode`
    :func:`date_parse`                      :func:`map_zip_with`                    :func:`url_extract_fragment`
    :func:`date_trunc`                      :func:`md5`                             :func:`url_extract_host`
    :func:`day`                             :func:`millisecond`                     :func:`url_extract_parameter`
    :func:`day_of_month`                    :func:`minus`                           :func:`url_extract_path`
    :func:`day_of_week`                     :func:`minute`                          :func:`url_extract_port`
    :func:`day_of_year`                     :func:`mod`                             :func:`url_extract_protocol`
    :func:`degrees`                         :func:`month`                           :func:`url_extract_query`
    :func:`distinct_from`                   :func:`multimap_from_entries`           :func:`week`
    :func:`divide`                          :func:`multiply`                        :func:`week_of_year`
    :func:`dow`                             :func:`nan`                             :func:`weibull_cdf`
    :func:`doy`                             :func:`negate`                          :func:`width_bucket`
    :func:`e`                               :func:`neq`                             :func:`wilson_interval_lower`
    :func:`element_at`                      :func:`ngrams`                          :func:`wilson_interval_upper`
    :func:`empty_approx_set`                :func:`no_keys_match`                   :func:`xxhash64`
    :func:`ends_with`                       :func:`no_values_match`                 :func:`year`
    :func:`eq`                              :func:`none_match`                      :func:`year_of_week`
    :func:`exp`                             :func:`normal_cdf`                      :func:`yow`
    :func:`f_cdf`                           not                                     :func:`zip`
    :func:`filter`                          :func:`parse_datetime`                  :func:`zip_with`
    :func:`find_first`                      :func:`pi`
    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
