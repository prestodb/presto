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
    functions/presto/enum
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

    =================================================  =================================================  =================================================  ==  =================================================  ==  =================================================
    Scalar Functions                                                                                                                                             Aggregate Functions                                    Window Functions
    =======================================================================================================================================================  ==  =================================================  ==  =================================================
    :func:`$internal$json_string_to_array_cast`        :func:`gt`                                         :func:`sequence`                                       :func:`any_value`                                      :func:`cume_dist`
    :func:`$internal$json_string_to_map_cast`          :func:`gte`                                        :func:`sha1`                                           :func:`approx_distinct`                                :func:`dense_rank`
    :func:`$internal$json_string_to_row_cast`          :func:`hamming_distance`                           :func:`sha256`                                         :func:`approx_most_frequent`                           :func:`first_value`
    :func:`$internal$split_to_map`                     :func:`hmac_md5`                                   :func:`sha512`                                         :func:`approx_percentile`                              :func:`lag`
    :func:`abs`                                        :func:`hmac_sha1`                                  :func:`shuffle`                                        :func:`approx_set`                                     :func:`last_value`
    :func:`acos`                                       :func:`hmac_sha256`                                :func:`sign`                                           :func:`arbitrary`                                      :func:`lead`
    :func:`all_keys_match`                             :func:`hmac_sha512`                                :func:`simplify_geometry`                              :func:`array_agg`                                      :func:`nth_value`
    :func:`all_match`                                  :func:`hour`                                       :func:`sin`                                            :func:`avg`                                            :func:`ntile`
    :func:`any_keys_match`                             in                                                 :func:`slice`                                          :func:`bitwise_and_agg`                                :func:`percent_rank`
    :func:`any_match`                                  :func:`infinity`                                   :func:`split`                                          :func:`bitwise_or_agg`                                 :func:`rank`
    :func:`any_values_match`                           :func:`inverse_beta_cdf`                           :func:`split_part`                                     :func:`bitwise_xor_agg`                                :func:`row_number`
    :func:`array_average`                              :func:`inverse_binomial_cdf`                       :func:`split_to_map`                                   :func:`bool_and`
    :func:`array_constructor`                          :func:`inverse_cauchy_cdf`                         :func:`split_to_multimap`                              :func:`bool_or`
    :func:`array_cum_sum`                              :func:`inverse_chi_squared_cdf`                    :func:`spooky_hash_v2_32`                              :func:`checksum`
    :func:`array_distinct`                             :func:`inverse_f_cdf`                              :func:`spooky_hash_v2_64`                              :func:`classification_fall_out`
    :func:`array_duplicates`                           :func:`inverse_gamma_cdf`                          :func:`sqrt`                                           :func:`classification_miss_rate`
    :func:`array_except`                               :func:`inverse_laplace_cdf`                        :func:`st_area`                                        :func:`classification_precision`
    :func:`array_frequency`                            :func:`inverse_normal_cdf`                         :func:`st_asbinary`                                    :func:`classification_recall`
    :func:`array_has_duplicates`                       :func:`inverse_poisson_cdf`                        :func:`st_astext`                                      :func:`classification_thresholds`
    :func:`array_intersect`                            :func:`inverse_weibull_cdf`                        :func:`st_boundary`                                    :func:`corr`
    :func:`array_join`                                 :func:`ip_prefix`                                  :func:`st_buffer`                                      :func:`count`
    :func:`array_max`                                  :func:`ip_prefix_collapse`                         :func:`st_centroid`                                    :func:`count_if`
    :func:`array_max_by`                               :func:`ip_prefix_subnets`                          :func:`st_contains`                                    :func:`covar_pop`
    :func:`array_min`                                  :func:`ip_subnet_max`                              :func:`st_convexhull`                                  :func:`covar_samp`
    :func:`array_min_by`                               :func:`ip_subnet_min`                              :func:`st_coorddim`                                    :func:`entropy`
    :func:`array_normalize`                            :func:`ip_subnet_range`                            :func:`st_crosses`                                     :func:`every`
    :func:`array_position`                             :func:`is_finite`                                  :func:`st_difference`                                  :func:`geometric_mean`
    :func:`array_remove`                               :func:`is_infinite`                                :func:`st_dimension`                                   :func:`histogram`
    :func:`array_sort`                                 :func:`is_json_scalar`                             :func:`st_disjoint`                                    :func:`kurtosis`
    :func:`array_sort_desc`                            :func:`is_nan`                                     :func:`st_distance`                                    :func:`map_agg`
    :func:`array_sum`                                  :func:`is_null`                                    :func:`st_endpoint`                                    :func:`map_union`
    :func:`array_sum_propagate_element_null`           :func:`is_private_ip`                              :func:`st_envelope`                                    :func:`map_union_sum`
    :func:`array_top_n`                                :func:`is_subnet_of`                               :func:`st_envelopeaspts`                               :func:`max`
    :func:`array_union`                                :func:`json_array_contains`                        :func:`st_equals`                                      :func:`max_by`
    :func:`arrays_overlap`                             :func:`json_array_get`                             :func:`st_exteriorring`                                :func:`max_data_size_for_stats`
    :func:`asin`                                       :func:`json_array_length`                          :func:`st_geometries`                                  :func:`merge`
    :func:`at_timezone`                                :func:`json_extract`                               :func:`st_geometryfromtext`                            :func:`min`
    :func:`atan`                                       :func:`json_extract_scalar`                        :func:`st_geometryn`                                   :func:`min_by`
    :func:`atan2`                                      :func:`json_format`                                :func:`st_geometrytype`                                :func:`multimap_agg`
    :func:`beta_cdf`                                   :func:`json_parse`                                 :func:`st_geomfrombinary`                              :func:`noisy_approx_distinct_sfm`
    :func:`between`                                    :func:`json_size`                                  :func:`st_interiorringn`                               :func:`noisy_approx_set_sfm`
    :func:`bing_tile`                                  :func:`laplace_cdf`                                :func:`st_interiorrings`                               :func:`noisy_approx_set_sfm_from_index_and_zeros`
    :func:`bing_tile_at`                               :func:`last_day_of_month`                          :func:`st_intersection`                                :func:`noisy_avg_gaussian`
    :func:`bing_tile_children`                         :func:`least`                                      :func:`st_intersects`                                  :func:`noisy_count_gaussian`
    :func:`bing_tile_coordinates`                      :func:`length`                                     :func:`st_isclosed`                                    :func:`noisy_count_if_gaussian`
    :func:`bing_tile_parent`                           :func:`levenshtein_distance`                       :func:`st_isempty`                                     :func:`noisy_sum_gaussian`
    :func:`bing_tile_quadkey`                          :func:`like`                                       :func:`st_isring`                                      :func:`numeric_histogram`
    :func:`bing_tile_zoom_level`                       :func:`line_interpolate_point`                     :func:`st_issimple`                                    :func:`qdigest_agg`
    :func:`bing_tiles_around`                          :func:`line_locate_point`                          :func:`st_isvalid`                                     :func:`reduce_agg`
    :func:`binomial_cdf`                               :func:`ln`                                         :func:`st_length`                                      :func:`regr_avgx`
    :func:`bit_count`                                  :func:`log10`                                      :func:`st_numgeometries`                               :func:`regr_avgy`
    :func:`bitwise_and`                                :func:`log2`                                       :func:`st_numinteriorring`                             :func:`regr_count`
    :func:`bitwise_arithmetic_shift_right`             :func:`lower`                                      :func:`st_numpoints`                                   :func:`regr_intercept`
    :func:`bitwise_left_shift`                         :func:`lpad`                                       :func:`st_overlaps`                                    :func:`regr_r2`
    :func:`bitwise_logical_shift_right`                :func:`lt`                                         :func:`st_point`                                       :func:`regr_slope`
    :func:`bitwise_not`                                :func:`lte`                                        :func:`st_pointn`                                      :func:`regr_sxx`
    :func:`bitwise_or`                                 :func:`ltrim`                                      :func:`st_points`                                      :func:`regr_sxy`
    :func:`bitwise_right_shift`                        :func:`map`                                        :func:`st_polygon`                                     :func:`regr_syy`
    :func:`bitwise_right_shift_arithmetic`             :func:`map_concat`                                 :func:`st_relate`                                      :func:`set_agg`
    :func:`bitwise_shift_left`                         :func:`map_entries`                                :func:`st_startpoint`                                  :func:`set_union`
    :func:`bitwise_xor`                                :func:`map_filter`                                 :func:`st_symdifference`                               :func:`skewness`
    :func:`cardinality`                                :func:`map_from_entries`                           :func:`st_touches`                                     :func:`stddev`
    :func:`cauchy_cdf`                                 :func:`map_key_exists`                             :func:`st_union`                                       :func:`stddev_pop`
    :func:`cbrt`                                       :func:`map_keys`                                   :func:`st_within`                                      :func:`stddev_samp`
    :func:`ceil`                                       :func:`map_keys_by_top_n_values`                   :func:`st_x`                                           :func:`sum`
    :func:`ceiling`                                    :func:`map_normalize`                              :func:`st_xmax`                                        :func:`sum_data_size_for_stats`
    :func:`chi_squared_cdf`                            :func:`map_remove_null_values`                     :func:`st_xmin`                                        :func:`tdigest_agg`
    :func:`chr`                                        :func:`map_subset`                                 :func:`st_y`                                           :func:`var_pop`
    :func:`clamp`                                      :func:`map_top_n`                                  :func:`st_ymax`                                        :func:`var_samp`
    :func:`codepoint`                                  :func:`map_top_n_keys`                             :func:`st_ymin`                                        :func:`variance`
    :func:`combinations`                               :func:`map_top_n_values`                           :func:`starts_with`
    :func:`combine_hash_internal`                      :func:`map_values`                                 :func:`strpos`
    :func:`concat`                                     :func:`map_zip_with`                               :func:`strrpos`
    :func:`construct_tdigest`                          :func:`md5`                                        :func:`subscript`
    :func:`contains`                                   :func:`merge_sfm`                                  :func:`substr`
    :func:`cos`                                        :func:`merge_tdigest`                              :func:`substring`
    :func:`cosh`                                       :func:`millisecond`                                :func:`tan`
    :func:`cosine_similarity`                          :func:`minus`                                      :func:`tanh`
    :func:`crc32`                                      :func:`minute`                                     :func:`timezone_hour`
    :func:`current_date`                               :func:`mod`                                        :func:`timezone_minute`
    :func:`date`                                       :func:`month`                                      :func:`to_base`
    :func:`date_add`                                   :func:`multimap_from_entries`                      :func:`to_base64`
    :func:`date_diff`                                  :func:`multiply`                                   :func:`to_base64url`
    :func:`date_format`                                :func:`murmur3_x64_128`                            :func:`to_big_endian_32`
    :func:`date_parse`                                 :func:`nan`                                        :func:`to_big_endian_64`
    :func:`date_trunc`                                 :func:`negate`                                     :func:`to_hex`
    :func:`day`                                        :func:`neq`                                        :func:`to_ieee754_32`
    :func:`day_of_month`                               :func:`ngrams`                                     :func:`to_ieee754_64`
    :func:`day_of_week`                                :func:`no_keys_match`                              :func:`to_iso8601`
    :func:`day_of_year`                                :func:`no_values_match`                            :func:`to_milliseconds`
    :func:`degrees`                                    :func:`noisy_empty_approx_set_sfm`                 :func:`to_unixtime`
    :func:`destructure_tdigest`                        :func:`none_match`                                 :func:`to_utf8`
    :func:`distinct_from`                              :func:`normal_cdf`                                 :func:`trail`
    :func:`divide`                                     :func:`normalize`                                  :func:`transform`
    :func:`dot_product`                                not                                                :func:`transform_keys`
    :func:`dow`                                        :func:`parse_datetime`                             :func:`transform_values`
    :func:`doy`                                        :func:`parse_duration`                             :func:`trim`
    :func:`e`                                          :func:`parse_presto_data_size`                     :func:`trim_array`
    :func:`element_at`                                 :func:`pi`                                         :func:`trimmed_mean`
    :func:`empty_approx_set`                           :func:`plus`                                       :func:`truncate`
    :func:`ends_with`                                  :func:`poisson_cdf`                                :func:`typeof`
    :func:`eq`                                         :func:`pow`                                        :func:`upper`
    :func:`exp`                                        :func:`power`                                      :func:`url_decode`
    :func:`f_cdf`                                      :func:`quantile_at_value`                          :func:`url_encode`
    :func:`fail`                                       :func:`quantiles_at_values`                        :func:`url_extract_fragment`
    :func:`filter`                                     :func:`quarter`                                    :func:`url_extract_host`
    :func:`find_first`                                 :func:`radians`                                    :func:`url_extract_parameter`
    :func:`find_first_index`                           :func:`rand`                                       :func:`url_extract_path`
    :func:`flatten`                                    :func:`random`                                     :func:`url_extract_port`
    :func:`flatten_geometry_collections`               :func:`reduce`                                     :func:`url_extract_protocol`
    :func:`floor`                                      :func:`regexp_extract`                             :func:`url_extract_query`
    :func:`format_datetime`                            :func:`regexp_extract_all`                         :func:`uuid`
    :func:`from_base`                                  :func:`regexp_like`                                :func:`value_at_quantile`
    :func:`from_base64`                                :func:`regexp_replace`                             :func:`values_at_quantiles`
    :func:`from_base64url`                             :func:`regexp_split`                               :func:`week`
    :func:`from_big_endian_32`                         :func:`remove_nulls`                               :func:`week_of_year`
    :func:`from_big_endian_64`                         :func:`repeat`                                     :func:`weibull_cdf`
    :func:`from_hex`                                   :func:`replace`                                    :func:`width_bucket`
    :func:`from_ieee754_32`                            :func:`replace_first`                              :func:`wilson_interval_lower`
    :func:`from_ieee754_64`                            :func:`reverse`                                    :func:`wilson_interval_upper`
    :func:`from_iso8601_date`                          :func:`round`                                      :func:`word_stem`
    :func:`from_iso8601_timestamp`                     :func:`rpad`                                       :func:`xxhash64`
    :func:`from_unixtime`                              :func:`rtrim`                                      :func:`xxhash64_internal`
    :func:`from_utf8`                                  :func:`scale_qdigest`                              :func:`year`
    :func:`gamma_cdf`                                  :func:`scale_tdigest`                              :func:`year_of_week`
    :func:`geometry_invalid_reason`                    :func:`second`                                     :func:`yow`
    :func:`geometry_nearest_points`                    :func:`secure_rand`                                :func:`zip`
    :func:`greatest`                                   :func:`secure_random`                              :func:`zip_with`
    =================================================  =================================================  =================================================  ==  =================================================  ==  =================================================
