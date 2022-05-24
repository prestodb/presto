=================
Function Coverage
=================

Here is a list of all scalar and aggregate Presto functions with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(2) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions
    ================================================================================================================================================================================================================  ==  ========================================
    :func:`abs`                               :func:`date_diff`                         is_json_scalar                            render                                    st_points                                     :func:`approx_distinct`
    :func:`acos`                              :func:`date_format`                       :func:`is_nan`                            repeat                                    st_polygon                                    approx_most_frequent
    all_match                                 :func:`date_parse`                        is_subnet_of                              :func:`replace`                           st_relate                                     :func:`approx_percentile`
    any_match                                 :func:`date_trunc`                        jaccard_index                             :func:`reverse`                           st_startpoint                                 :func:`approx_set`
    array_average                             :func:`day`                               json_array_contains                       rgb                                       st_symdifference                              :func:`arbitrary`
    :func:`array_distinct`                    :func:`day_of_month`                      json_array_get                            :func:`round`                             st_touches                                    :func:`array_agg`
    :func:`array_duplicates`                  :func:`day_of_week`                       json_array_length                         :func:`rpad`                              st_union                                      :func:`avg`
    :func:`array_except`                      :func:`day_of_year`                       json_extract                              :func:`rtrim`                             st_within                                     :func:`bitwise_and_agg`
    array_frequency                           degrees                                   :func:`json_extract_scalar`               scale_qdigest                             st_x                                          :func:`bitwise_or_agg`
    array_has_duplicates                      :func:`dow`                               json_format                               :func:`second`                            st_xmax                                       :func:`bool_and`
    :func:`array_intersect`                   :func:`doy`                               json_parse                                sequence                                  st_xmin                                       :func:`bool_or`
    :func:`array_join`                        e                                         json_size                                 sha1                                      st_y                                          :func:`checksum`
    :func:`array_max`                         :func:`element_at`                        :func:`least`                             :func:`sha256`                            st_ymax                                       classification_fall_out
    :func:`array_min`                         :func:`empty_approx_set`                  :func:`length`                            sha512                                    st_ymin                                       classification_miss_rate
    array_normalize                           enum_key                                  levenshtein_distance                      shuffle                                   :func:`strpos`                                classification_precision
    :func:`array_position`                    :func:`exp`                               line_interpolate_point                    :func:`sign`                              strrpos                                       classification_recall
    array_remove                              expand_envelope                           line_locate_point                         simplify_geometry                         :func:`substr`                                classification_thresholds
    array_sort                                features                                  :func:`ln`                                :func:`sin`                               :func:`tan`                                   convex_hull_agg
    array_sum                                 :func:`filter`                            localtime                                 :func:`slice`                             :func:`tanh`                                  :func:`corr`
    array_union                               flatten                                   localtimestamp                            spatial_partitions                        timezone_hour                                 :func:`count`
    :func:`arrays_overlap`                    flatten_geometry_collections              :func:`log10`                             :func:`split`                             timezone_minute                               :func:`count_if`
    :func:`asin`                              :func:`floor`                             :func:`log2`                              :func:`split_part`                        :func:`to_base`                               :func:`covar_pop`
    :func:`atan`                              fnv1_32                                   :func:`lower`                             split_to_map                              :func:`to_base64`                             :func:`covar_samp`
    :func:`atan2`                             fnv1_64                                   :func:`lpad`                              split_to_multimap                         to_base64url                                  differential_entropy
    bar                                       fnv1a_32                                  :func:`ltrim`                             spooky_hash_v2_32                         to_big_endian_32                              entropy
    beta_cdf                                  fnv1a_64                                  :func:`map`                               spooky_hash_v2_64                         to_big_endian_64                              evaluate_classifier_predictions
    bing_tile                                 format_datetime                           :func:`map_concat`                        :func:`sqrt`                              to_geometry                                   :func:`every`
    bing_tile_at                              :func:`from_base`                         :func:`map_entries`                       st_area                                   :func:`to_hex`                                geometric_mean
    bing_tile_children                        :func:`from_base64`                       :func:`map_filter`                        st_asbinary                               to_ieee754_32                                 geometry_union_agg
    bing_tile_coordinates                     from_base64url                            map_from_entries                          st_astext                                 to_ieee754_64                                 :func:`histogram`
    bing_tile_parent                          from_big_endian_32                        :func:`map_keys`                          st_boundary                               to_iso8601                                    khyperloglog_agg
    bing_tile_polygon                         from_big_endian_64                        map_normalize                             st_buffer                                 to_milliseconds                               kurtosis
    bing_tile_quadkey                         :func:`from_hex`                          :func:`map_values`                        st_centroid                               to_spherical_geography                        learn_classifier
    bing_tile_zoom_level                      from_ieee754_32                           map_zip_with                              st_contains                               :func:`to_unixtime`                           learn_libsvm_classifier
    bing_tiles_around                         from_ieee754_64                           :func:`md5`                               st_convexhull                             :func:`to_utf8`                               learn_libsvm_regressor
    binomial_cdf                              from_iso8601_date                         merge_hll                                 st_coorddim                               :func:`transform`                             learn_regressor
    :func:`bit_count`                         from_iso8601_timestamp                    merge_khll                                st_crosses                                transform_keys                                make_set_digest
    :func:`bitwise_and`                       :func:`from_unixtime`                     :func:`millisecond`                       st_difference                             transform_values                              :func:`map_agg`
    :func:`bitwise_arithmetic_shift_right`    from_utf8                                 :func:`minute`                            st_dimension                              :func:`trim`                                  map_union
    :func:`bitwise_left_shift`                geometry_as_geojson                       :func:`mod`                               st_disjoint                               truncate                                      map_union_sum
    :func:`bitwise_logical_shift_right`       geometry_from_geojson                     :func:`month`                             st_distance                               typeof                                        :func:`max`
    :func:`bitwise_not`                       geometry_invalid_reason                   multimap_from_entries                     st_endpoint                               uniqueness_distribution                       :func:`max_by`
    :func:`bitwise_or`                        geometry_nearest_points                   myanmar_font_encoding                     st_envelope                               :func:`upper`                                 :func:`merge`
    :func:`bitwise_right_shift`               geometry_to_bing_tiles                    myanmar_normalize_unicode                 st_envelopeaspts                          :func:`url_decode`                            merge_set_digest
    :func:`bitwise_right_shift_arithmetic`    geometry_to_dissolved_bing_tiles          :func:`nan`                               st_equals                                 :func:`url_encode`                            :func:`min`
    :func:`bitwise_shift_left`                geometry_union                            ngrams                                    st_exteriorring                           :func:`url_extract_fragment`                  :func:`min_by`
    :func:`bitwise_xor`                       great_circle_distance                     none_match                                st_geometries                             :func:`url_extract_host`                      multimap_agg
    :func:`cardinality`                       :func:`greatest`                          normal_cdf                                st_geometryfromtext                       :func:`url_extract_parameter`                 numeric_histogram
    cauchy_cdf                                hamming_distance                          normalize                                 st_geometryn                              :func:`url_extract_path`                      qdigest_agg
    :func:`cbrt`                              hash_counts                               now                                       st_geometrytype                           :func:`url_extract_port`                      reduce_agg
    :func:`ceil`                              hmac_md5                                  :func:`parse_datetime`                    st_geomfrombinary                         :func:`url_extract_protocol`                  regr_intercept
    :func:`ceiling`                           hmac_sha1                                 parse_duration                            st_interiorringn                          :func:`url_extract_query`                     regr_slope
    chi_squared_cdf                           hmac_sha256                               parse_presto_data_size                    st_interiorrings                          value_at_quantile                             set_agg
    :func:`chr`                               hmac_sha512                               :func:`pi`                                st_intersection                           values_at_quantiles                           set_union
    classify                                  :func:`hour`                              poisson_cdf                               st_intersects                             week                                          skewness
    :func:`codepoint`                         :func:`infinity`                          :func:`pow`                               st_isclosed                               week_of_year                                  spatial_partitioning
    color                                     intersection_cardinality                  :func:`power`                             st_isempty                                weibull_cdf                                   :func:`stddev`
    combinations                              inverse_beta_cdf                          quantile_at_value                         st_isring                                 :func:`width_bucket`                          :func:`stddev_pop`
    :func:`concat`                            inverse_binomial_cdf                      :func:`quarter`                           st_issimple                               wilson_interval_lower                         :func:`stddev_samp`
    :func:`contains`                          inverse_cauchy_cdf                        :func:`radians`                           st_isvalid                                wilson_interval_upper                         :func:`sum`
    :func:`cos`                               inverse_chi_squared_cdf                   :func:`rand`                              st_length                                 word_stem                                     tdigest_agg
    :func:`cosh`                              inverse_normal_cdf                        :func:`random`                            st_linefromtext                           :func:`xxhash64`                              :func:`var_pop`
    cosine_similarity                         inverse_poisson_cdf                       :func:`reduce`                            st_linestring                             :func:`year`                                  :func:`var_samp`
    crc32                                     inverse_weibull_cdf                       :func:`regexp_extract`                    st_multipoint                             :func:`year_of_week`                          :func:`variance`
    current_date                              ip_prefix                                 :func:`regexp_extract_all`                st_numgeometries                          :func:`yow`
    current_time                              ip_subnet_max                             :func:`regexp_like`                       st_numinteriorring                        :func:`zip`
    current_timestamp                         ip_subnet_min                             :func:`regexp_replace`                    st_numpoints                              zip_with
    current_timezone                          ip_subnet_range                           regexp_split                              st_overlaps
    date                                      :func:`is_finite`                         regress                                   st_point
    :func:`date_add`                          :func:`is_infinite`                       reidentification_potential                st_pointn
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================
