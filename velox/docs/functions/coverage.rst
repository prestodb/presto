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
    table.coverage tr:nth-child(1) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(3) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions
    ================================================================================================================================================================================================================  ==  ========================================
    :func:`abs`                               current_timestamp                         ip_subnet_range                           regress                                   st_pointn                                     :func:`approx_distinct`
    acos                                      current_timezone                          is_finite                                 reidentification_potential                st_points                                     approx_most_frequent
    all_match                                 date                                      is_infinite                               render                                    st_polygon                                    :func:`approx_percentile`
    any_match                                 date_add                                  is_json_scalar                            repeat                                    st_relate                                     approx_set
    array_average                             date_diff                                 is_nan                                    :func:`replace`                           st_startpoint                                 :func:`arbitrary`
    array_distinct                            date_format                               is_subnet_of                              reverse                                   st_symdifference                              :func:`array_agg`
    array_dupes                               date_parse                                jaccard_index                             rgb                                       st_touches                                    :func:`avg`
    :func:`array_except`                      date_trunc                                json_array_contains                       :func:`round`                             st_union                                      :func:`bitwise_and_agg`
    array_frequency                           day                                       json_array_get                            rpad                                      st_within                                     :func:`bitwise_or_agg`
    array_has_dupes                           day_of_month                              json_array_length                         rtrim                                     st_x                                          :func:`bool_and`
    :func:`array_intersect`                   day_of_week                               json_extract                              scale_qdigest                             st_xmax                                       :func:`bool_or`
    array_join                                day_of_year                               :func:`json_extract_scalar`               second                                    st_xmin                                       checksum
    :func:`array_max`                         degrees                                   json_format                               sequence                                  st_y                                          classification_fall_out
    :func:`array_min`                         dow                                       json_parse                                sha1                                      st_ymax                                       classification_miss_rate
    array_normalize                           doy                                       json_size                                 sha256                                    st_ymin                                       classification_precision
    array_position                            e                                         least                                     sha512                                    :func:`strpos`                                classification_recall
    array_remove                              :func:`element_at`                        :func:`length`                            shuffle                                   strrpos                                       classification_thresholds
    array_sort                                empty_approx_set                          levenshtein_distance                      sign                                      :func:`substr`                                convex_hull_agg
    array_sum                                 enum_key                                  line_interpolate_point                    simplify_geometry                         tan                                           corr
    array_union                               :func:`exp`                               line_locate_point                         sin                                       tanh                                          :func:`count`
    arrays_overlap                            expand_envelope                           :func:`ln`                                slice                                     timezone_hour                                 :func:`count_if`
    asin                                      features                                  localtime                                 spatial_partitions                        timezone_minute                               covar_pop
    :func:`atan`                              :func:`filter`                            localtimestamp                            split                                     to_base                                       covar_samp
    :func:`atan2`                             flatten                                   log10                                     split_part                                :func:`to_base64`                             differential_entropy
    atlas_action_is_classic_xdata             flatten_geometry_collections              log2                                      split_to_map                              to_base64url                                  entropy
    atlas_action_xdata_category               :func:`floor`                             :func:`lower`                             split_to_multimap                         to_big_endian_32                              evaluate_classifier_predictions
    atlas_action_xdata_metric                 fnv1_32                                   lpad                                      spooky_hash_v2_32                         to_big_endian_64                              every
    atlas_action_xdata_parse                  fnv1_64                                   ltrim                                     spooky_hash_v2_64                         to_geometry                                   geometric_mean
    atlas_search_xdata_parse                  fnv1a_32                                  :func:`map`                               :func:`sqrt`                              :func:`to_hex`                                geometry_union_agg
    bar                                       fnv1a_64                                  :func:`map_concat`                        st_area                                   to_ieee754_32                                 histogram
    beta_cdf                                  format_datetime                           :func:`map_entries`                       st_asbinary                               to_ieee754_64                                 khyperloglog_agg
    bing_tile                                 from_base                                 :func:`map_filter`                        st_astext                                 to_iso8601                                    kurtosis
    bing_tile_at                              :func:`from_base64`                       map_from_entries                          st_boundary                               to_milliseconds                               learn_classifier
    bing_tile_children                        from_base64url                            :func:`map_keys`                          st_buffer                                 to_spherical_geography                        learn_libsvm_classifier
    bing_tile_coordinates                     from_big_endian_32                        map_normalize                             st_centroid                               :func:`to_unixtime`                           learn_libsvm_regressor
    bing_tile_parent                          from_big_endian_64                        :func:`map_values`                        st_contains                               :func:`to_utf8`                               learn_regressor
    bing_tile_polygon                         :func:`from_hex`                          map_zip_with                              st_convexhull                             :func:`transform`                             make_set_digest
    bing_tile_quadkey                         from_ieee754_32                           :func:`md5`                               st_coorddim                               transform_keys                                :func:`map_agg`
    bing_tile_zoom_level                      from_ieee754_64                           merge_hll                                 st_crosses                                transform_values                              map_union
    bing_tiles_around                         from_iso8601_date                         merge_khll                                st_difference                             trim                                          map_union_sum
    binomial_cdf                              from_iso8601_timestamp                    millisecond                               st_dimension                              truncate                                      :func:`max`
    bit_count                                 :func:`from_unixtime`                     minute                                    st_disjoint                               typeof                                        :func:`max_by`
    :func:`bitwise_and`                       from_utf8                                 mod                                       st_distance                               uniqueness_distribution                       merge
    bitwise_arithmetic_shift_right            geometry_as_geojson                       month                                     st_endpoint                               :func:`upper`                                 merge_set_digest
    bitwise_left_shift                        geometry_from_geojson                     multimap_from_entries                     st_envelope                               :func:`url_decode`                            :func:`min`
    bitwise_logical_shift_right               geometry_invalid_reason                   myanmar_font_encoding                     st_envelopeaspts                          :func:`url_encode`                            :func:`min_by`
    :func:`bitwise_not`                       geometry_nearest_points                   myanmar_normalize_unicode                 st_equals                                 url_extract_fragment                          multimap_agg
    :func:`bitwise_or`                        geometry_to_bing_tiles                    nan                                       st_exteriorring                           url_extract_host                              numeric_histogram
    bitwise_right_shift                       geometry_to_dissolved_bing_tiles          ngrams                                    st_geometries                             url_extract_parameter                         qdigest_agg
    bitwise_right_shift_arithmetic            geometry_union                            none_match                                st_geometryfromtext                       url_extract_path                              reduce_agg
    bitwise_shift_left                        great_circle_distance                     normal_cdf                                st_geometryn                              url_extract_port                              regr_intercept
    :func:`bitwise_xor`                       greatest                                  normalize                                 st_geometrytype                           url_extract_protocol                          regr_slope
    :func:`cardinality`                       hamming_distance                          now                                       st_geomfrombinary                         url_extract_query                             set_agg
    cauchy_cdf                                hash_counts                               parse_datetime                            st_interiorringn                          value_at_quantile                             set_union
    :func:`cbrt`                              hmac_md5                                  parse_duration                            st_interiorrings                          values_at_quantiles                           skewness
    :func:`ceil`                              hmac_sha1                                 parse_presto_data_size                    st_intersection                           week                                          spatial_partitioning
    ceiling                                   hmac_sha256                               pi                                        st_intersects                             week_of_year                                  :func:`stddev`
    chi_squared_cdf                           hmac_sha512                               poisson_cdf                               st_isclosed                               weibull_cdf                                   :func:`stddev_pop`
    :func:`chr`                               hour                                      pow                                       st_isempty                                :func:`width_bucket`                          :func:`stddev_samp`
    classify                                  infinity                                  :func:`power`                             st_isring                                 wilson_interval_lower                         :func:`sum`
    :func:`codepoint`                         intersection_cardinality                  quantile_at_value                         st_issimple                               wilson_interval_upper                         :func:`var_pop`
    color                                     inverse_beta_cdf                          quarter                                   st_isvalid                                word_stem                                     :func:`var_samp`
    combinations                              inverse_binomial_cdf                      radians                                   st_length                                 :func:`xxhash64`                              :func:`variance`
    :func:`concat`                            inverse_cauchy_cdf                        :func:`rand`                              st_linefromtext                           year
    :func:`contains`                          inverse_chi_squared_cdf                   random                                    st_linestring                             year_of_week
    cos                                       inverse_normal_cdf                        :func:`reduce`                            st_multipoint                             yow
    cosh                                      inverse_poisson_cdf                       :func:`regexp_extract`                    st_numgeometries                          zip
    cosine_similarity                         inverse_weibull_cdf                       regexp_extract_all                        st_numinteriorring                        zip_with
    crc32                                     ip_prefix                                 :func:`regexp_like`                       st_numpoints
    current_date                              ip_subnet_max                             regexp_replace                            st_overlaps
    current_time                              ip_subnet_min                             regexp_split                              st_point
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================
