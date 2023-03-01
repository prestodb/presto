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
    :func:`abs`                             :func:`eq`                              :func:`quarter`                             :func:`approx_distinct`                     :func:`cume_dist`
    :func:`acos`                            :func:`exp`                             :func:`radians`                             :func:`approx_most_frequent`                :func:`dense_rank`
    :func:`array_average`                   :func:`filter`                          :func:`rand`                                :func:`approx_percentile`                   :func:`nth_value`
    :func:`array_constructor`               :func:`floor`                           :func:`random`                              :func:`approx_set`                          :func:`ntile`
    :func:`array_distinct`                  :func:`format_datetime`                 :func:`reduce`                              :func:`arbitrary`                           :func:`percent_rank`
    :func:`array_duplicates`                :func:`from_base`                       :func:`regexp_extract`                      :func:`array_agg`                           :func:`rank`
    :func:`array_except`                    :func:`from_base64`                     :func:`regexp_extract_all`                  :func:`avg`                                 :func:`row_number`
    :func:`array_frequency`                 :func:`from_hex`                        :func:`regexp_like`                         :func:`bitwise_and_agg`
    :func:`array_has_duplicates`            :func:`from_unixtime`                   :func:`regexp_replace`                      :func:`bitwise_or_agg`
    :func:`array_intersect`                 :func:`greatest`                        :func:`repeat`                              :func:`bool_and`
    :func:`array_join`                      :func:`gt`                              :func:`replace`                             :func:`bool_or`
    :func:`array_max`                       :func:`gte`                             :func:`reverse`                             :func:`checksum`
    :func:`array_min`                       :func:`hmac_sha1`                       :func:`round`                               :func:`corr`
    :func:`array_normalize`                 :func:`hmac_sha256`                     :func:`rpad`                                :func:`count`
    :func:`array_position`                  :func:`hmac_sha512`                     :func:`rtrim`                               :func:`count_if`
    :func:`array_sort`                      :func:`hour`                            :func:`second`                              :func:`covar_pop`
    :func:`array_sum`                       in                                      :func:`sha1`                                :func:`covar_samp`
    :func:`arrays_overlap`                  :func:`infinity`                        :func:`sha256`                              :func:`every`
    :func:`asin`                            :func:`is_finite`                       :func:`sha512`                              :func:`histogram`
    :func:`atan`                            :func:`is_infinite`                     :func:`shuffle`                             :func:`map_agg`
    :func:`atan2`                           :func:`is_json_scalar`                  :func:`sign`                                :func:`map_union`
    :func:`between`                         :func:`is_nan`                          :func:`sin`                                 :func:`max`
    :func:`bit_count`                       :func:`is_null`                         :func:`slice`                               :func:`max_by`
    :func:`bitwise_and`                     :func:`json_array_contains`             :func:`split`                               :func:`max_data_size_for_stats`
    :func:`bitwise_arithmetic_shift_right`  :func:`json_array_length`               :func:`split_part`                          :func:`merge`
    :func:`bitwise_left_shift`              :func:`json_extract_scalar`             :func:`spooky_hash_v2_32`                   :func:`min`
    :func:`bitwise_logical_shift_right`     :func:`json_format`                     :func:`spooky_hash_v2_64`                   :func:`min_by`
    :func:`bitwise_not`                     :func:`json_parse`                      :func:`sqrt`                                :func:`stddev`
    :func:`bitwise_or`                      :func:`json_size`                       :func:`strpos`                              :func:`stddev_pop`
    :func:`bitwise_right_shift`             :func:`least`                           :func:`strrpos`                             :func:`stddev_samp`
    :func:`bitwise_right_shift_arithmetic`  :func:`length`                          :func:`subscript`                           :func:`sum`
    :func:`bitwise_shift_left`              :func:`like`                            :func:`substr`                              :func:`var_pop`
    :func:`bitwise_xor`                     :func:`ln`                              :func:`tan`                                 :func:`var_samp`
    :func:`cardinality`                     :func:`log10`                           :func:`tanh`                                :func:`variance`
    :func:`cbrt`                            :func:`log2`                            :func:`to_base`
    :func:`ceil`                            :func:`lower`                           :func:`to_base64`
    :func:`ceiling`                         :func:`lpad`                            :func:`to_hex`
    :func:`chr`                             :func:`lt`                              :func:`to_unixtime`
    :func:`clamp`                           :func:`lte`                             :func:`to_utf8`
    :func:`codepoint`                       :func:`ltrim`                           :func:`transform`
    :func:`combinations`                    :func:`map`                             :func:`transform_keys`
    :func:`concat`                          :func:`map_concat`                      :func:`transform_values`
    :func:`contains`                        :func:`map_entries`                     :func:`trim`
    :func:`cos`                             :func:`map_filter`                      :func:`truncate`
    :func:`cosh`                            :func:`map_keys`                        :func:`upper`
    :func:`crc32`                           :func:`map_values`                      :func:`url_decode`
    :func:`date_add`                        :func:`map_zip_with`                    :func:`url_encode`
    :func:`date_diff`                       :func:`md5`                             :func:`url_extract_fragment`
    :func:`date_format`                     :func:`millisecond`                     :func:`url_extract_host`
    :func:`date_parse`                      :func:`minus`                           :func:`url_extract_parameter`
    :func:`date_trunc`                      :func:`minute`                          :func:`url_extract_path`
    :func:`day`                             :func:`mod`                             :func:`url_extract_port`
    :func:`day_of_month`                    :func:`month`                           :func:`url_extract_protocol`
    :func:`day_of_week`                     :func:`multiply`                        :func:`url_extract_query`
    :func:`day_of_year`                     :func:`nan`                             :func:`week`
    :func:`degrees`                         :func:`negate`                          :func:`week_of_year`
    :func:`distinct_from`                   :func:`neq`                             :func:`width_bucket`
    :func:`divide`                          not                                     :func:`xxhash64`
    :func:`dow`                             :func:`parse_datetime`                  :func:`year`
    :func:`doy`                             :func:`pi`                              :func:`year_of_week`
    :func:`e`                               :func:`plus`                            :func:`yow`
    :func:`element_at`                      :func:`pow`                             :func:`zip`
    :func:`empty_approx_set`                :func:`power`                           :func:`zip_with`
    ======================================  ======================================  ======================================  ==  ======================================  ==  ======================================
