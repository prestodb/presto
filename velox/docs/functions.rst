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
    :func:`abs`                             :func:`exp`                             :func:`quarter`                             :func:`approx_distinct`
    :func:`acos`                            :func:`filter`                          :func:`radians`                             :func:`approx_most_frequent`
    :func:`array_constructor`               :func:`floor`                           :func:`rand`                                :func:`approx_percentile`
    :func:`array_distinct`                  :func:`format_datetime`                 :func:`random`                              :func:`approx_set`
    :func:`array_duplicates`                :func:`from_base`                       :func:`reduce`                              :func:`arbitrary`
    :func:`array_except`                    :func:`from_base64`                     :func:`regexp_extract`                      :func:`array_agg`
    :func:`array_has_duplicates`            :func:`from_hex`                        :func:`regexp_extract_all`                  :func:`avg`
    :func:`array_intersect`                 :func:`from_unixtime`                   :func:`regexp_like`                         :func:`bitwise_and_agg`
    :func:`array_join`                      :func:`greatest`                        :func:`regexp_replace`                      :func:`bitwise_or_agg`
    :func:`array_max`                       :func:`gt`                              :func:`repeat`                              :func:`bool_and`
    :func:`array_min`                       :func:`gte`                             :func:`replace`                             :func:`bool_or`
    :func:`array_position`                  :func:`hmac_sha1`                       :func:`reverse`                             :func:`checksum`
    :func:`array_sort`                      :func:`hmac_sha256`                     :func:`round`                               :func:`corr`
    :func:`array_sum`                       :func:`hmac_sha512`                     :func:`rpad`                                :func:`count`
    :func:`arrays_overlap`                  :func:`hour`                            :func:`rtrim`                               :func:`count_if`
    :func:`asin`                            in                                      :func:`second`                              :func:`covar_pop`
    :func:`atan`                            :func:`infinity`                        :func:`sha256`                              :func:`covar_samp`
    :func:`atan2`                           :func:`is_finite`                       :func:`sha512`                              :func:`every`
    :func:`between`                         :func:`is_infinite`                     :func:`sign`                                :func:`histogram`
    :func:`bit_count`                       :func:`is_json_scalar`                  :func:`sin`                                 :func:`map_agg`
    :func:`bitwise_and`                     :func:`is_nan`                          :func:`slice`                               :func:`map_union`
    :func:`bitwise_arithmetic_shift_right`  :func:`is_null`                         :func:`split`                               :func:`max`
    :func:`bitwise_left_shift`              :func:`json_array_contains`             :func:`split_part`                          :func:`max_by`
    :func:`bitwise_logical_shift_right`     :func:`json_array_length`               :func:`spooky_hash_v2_32`                   :func:`max_data_size_for_stats`
    :func:`bitwise_not`                     :func:`json_extract_scalar`             :func:`spooky_hash_v2_64`                   :func:`merge`
    :func:`bitwise_or`                      :func:`json_format`                     :func:`sqrt`                                :func:`min`
    :func:`bitwise_right_shift`             :func:`json_size`                       :func:`strpos`                              :func:`min_by`
    :func:`bitwise_right_shift_arithmetic`  :func:`least`                           :func:`strrpos`                             :func:`stddev`
    :func:`bitwise_shift_left`              :func:`length`                          :func:`subscript`                           :func:`stddev_pop`
    :func:`bitwise_xor`                     :func:`like`                            :func:`substr`                              :func:`stddev_samp`
    :func:`cardinality`                     :func:`ln`                              :func:`tan`                                 :func:`sum`
    :func:`cbrt`                            :func:`log10`                           :func:`tanh`                                :func:`var_pop`
    :func:`ceil`                            :func:`log2`                            :func:`to_base`                             :func:`var_samp`
    :func:`ceiling`                         :func:`lower`                           :func:`to_base64`                           :func:`variance`
    :func:`chr`                             :func:`lpad`                            :func:`to_hex`
    :func:`clamp`                           :func:`lt`                              :func:`to_unixtime`
    :func:`codepoint`                       :func:`lte`                             :func:`to_utf8`
    :func:`combinations`                    :func:`ltrim`                           :func:`transform`
    :func:`concat`                          :func:`map`                             :func:`transform_keys`
    :func:`contains`                        :func:`map_concat`                      :func:`transform_values`
    :func:`cos`                             :func:`map_entries`                     :func:`trim`
    :func:`cosh`                            :func:`map_filter`                      :func:`truncate`
    :func:`crc32`                           :func:`map_keys`                        :func:`upper`
    :func:`date_add`                        :func:`map_values`                      :func:`url_decode`
    :func:`date_diff`                       :func:`map_zip_with`                    :func:`url_encode`
    :func:`date_format`                     :func:`md5`                             :func:`url_extract_fragment`
    :func:`date_parse`                      :func:`millisecond`                     :func:`url_extract_host`
    :func:`date_trunc`                      :func:`minus`                           :func:`url_extract_parameter`
    :func:`day`                             :func:`minute`                          :func:`url_extract_path`
    :func:`day_of_month`                    :func:`mod`                             :func:`url_extract_port`
    :func:`day_of_week`                     :func:`month`                           :func:`url_extract_protocol`
    :func:`day_of_year`                     :func:`multiply`                        :func:`url_extract_query`
    :func:`degrees`                         :func:`nan`                             :func:`week`
    :func:`distinct_from`                   :func:`negate`                          :func:`week_of_year`
    :func:`divide`                          :func:`neq`                             :func:`width_bucket`
    :func:`dow`                             not                                     :func:`xxhash64`
    :func:`doy`                             :func:`parse_datetime`                  :func:`year`
    :func:`e`                               :func:`pi`                              :func:`year_of_week`
    :func:`element_at`                      :func:`plus`                            :func:`yow`
    :func:`empty_approx_set`                :func:`pow`                             :func:`zip`
    :func:`eq`                              :func:`power`                           :func:`zip_with`
    ======================================  ======================================  ======================================  ==  ======================================
