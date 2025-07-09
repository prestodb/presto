=================
Function Coverage
=================

Here is a list of all scalar, aggregate, and window functions from Spark, with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage td:nth-child(8) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(4) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    =========================================  =========================================  =========================================  =========================================  =========================================  ==  =========================================  ==  =========================================
    Scalar Functions                                                                                                                                                                                                           Aggregate Functions                            Window Functions
    =====================================================================================================================================================================================================================  ==  =========================================  ==  =========================================
    :spark:func:`abs`                          count_if                                   inline                                     nvl                                        :spark:func:`sqrt`                             any                                            cume_dist
    :spark:func:`acos`                         count_min_sketch                           inline_outer                               nvl2                                       stack                                          approx_count_distinct                          :spark:func:`dense_rank`
    :spark:func:`acosh`                        covar_pop                                  input_file_block_length                    octet_length                               std                                            approx_percentile                              first_value
    :spark:func:`add_months`                   covar_samp                                 input_file_block_start                     or                                         stddev                                         array_agg                                      lag
    :spark:func:`aggregate`                    :spark:func:`crc32`                        input_file_name                            :spark:func:`overlay`                      stddev_pop                                     :spark:func:`avg`                              last_value
    and                                        cume_dist                                  :spark:func:`instr`                        parse_url                                  stddev_samp                                    bit_and                                        lead
    any                                        current_catalog                            int                                        percent_rank                               :spark:func:`str_to_map`                       bit_or                                         :spark:func:`nth_value`
    approx_count_distinct                      current_database                           :spark:func:`isnan`                        percentile                                 string                                         :spark:func:`bit_xor`                          :spark:func:`ntile`
    approx_percentile                          current_date                               :spark:func:`isnotnull`                    percentile_approx                          struct                                         bool_and                                       percent_rank
    :spark:func:`array`                        current_timestamp                          :spark:func:`isnull`                       pi                                         substr                                         bool_or                                        :spark:func:`rank`
    :spark:func:`array_contains`               current_timezone                           java_method                                :spark:func:`pmod`                         :spark:func:`substring`                        :spark:func:`collect_list`                     :spark:func:`row_number`
    :spark:func:`array_distinct`               current_user                               :spark:func:`json_array_length`            posexplode                                 :spark:func:`substring_index`                  :spark:func:`collect_set`
    :spark:func:`array_except`                 date                                       :spark:func:`json_object_keys`             posexplode_outer                           sum                                            :spark:func:`corr`
    :spark:func:`array_intersect`              :spark:func:`date_add`                     json_tuple                                 position                                   tan                                            count
    :spark:func:`array_join`                   :spark:func:`date_format`                  kurtosis                                   positive                                   tanh                                           count_if
    :spark:func:`array_max`                    :spark:func:`date_from_unix_date`          lag                                        pow                                        timestamp                                      count_min_sketch
    :spark:func:`array_min`                    date_part                                  last                                       :spark:func:`power`                        :spark:func:`timestamp_micros`                 covar_pop
    :spark:func:`array_position`               :spark:func:`date_sub`                     :spark:func:`last_day`                     printf                                     :spark:func:`timestamp_millis`                 :spark:func:`covar_samp`
    :spark:func:`array_remove`                 :spark:func:`date_trunc`                   last_value                                 :spark:func:`quarter`                      timestamp_seconds                              every
    :spark:func:`array_repeat`                 :spark:func:`datediff`                     lcase                                      radians                                    tinyint                                        :spark:func:`first`
    :spark:func:`array_sort`                   :spark:func:`day`                          lead                                       :spark:func:`raise_error`                  to_csv                                         first_value
    :spark:func:`array_union`                  :spark:func:`dayofmonth`                   :spark:func:`least`                        :spark:func:`rand`                         to_date                                        grouping
    arrays_overlap                             :spark:func:`dayofweek`                    :spark:func:`left`                         randn                                      to_json                                        grouping_id
    :spark:func:`arrays_zip`                   :spark:func:`dayofyear`                    :spark:func:`length`                       :spark:func:`random`                       to_timestamp                                   histogram_numeric
    :spark:func:`ascii`                        decimal                                    :spark:func:`levenshtein`                  range                                      :spark:func:`to_unix_timestamp`                :spark:func:`kurtosis`
    :spark:func:`asin`                         decode                                     :spark:func:`like`                         rank                                       :spark:func:`to_utc_timestamp`                 :spark:func:`last`
    :spark:func:`asinh`                        :spark:func:`degrees`                      ln                                         reflect                                    :spark:func:`transform`                        last_value
    assert_true                                dense_rank                                 :spark:func:`locate`                       regexp                                     transform_keys                                 :spark:func:`max`
    :spark:func:`atan`                         div                                        :spark:func:`log`                          :spark:func:`regexp_extract`               transform_values                               :spark:func:`max_by`
    :spark:func:`atan2`                        double                                     :spark:func:`log10`                        :spark:func:`regexp_extract_all`           :spark:func:`translate`                        mean
    :spark:func:`atanh`                        e                                          :spark:func:`log1p`                        regexp_like                                :spark:func:`trim`                             :spark:func:`min`
    avg                                        :spark:func:`element_at`                   :spark:func:`log2`                         :spark:func:`regexp_replace`               :spark:func:`trunc`                            :spark:func:`min_by`
    base64                                     elt                                        :spark:func:`lower`                        :spark:func:`repeat`                       try_add                                        percentile
    :spark:func:`between`                      encode                                     :spark:func:`lpad`                         :spark:func:`replace`                      try_divide                                     percentile_approx
    bigint                                     every                                      :spark:func:`ltrim`                        :spark:func:`reverse`                      typeof                                         regr_avgx
    :spark:func:`bin`                          :spark:func:`exists`                       :spark:func:`make_date`                    right                                      ucase                                          regr_avgy
    binary                                     :spark:func:`exp`                          make_dt_interval                           :spark:func:`rint`                         :spark:func:`unbase64`                         regr_count
    bit_and                                    explode                                    make_interval                              :spark:func:`rlike`                        :spark:func:`unhex`                            regr_r2
    :spark:func:`bit_count`                    explode_outer                              :spark:func:`make_timestamp`               :spark:func:`round`                        :spark:func:`unix_date`                        :spark:func:`skewness`
    :spark:func:`bit_get`                      :spark:func:`expm1`                        :spark:func:`make_ym_interval`             row_number                                 :spark:func:`unix_micros`                      some
    :spark:func:`bit_length`                   extract                                    :spark:func:`map`                          :spark:func:`rpad`                         :spark:func:`unix_millis`                      std
    bit_or                                     :spark:func:`factorial`                    :spark:func:`map_concat`                   :spark:func:`rtrim`                        :spark:func:`unix_seconds`                     :spark:func:`stddev`
    bit_xor                                    :spark:func:`filter`                       :spark:func:`map_entries`                  schema_of_csv                              :spark:func:`unix_timestamp`                   stddev_pop
    bool_and                                   :spark:func:`find_in_set`                  :spark:func:`map_filter`                   schema_of_json                             :spark:func:`upper`                            :spark:func:`stddev_samp`
    bool_or                                    first                                      :spark:func:`map_from_arrays`              :spark:func:`second`                       :spark:func:`uuid`                             :spark:func:`sum`
    boolean                                    first_value                                map_from_entries                           sentences                                  var_pop                                        try_avg
    bround                                     :spark:func:`flatten`                      :spark:func:`map_keys`                     sequence                                   var_samp                                       try_sum
    btrim                                      float                                      :spark:func:`map_values`                   session_window                             variance                                       var_pop
    cardinality                                :spark:func:`floor`                        :spark:func:`map_zip_with`                 sha                                        version                                        :spark:func:`var_samp`
    case                                       :spark:func:`forall`                       max                                        :spark:func:`sha1`                         :spark:func:`weekday`                          :spark:func:`variance`
    cast                                       format_number                              max_by                                     :spark:func:`sha2`                         weekofyear
    :spark:func:`cbrt`                         format_string                              :spark:func:`md5`                          :spark:func:`shiftleft`                    when
    :spark:func:`ceil`                         from_csv                                   mean                                       :spark:func:`shiftright`                   :spark:func:`width_bucket`
    ceiling                                    from_json                                  min                                        shiftrightunsigned                         window
    char                                       :spark:func:`from_unixtime`                min_by                                     :spark:func:`shuffle`                      xpath
    char_length                                :spark:func:`from_utc_timestamp`           :spark:func:`minute`                       :spark:func:`sign`                         xpath_boolean
    character_length                           :spark:func:`get_json_object`              mod                                        signum                                     xpath_double
    :spark:func:`chr`                          getbit                                     :spark:func:`monotonically_increasing_id`  sin                                        xpath_float
    coalesce                                   :spark:func:`greatest`                     :spark:func:`month`                        :spark:func:`sinh`                         xpath_int
    collect_list                               grouping                                   months_between                             :spark:func:`size`                         xpath_long
    collect_set                                grouping_id                                named_struct                               skewness                                   xpath_number
    :spark:func:`concat`                       :spark:func:`hash`                         nanvl                                      :spark:func:`slice`                        xpath_short
    concat_ws                                  :spark:func:`hex`                          negative                                   smallint                                   xpath_string
    :spark:func:`conv`                         :spark:func:`hour`                         :spark:func:`next_day`                     some                                       :spark:func:`xxhash64`
    corr                                       :spark:func:`hypot`                        :spark:func:`not`                          :spark:func:`sort_array`                   :spark:func:`year`
    :spark:func:`cos`                          if                                         now                                        :spark:func:`soundex`                      :spark:func:`zip_with`
    :spark:func:`cosh`                         ifnull                                     nth_value                                  space
    :spark:func:`cot`                          :spark:func:`in`                           ntile                                      :spark:func:`spark_partition_id`
    count                                      initcap                                    nullif                                     :spark:func:`split`
    =========================================  =========================================  =========================================  =========================================  =========================================  ==  =========================================  ==  =========================================
