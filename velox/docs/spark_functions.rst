***********************
Spark Functions
***********************

The semantics of Spark functions align with
`Spark 3.5 <https://spark.apache.org/docs/3.5.0/api/sql/index.html>`_.
In the function descriptions, a function is marked as *ANSI compliant*
if it adheres to ANSI standard, subject to the :doc:`spark.ansi_enabled <configs>`
configuration. Otherwise, it simply follows Spark's semantics in ANSI OFF mode.

.. toctree::
    :maxdepth: 1

    functions/spark/math
    functions/spark/bitwise
    functions/spark/decimal
    functions/spark/comparison
    functions/spark/string
    functions/spark/datetime
    functions/spark/array
    functions/spark/map
    functions/spark/misc
    functions/spark/regexp
    functions/spark/binary
    functions/spark/aggregate
    functions/spark/window
    functions/spark/conversion
    functions/spark/url
    functions/spark/json

Here is a list of all scalar and aggregate Spark functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/spark/coverage>` functions.

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

    ===========================================  ===========================================  ===========================================  ==  ===========================================  ==  ===========================================
    Scalar Functions                                                                                                                           Aggregate Functions                              Window Functions
    =====================================================================================================================================  ==  ===========================================  ==  ===========================================
    :spark:func:`abs`                            :spark:func:`divide_deny_precision_loss`     :spark:func:`not`                                :spark:func:`avg`                                :spark:func:`dense_rank`
    :spark:func:`acos`                           :spark:func:`doy`                            :spark:func:`overlay`                            :spark:func:`bit_xor`                            :spark:func:`nth_value`
    :spark:func:`acosh`                          :spark:func:`element_at`                     :spark:func:`pmod`                               :spark:func:`bloom_filter_agg`                   :spark:func:`ntile`
    :spark:func:`add`                            :spark:func:`empty2null`                     :spark:func:`power`                              :spark:func:`collect_list`                       :spark:func:`rank`
    :spark:func:`add_deny_precision_loss`        :spark:func:`endswith`                       :spark:func:`quarter`                            :spark:func:`collect_set`                        :spark:func:`row_number`
    :spark:func:`add_months`                     :spark:func:`equalnullsafe`                  :spark:func:`raise_error`                        :spark:func:`corr`
    :spark:func:`aggregate`                      :spark:func:`equalto`                        :spark:func:`rand`                               :spark:func:`covar_samp`
    :spark:func:`array`                          :spark:func:`exists`                         :spark:func:`random`                             :spark:func:`first`
    :spark:func:`array_append`                   :spark:func:`exp`                            :spark:func:`regexp_extract`                     :spark:func:`first_ignore_null`
    :spark:func:`array_compact`                  :spark:func:`expm1`                          :spark:func:`regexp_extract_all`                 :spark:func:`kurtosis`
    :spark:func:`array_contains`                 :spark:func:`factorial`                      :spark:func:`regexp_replace`                     :spark:func:`last`
    :spark:func:`array_distinct`                 :spark:func:`filter`                         :spark:func:`remainder`                          :spark:func:`last_ignore_null`
    :spark:func:`array_except`                   :spark:func:`find_in_set`                    :spark:func:`repeat`                             :spark:func:`max`
    :spark:func:`array_insert`                   :spark:func:`flatten`                        :spark:func:`replace`                            :spark:func:`max_by`
    :spark:func:`array_intersect`                :spark:func:`floor`                          :spark:func:`reverse`                            :spark:func:`min`
    :spark:func:`array_join`                     :spark:func:`forall`                         :spark:func:`rint`                               :spark:func:`min_by`
    :spark:func:`array_max`                      :spark:func:`from_unixtime`                  :spark:func:`rlike`                              :spark:func:`mode`
    :spark:func:`array_min`                      :spark:func:`from_utc_timestamp`             :spark:func:`round`                              :spark:func:`regr_replacement`
    :spark:func:`array_position`                 :spark:func:`get`                            :spark:func:`rpad`                               :spark:func:`skewness`
    :spark:func:`array_prepend`                  :spark:func:`get_json_object`                :spark:func:`rtrim`                              :spark:func:`stddev`
    :spark:func:`array_remove`                   :spark:func:`get_timestamp`                  :spark:func:`sec`                                :spark:func:`stddev_samp`
    :spark:func:`array_repeat`                   :spark:func:`greaterthan`                    :spark:func:`second`                             :spark:func:`sum`
    :spark:func:`array_sort`                     :spark:func:`greaterthanorequal`             :spark:func:`sha1`                               :spark:func:`var_samp`
    :spark:func:`array_union`                    :spark:func:`greatest`                       :spark:func:`sha2`                               :spark:func:`variance`
    :spark:func:`arrays_zip`                     :spark:func:`hash`                           :spark:func:`shiftleft`
    :spark:func:`ascii`                          :spark:func:`hash_with_seed`                 :spark:func:`shiftright`
    :spark:func:`asin`                           :spark:func:`hex`                            :spark:func:`shuffle`
    :spark:func:`asinh`                          :spark:func:`hour`                           :spark:func:`sign`
    :spark:func:`atan`                           :spark:func:`hypot`                          :spark:func:`sinh`
    :spark:func:`atan2`                          :spark:func:`in`                             :spark:func:`size`
    :spark:func:`atanh`                          :spark:func:`instr`                          :spark:func:`slice`
    :spark:func:`between`                        :spark:func:`isnan`                          :spark:func:`sort_array`
    :spark:func:`bin`                            :spark:func:`isnotnull`                      :spark:func:`soundex`
    :spark:func:`bit_count`                      :spark:func:`isnull`                         :spark:func:`spark_partition_id`
    :spark:func:`bit_get`                        :spark:func:`json_array_length`              :spark:func:`split`
    :spark:func:`bit_length`                     :spark:func:`json_object_keys`               :spark:func:`sqrt`
    :spark:func:`bitwise_and`                    :spark:func:`last_day`                       :spark:func:`startswith`
    :spark:func:`bitwise_not`                    :spark:func:`least`                          :spark:func:`str_to_map`
    :spark:func:`bitwise_or`                     :spark:func:`left`                           :spark:func:`substring`
    :spark:func:`bitwise_xor`                    :spark:func:`length`                         :spark:func:`substring_index`
    :spark:func:`cbrt`                           :spark:func:`lessthan`                       :spark:func:`subtract`
    :spark:func:`ceil`                           :spark:func:`lessthanorequal`                :spark:func:`subtract_deny_precision_loss`
    :spark:func:`checked_add`                    :spark:func:`levenshtein`                    :spark:func:`timestamp_micros`
    :spark:func:`checked_divide`                 :spark:func:`like`                           :spark:func:`timestamp_millis`
    :spark:func:`checked_multiply`               :spark:func:`locate`                         :spark:func:`to_unix_timestamp`
    :spark:func:`checked_subtract`               :spark:func:`log`                            :spark:func:`to_utc_timestamp`
    :spark:func:`chr`                            :spark:func:`log10`                          :spark:func:`transform`
    :spark:func:`concat`                         :spark:func:`log1p`                          :spark:func:`translate`
    :spark:func:`contains`                       :spark:func:`log2`                           :spark:func:`trim`
    :spark:func:`conv`                           :spark:func:`lower`                          :spark:func:`trunc`
    :spark:func:`cos`                            :spark:func:`lpad`                           :spark:func:`unaryminus`
    :spark:func:`cosh`                           :spark:func:`ltrim`                          :spark:func:`unbase64`
    :spark:func:`cot`                            :spark:func:`luhn_check`                     :spark:func:`unhex`
    :spark:func:`crc32`                          :spark:func:`make_date`                      :spark:func:`unix_date`
    :spark:func:`csc`                            :spark:func:`make_timestamp`                 :spark:func:`unix_micros`
    :spark:func:`date_add`                       :spark:func:`make_ym_interval`               :spark:func:`unix_millis`
    :spark:func:`date_format`                    :spark:func:`map`                            :spark:func:`unix_seconds`
    :spark:func:`date_from_unix_date`            :spark:func:`map_concat`                     :spark:func:`unix_timestamp`
    :spark:func:`date_sub`                       :spark:func:`map_entries`                    :spark:func:`unscaled_value`
    :spark:func:`date_trunc`                     :spark:func:`map_filter`                     :spark:func:`upper`
    :spark:func:`datediff`                       :spark:func:`map_from_arrays`                :spark:func:`url_decode`
    :spark:func:`day`                            :spark:func:`map_keys`                       :spark:func:`url_encode`
    :spark:func:`dayofmonth`                     :spark:func:`map_values`                     :spark:func:`uuid`
    :spark:func:`dayofweek`                      :spark:func:`map_zip_with`                   :spark:func:`varchar_type_write_side_check`
    :spark:func:`dayofyear`                      :spark:func:`mask`                           :spark:func:`week_of_year`
    :spark:func:`decimal_equalto`                :spark:func:`md5`                            :spark:func:`weekday`
    :spark:func:`decimal_greaterthan`            :spark:func:`might_contain`                  :spark:func:`width_bucket`
    :spark:func:`decimal_greaterthanorequal`     :spark:func:`minute`                         :spark:func:`xxhash64`
    :spark:func:`decimal_lessthan`               :spark:func:`monotonically_increasing_id`    :spark:func:`xxhash64_with_seed`
    :spark:func:`decimal_lessthanorequal`        :spark:func:`month`                          :spark:func:`year`
    :spark:func:`decimal_notequalto`             :spark:func:`multiply`                       :spark:func:`year_of_week`
    :spark:func:`degrees`                        :spark:func:`multiply_deny_precision_loss`   :spark:func:`zip_with`
    :spark:func:`divide`                         :spark:func:`next_day`
    ===========================================  ===========================================  ===========================================  ==  ===========================================  ==  ===========================================
