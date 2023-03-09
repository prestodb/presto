.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(4) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    =========================================  =========================================  =========================================  =========================================  =========================================  ==  =========================================
    Scalar Functions                                                                                                                                                                                                           Aggregate Functions
    =====================================================================================================================================================================================================================  ==  =========================================
    :spark:func:`abs`                          count_if                                   inline                                     nvl                                        sqrt                                           any
    acos                                       count_min_sketch                           inline_outer                               nvl2                                       stack                                          approx_count_distinct
    acosh                                      covar_pop                                  input_file_block_length                    octet_length                               std                                            approx_percentile
    add_months                                 covar_samp                                 input_file_block_start                     or                                         stddev                                         array_agg
    :spark:func:`aggregate`                    crc32                                      input_file_name                            overlay                                    stddev_pop                                     avg
    and                                        cume_dist                                  :spark:func:`instr`                        parse_url                                  stddev_samp                                    bit_and
    any                                        current_catalog                            int                                        percent_rank                               str_to_map                                     bit_or
    approx_count_distinct                      current_database                           isnan                                      percentile                                 string                                         bit_xor
    approx_percentile                          current_date                               :spark:func:`isnotnull`                    percentile_approx                          struct                                         bool_and
    :spark:func:`array`                        current_timestamp                          :spark:func:`isnull`                       pi                                         substr                                         bool_or
    :spark:func:`array_contains`               current_timezone                           java_method                                :spark:func:`pmod`                         :spark:func:`substring`                        collect_list
    array_distinct                             current_user                               json_array_length                          posexplode                                 substring_index                                collect_set
    array_except                               date                                       json_object_keys                           posexplode_outer                           sum                                            corr
    :spark:func:`array_intersect`              date_add                                   json_tuple                                 position                                   tan                                            count
    array_join                                 date_format                                kurtosis                                   positive                                   tanh                                           count_if
    array_max                                  date_from_unix_date                        lag                                        pow                                        timestamp                                      count_min_sketch
    array_min                                  date_part                                  last                                       :spark:func:`power`                        timestamp_micros                               covar_pop
    array_position                             date_sub                                   last_day                                   printf                                     timestamp_millis                               covar_samp
    array_remove                               date_trunc                                 last_value                                 quarter                                    timestamp_seconds                              every
    array_repeat                               datediff                                   lcase                                      radians                                    tinyint                                        first
    :spark:func:`array_sort`                   day                                        lead                                       raise_error                                to_csv                                         first_value
    array_union                                dayofmonth                                 :spark:func:`least`                        :spark:func:`rand`                         to_date                                        grouping
    arrays_overlap                             dayofweek                                  left                                       randn                                      to_json                                        grouping_id
    arrays_zip                                 dayofyear                                  :spark:func:`length`                       random                                     to_timestamp                                   histogram_numeric
    :spark:func:`ascii`                        decimal                                    levenshtein                                range                                      to_unix_timestamp                              kurtosis
    asin                                       decode                                     like                                       rank                                       to_utc_timestamp                               :spark:func:`last`
    asinh                                      degrees                                    ln                                         reflect                                    :spark:func:`transform`                        last_value
    assert_true                                dense_rank                                 locate                                     regexp                                     transform_keys                                 max
    atan                                       div                                        log                                        :spark:func:`regexp_extract`               transform_values                               max_by
    atan2                                      double                                     log10                                      regexp_extract_all                         translate                                      mean
    atanh                                      e                                          log1p                                      regexp_like                                trim                                           min
    avg                                        :spark:func:`element_at`                   log2                                       regexp_replace                             trunc                                          min_by
    base64                                     elt                                        :spark:func:`lower`                        repeat                                     try_add                                        percentile
    :spark:func:`between`                      encode                                     lpad                                       :spark:func:`replace`                      try_divide                                     percentile_approx
    bigint                                     every                                      ltrim                                      reverse                                    typeof                                         regr_avgx
    bin                                        exists                                     make_date                                  right                                      ucase                                          regr_avgy
    binary                                     :spark:func:`exp`                          make_dt_interval                           rint                                       unbase64                                       regr_count
    bit_and                                    explode                                    make_interval                              :spark:func:`rlike`                        unhex                                          regr_r2
    bit_count                                  explode_outer                              make_timestamp                             :spark:func:`round`                        unix_date                                      skewness
    bit_get                                    expm1                                      make_ym_interval                           row_number                                 unix_micros                                    some
    bit_length                                 extract                                    :spark:func:`map`                          rpad                                       unix_millis                                    std
    bit_or                                     factorial                                  map_concat                                 rtrim                                      unix_seconds                                   stddev
    bit_xor                                    :spark:func:`filter`                       map_entries                                schema_of_csv                              unix_timestamp                                 stddev_pop
    bool_and                                   find_in_set                                :spark:func:`map_filter`                   schema_of_json                             :spark:func:`upper`                            stddev_samp
    bool_or                                    first                                      :spark:func:`map_from_arrays`              second                                     uuid                                           sum
    boolean                                    first_value                                map_from_entries                           sentences                                  var_pop                                        try_avg
    bround                                     flatten                                    map_keys                                   sequence                                   var_samp                                       try_sum
    btrim                                      float                                      map_values                                 session_window                             variance                                       var_pop
    cardinality                                :spark:func:`floor`                        map_zip_with                               sha                                        version                                        var_samp
    case                                       forall                                     max                                        :spark:func:`sha1`                         weekday                                        variance
    cast                                       format_number                              max_by                                     :spark:func:`sha2`                         weekofyear
    cbrt                                       format_string                              :spark:func:`md5`                          :spark:func:`shiftleft`                    when
    :spark:func:`ceil`                         from_csv                                   mean                                       :spark:func:`shiftright`                   width_bucket
    ceiling                                    from_json                                  min                                        shiftrightunsigned                         window
    char                                       from_unixtime                              min_by                                     shuffle                                    xpath
    char_length                                from_utc_timestamp                         minute                                     sign                                       xpath_boolean
    character_length                           :spark:func:`get_json_object`              mod                                        signum                                     xpath_double
    :spark:func:`chr`                          getbit                                     monotonically_increasing_id                sin                                        xpath_float
    coalesce                                   :spark:func:`greatest`                     month                                      sinh                                       xpath_int
    collect_list                               grouping                                   months_between                             :spark:func:`size`                         xpath_long
    collect_set                                grouping_id                                named_struct                               skewness                                   xpath_number
    :spark:func:`concat`                       :spark:func:`hash`                         nanvl                                      slice                                      xpath_short
    concat_ws                                  hex                                        negative                                   smallint                                   xpath_string
    conv                                       hour                                       next_day                                   some                                       :spark:func:`xxhash64`
    corr                                       hypot                                      :spark:func:`not`                          :spark:func:`sort_array`                   :spark:func:`year`
    cos                                        if                                         now                                        soundex                                    zip_with
    cosh                                       ifnull                                     nth_value                                  space
    cot                                        :spark:func:`in`                           ntile                                      spark_partition_id
    count                                      initcap                                    nullif                                     :spark:func:`split`
    =========================================  =========================================  =========================================  =========================================  =========================================  ==  =========================================
