***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/math
    functions/bitwise
    functions/comparison
    functions/string
    functions/datetime
    functions/array
    functions/map
    functions/regexp
    functions/binary
    functions/url
    functions/aggregate

Here is a list of all scalar and aggregate Presto functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/coverage>` and :doc:`most used
<functions/most_used_coverage>` functions for broader context.

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

    ===========================  ===========================  ===========================  ==  ===========================
    Scalar Functions                                                                           Aggregate Functions
    =====================================================================================  ==  ===========================
    :func:`abs`                  :func:`exp`                  plus                             :func:`approx_distinct`
    :func:`acos`                 :func:`filter`               :func:`pow`                      :func:`approx_percentile`
    :func:`array_constructor`    :func:`floor`                :func:`power`                    :func:`arbitrary`
    :func:`array_distinct`       :func:`from_base64`          :func:`radians`                  :func:`array_agg`
    :func:`array_except`         :func:`from_hex`             :func:`rand`                     :func:`avg`
    :func:`array_intersect`      :func:`from_unixtime`        :func:`reduce`                   :func:`bitwise_and_agg`
    :func:`array_max`            gt                           :func:`regexp_extract`           :func:`bitwise_or_agg`
    :func:`array_min`            gte                          :func:`regexp_extract_all`       :func:`bool_and`
    :func:`asin`                 :func:`hour`                 :func:`regexp_like`              :func:`bool_or`
    :func:`atan`                 :func:`in`                   :func:`replace`                  :func:`count`
    :func:`atan2`                :func:`is_null`              :func:`reverse`                  :func:`count_if`
    :func:`between`              :func:`json_extract_scalar`  :func:`round`                    :func:`map_agg`
    :func:`bitwise_and`          :func:`length`               :func:`rtrim`                    :func:`max`
    :func:`bitwise_not`          :func:`like`                 :func:`second`                   :func:`max_by`
    :func:`bitwise_or`           :func:`ln`                   :func:`sin`                      :func:`min`
    :func:`bitwise_xor`          :func:`log10`                :func:`split`                    :func:`min_by`
    :func:`cardinality`          :func:`log2`                 :func:`sqrt`                     :func:`stddev`
    :func:`cbrt`                 :func:`lower`                :func:`strpos`                   :func:`stddev_pop`
    :func:`ceil`                 lt                           :func:`subscript`                :func:`stddev_samp`
    :func:`ceiling`              lte                          :func:`substr`                   :func:`sum`
    :func:`chr`                  :func:`ltrim`                :func:`tan`                      :func:`var_pop`
    :func:`clamp`                :func:`map`                  :func:`tanh`                     :func:`var_samp`
    :func:`coalesce`             :func:`map_concat`           :func:`to_base64`                :func:`variance`
    :func:`codepoint`            :func:`map_entries`          :func:`to_hex`
    :func:`concat`               :func:`map_filter`           :func:`to_unix_timestamp`
    :func:`contains`             :func:`map_keys`             :func:`to_unixtime`
    :func:`cos`                  :func:`map_values`           :func:`to_utf8`
    :func:`cosh`                 :func:`md5`                  :func:`transform`
    :func:`day`                  :func:`millisecond`          :func:`trim`
    :func:`day_of_month`         minus                        :func:`upper`
    :func:`day_of_week`          :func:`minute`               :func:`url_decode`
    :func:`day_of_year`          modulus                      :func:`url_encode`
    divide                       :func:`month`                :func:`width_bucket`
    :func:`dow`                  multiply                     :func:`xxhash64`
    :func:`doy`                  negate                       :func:`year`
    :func:`element_at`           neq
    eq                           not
    ===========================  ===========================  ===========================  ==  ===========================
