***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/math
    functions/bitwise
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
    :func:`abs`                  divide                       multiply                         :func:`approx_distinct`
    :func:`array_constructor`    :func:`element_at`           negate                           :func:`approx_percentile`
    :func:`array_except`         eq                           neq                              :func:`arbitrary`
    :func:`array_intersect`      :func:`exp`                  not                              :func:`array_agg`
    :func:`array_max`            :func:`filter`               plus                             :func:`avg`
    :func:`array_min`            :func:`floor`                :func:`power`                    :func:`bitwise_and_agg`
    :func:`atan`                 :func:`from_base64`          :func:`rand`                     :func:`bitwise_or_agg`
    :func:`atan2`                :func:`from_hex`             :func:`reduce`                   :func:`bool_and`
    :func:`between`              :func:`from_unixtime`        :func:`regexp_extract`           :func:`bool_or`
    :func:`bitwise_and`          gt                           :func:`regexp_like`              :func:`count`
    :func:`bitwise_not`          gte                          :func:`replace`                  :func:`count_if`
    :func:`bitwise_or`           :func:`in`                   :func:`round`                    :func:`map_agg`
    :func:`bitwise_xor`          :func:`is_null`              :func:`sqrt`                     :func:`max`
    :func:`cardinality`          :func:`json_extract_scalar`  :func:`strpos`                   :func:`max_by`
    :func:`cbrt`                 :func:`length`               :func:`subscript`                :func:`min`
    :func:`ceil`                 :func:`ln`                   :func:`substr`                   :func:`min_by`
    checked_divide               :func:`lower`                :func:`to_base64`                :func:`stddev`
    checked_minus                lt                           :func:`to_hex`                   :func:`stddev_pop`
    checked_modulus              lte                          :func:`to_unixtime`              :func:`stddev_samp`
    checked_multiply             :func:`map`                  :func:`to_utf8`                  :func:`sum`
    checked_negate               :func:`map_concat`           :func:`transform`                :func:`var_pop`
    checked_plus                 :func:`map_entries`          :func:`upper`                    :func:`var_samp`
    :func:`chr`                  :func:`map_filter`           :func:`url_decode`               :func:`variance`
    :func:`clamp`                :func:`map_keys`             :func:`url_encode`
    :func:`coalesce`             :func:`map_values`           :func:`width_bucket`
    :func:`codepoint`            :func:`md5`                  :func:`xxhash64`
    :func:`concat`               minus
    :func:`contains`             modulus
    ===========================  ===========================  ===========================  ==  ===========================
