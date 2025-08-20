===========================
Most Used Function Coverage
===========================

Here is a list of most used scalar and aggregate Presto functions with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage td:nth-child(8) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(7) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ===========================  ===========================  ===========================  ===========================  ===========================  ==  ===========================  ==  ===========================
    Scalar Functions                                                                                                                                     Aggregate Functions              Window Functions
    ===============================================================================================================================================  ==  ===========================  ==  ===========================
    :func:`map`                  :func:`date_format`          :func:`element_at`           :func:`strpos`               :func:`arrays_overlap`           :func:`count`                    :func:`row_number`
    :func:`lower`                :func:`json_extract_scalar`  :func:`abs`                  :func:`flatten`              :func:`from_big_endian_64`       :func:`sum`                      :func:`rank`
    :func:`json_format`          :func:`array_sort`           :func:`round`                :func:`trim`                 :func:`md5`                      :func:`max`                      :func:`lag`
    :func:`upper`                :func:`transform_values`     :func:`map_filter`           :func:`map_zip_with`         :func:`xxhash64`                 :func:`array_agg`
    :func:`split`                :func:`map_entries`          :func:`regexp_extract`       :func:`year`                 :func:`to_hex`                   :func:`arbitrary`
    :func:`random`               :func:`concat`               :func:`map_values`           :func:`slice`                :func:`transform_keys`           :func:`min`
    :func:`floor`                :func:`cardinality`          :func:`map_keys`             :func:`month`                :func:`bing_tile_quadkey`        :func:`max_by`
    :func:`contains`             :func:`sequence`             :func:`reduce`               :func:`any_match`            :func:`to_utf8`                  :func:`approx_distinct`
    :func:`map_concat`           :func:`substr`               :func:`greatest`             :func:`bitwise_and`          :func:`crc32`                    :func:`count_if`
    :func:`length`               :func:`date`                 :func:`date_trunc`           :func:`date_parse`           :func:`st_y`                     :func:`approx_percentile`
    :func:`from_unixtime`        :func:`is_nan`               :func:`date_diff`            :func:`bing_tile_at`         :func:`st_x`                     :func:`avg`
    :func:`transform`            :func:`rand`                 :func:`array_max`            :func:`array_union`          now                              :func:`map_agg`
    :func:`to_unixtime`          :func:`filter`               :func:`from_iso8601_date`    :func:`reverse`              :func:`truncate`                 :func:`min_by`
    :func:`regexp_like`          :func:`sqrt`                 :func:`json_extract`         :func:`array_intersect`                                       :func:`stddev`
    :func:`array_join`           :func:`least`                :func:`mod`                  :func:`repeat`                                                :func:`set_agg`
    :func:`replace`              :func:`json_parse`           :func:`array_distinct`       :func:`st_geometryfromtext`                                   :func:`histogram`
    :func:`regexp_replace`       :func:`map_from_entries`     :func:`pow`                  :func:`split_part`                                            :func:`set_union`
    :func:`parse_datetime`       :func:`date_add`             :func:`power`                :func:`log10`                                                 :func:`merge`
    ===========================  ===========================  ===========================  ===========================  ===========================  ==  ===========================  ==  ===========================
