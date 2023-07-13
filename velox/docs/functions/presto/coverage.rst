=================
Function Coverage
=================

Here is a list of all scalar and aggregate Presto functions with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage td:nth-child(8) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(3) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions                           Window Functions
    ================================================================================================================================================================================================================  ==  ========================================  ==  ========================================
    :func:`abs`                               :func:`date_diff`                         :func:`is_nan`                            :func:`replace`                           st_startpoint                                 :func:`approx_distinct`                       :func:`cume_dist`
    :func:`acos`                              :func:`date_format`                       is_subnet_of                              :func:`reverse`                           st_symdifference                              :func:`approx_most_frequent`                  :func:`dense_rank`
    :func:`all_match`                         :func:`date_parse`                        jaccard_index                             rgb                                       st_touches                                    :func:`approx_percentile`                     :func:`first_value`
    :func:`any_match`                         :func:`date_trunc`                        :func:`json_array_contains`               :func:`round`                             st_union                                      :func:`approx_set`                            :func:`lag`
    :func:`array_average`                     :func:`day`                               json_array_get                            :func:`rpad`                              st_within                                     :func:`arbitrary`                             :func:`last_value`
    :func:`array_distinct`                    :func:`day_of_month`                      :func:`json_array_length`                 :func:`rtrim`                             st_x                                          :func:`array_agg`                             :func:`lead`
    :func:`array_duplicates`                  :func:`day_of_week`                       json_extract                              scale_qdigest                             st_xmax                                       :func:`avg`                                   :func:`nth_value`
    :func:`array_except`                      :func:`day_of_year`                       :func:`json_extract_scalar`               :func:`second`                            st_xmin                                       :func:`bitwise_and_agg`                       :func:`ntile`
    :func:`array_frequency`                   :func:`degrees`                           :func:`json_format`                       :func:`sequence`                          st_y                                          :func:`bitwise_or_agg`                        :func:`percent_rank`
    :func:`array_has_duplicates`              :func:`dow`                               :func:`json_parse`                        :func:`sha1`                              st_ymax                                       :func:`bool_and`                              :func:`rank`
    :func:`array_intersect`                   :func:`doy`                               :func:`json_size`                         :func:`sha256`                            st_ymin                                       :func:`bool_or`                               :func:`row_number`
    :func:`array_join`                        :func:`e`                                 :func:`least`                             :func:`sha512`                            :func:`strpos`                                :func:`checksum`
    :func:`array_max`                         :func:`element_at`                        :func:`length`                            :func:`shuffle`                           :func:`strrpos`                               classification_fall_out
    :func:`array_min`                         :func:`empty_approx_set`                  levenshtein_distance                      :func:`sign`                              :func:`substr`                                classification_miss_rate
    :func:`array_normalize`                   enum_key                                  line_interpolate_point                    simplify_geometry                         :func:`tan`                                   classification_precision
    :func:`array_position`                    :func:`exp`                               line_locate_point                         :func:`sin`                               :func:`tanh`                                  classification_recall
    array_remove                              expand_envelope                           :func:`ln`                                :func:`slice`                             :func:`timezone_hour`                         classification_thresholds
    :func:`array_sort`                        features                                  localtime                                 spatial_partitions                        :func:`timezone_minute`                       convex_hull_agg
    :func:`array_sort_desc`                   :func:`filter`                            localtimestamp                            :func:`split`                             :func:`to_base`                               :func:`corr`
    :func:`array_sum`                         flatten                                   :func:`log10`                             :func:`split_part`                        :func:`to_base64`                             :func:`count`
    array_union                               flatten_geometry_collections              :func:`log2`                              split_to_map                              :func:`to_base64url`                          :func:`count_if`
    :func:`arrays_overlap`                    :func:`floor`                             :func:`lower`                             split_to_multimap                         :func:`to_big_endian_32`                      :func:`covar_pop`
    :func:`asin`                              fnv1_32                                   :func:`lpad`                              :func:`spooky_hash_v2_32`                 :func:`to_big_endian_64`                      :func:`covar_samp`
    :func:`atan`                              fnv1_64                                   :func:`ltrim`                             :func:`spooky_hash_v2_64`                 to_geometry                                   differential_entropy
    :func:`atan2`                             fnv1a_32                                  :func:`map`                               :func:`sqrt`                              :func:`to_hex`                                entropy
    bar                                       fnv1a_64                                  :func:`map_concat`                        st_area                                   to_ieee754_32                                 evaluate_classifier_predictions
    :func:`beta_cdf`                          :func:`format_datetime`                   :func:`map_entries`                       st_asbinary                               :func:`to_ieee754_64`                         :func:`every`
    bing_tile                                 :func:`from_base`                         :func:`map_filter`                        st_astext                                 to_iso8601                                    geometric_mean
    bing_tile_at                              :func:`from_base64`                       map_from_entries                          st_boundary                               to_milliseconds                               geometry_union_agg
    bing_tile_children                        :func:`from_base64url`                    :func:`map_keys`                          st_buffer                                 to_spherical_geography                        :func:`histogram`
    bing_tile_coordinates                     :func:`from_big_endian_32`                map_normalize                             st_centroid                               :func:`to_unixtime`                           khyperloglog_agg
    bing_tile_parent                          :func:`from_big_endian_64`                :func:`map_values`                        st_contains                               :func:`to_utf8`                               kurtosis
    bing_tile_polygon                         :func:`from_hex`                          :func:`map_zip_with`                      st_convexhull                             :func:`transform`                             learn_classifier
    bing_tile_quadkey                         from_ieee754_32                           :func:`md5`                               st_coorddim                               :func:`transform_keys`                        learn_libsvm_classifier
    bing_tile_zoom_level                      from_ieee754_64                           merge_hll                                 st_crosses                                :func:`transform_values`                      learn_libsvm_regressor
    bing_tiles_around                         from_iso8601_date                         merge_khll                                st_difference                             :func:`trim`                                  learn_regressor
    :func:`binomial_cdf`                      from_iso8601_timestamp                    :func:`millisecond`                       st_dimension                              :func:`truncate`                              make_set_digest
    :func:`bit_count`                         :func:`from_unixtime`                     :func:`minute`                            st_disjoint                               typeof                                        :func:`map_agg`
    :func:`bitwise_and`                       :func:`from_utf8`                         :func:`mod`                               st_distance                               uniqueness_distribution                       :func:`map_union`
    :func:`bitwise_arithmetic_shift_right`    geometry_as_geojson                       :func:`month`                             st_endpoint                               :func:`upper`                                 :func:`map_union_sum`
    :func:`bitwise_left_shift`                geometry_from_geojson                     multimap_from_entries                     st_envelope                               :func:`url_decode`                            :func:`max`
    :func:`bitwise_logical_shift_right`       geometry_invalid_reason                   myanmar_font_encoding                     st_envelopeaspts                          :func:`url_encode`                            :func:`max_by`
    :func:`bitwise_not`                       geometry_nearest_points                   myanmar_normalize_unicode                 st_equals                                 :func:`url_extract_fragment`                  :func:`merge`
    :func:`bitwise_or`                        geometry_to_bing_tiles                    :func:`nan`                               st_exteriorring                           :func:`url_extract_host`                      merge_set_digest
    :func:`bitwise_right_shift`               geometry_to_dissolved_bing_tiles          ngrams                                    st_geometries                             :func:`url_extract_parameter`                 :func:`min`
    :func:`bitwise_right_shift_arithmetic`    geometry_union                            :func:`none_match`                        st_geometryfromtext                       :func:`url_extract_path`                      :func:`min_by`
    :func:`bitwise_shift_left`                great_circle_distance                     :func:`normal_cdf`                        st_geometryn                              :func:`url_extract_port`                      multimap_agg
    :func:`bitwise_xor`                       :func:`greatest`                          normalize                                 st_geometrytype                           :func:`url_extract_protocol`                  numeric_histogram
    :func:`cardinality`                       hamming_distance                          now                                       st_geomfrombinary                         :func:`url_extract_query`                     qdigest_agg
    cauchy_cdf                                hash_counts                               :func:`parse_datetime`                    st_interiorringn                          value_at_quantile                             reduce_agg
    :func:`cbrt`                              :func:`hmac_md5`                          parse_duration                            st_interiorrings                          values_at_quantiles                           :func:`regr_intercept`
    :func:`ceil`                              :func:`hmac_sha1`                         parse_presto_data_size                    st_intersection                           :func:`week`                                  :func:`regr_slope`
    :func:`ceiling`                           :func:`hmac_sha256`                       :func:`pi`                                st_intersects                             :func:`week_of_year`                          set_agg
    chi_squared_cdf                           :func:`hmac_sha512`                       poisson_cdf                               st_isclosed                               weibull_cdf                                   set_union
    :func:`chr`                               :func:`hour`                              :func:`pow`                               st_isempty                                :func:`width_bucket`                          skewness
    classify                                  :func:`infinity`                          :func:`power`                             st_isring                                 wilson_interval_lower                         spatial_partitioning
    :func:`codepoint`                         intersection_cardinality                  quantile_at_value                         st_issimple                               wilson_interval_upper                         :func:`stddev`
    color                                     inverse_beta_cdf                          :func:`quarter`                           st_isvalid                                word_stem                                     :func:`stddev_pop`
    :func:`combinations`                      inverse_binomial_cdf                      :func:`radians`                           st_length                                 :func:`xxhash64`                              :func:`stddev_samp`
    :func:`concat`                            inverse_cauchy_cdf                        :func:`rand`                              st_linefromtext                           :func:`year`                                  :func:`sum`
    :func:`contains`                          inverse_chi_squared_cdf                   :func:`random`                            st_linestring                             :func:`year_of_week`                          tdigest_agg
    :func:`cos`                               inverse_normal_cdf                        :func:`reduce`                            st_multipoint                             :func:`yow`                                   :func:`var_pop`
    :func:`cosh`                              inverse_poisson_cdf                       :func:`regexp_extract`                    st_numgeometries                          :func:`zip`                                   :func:`var_samp`
    cosine_similarity                         inverse_weibull_cdf                       :func:`regexp_extract_all`                st_numinteriorring                        :func:`zip_with`                              :func:`variance`
    :func:`crc32`                             ip_prefix                                 :func:`regexp_like`                       st_numpoints
    :func:`current_date`                      ip_subnet_max                             :func:`regexp_replace`                    st_overlaps
    current_time                              ip_subnet_min                             regexp_split                              st_point
    current_timestamp                         ip_subnet_range                           regress                                   st_pointn
    current_timezone                          :func:`is_finite`                         reidentification_potential                st_points
    :func:`date`                              :func:`is_infinite`                       render                                    st_polygon
    :func:`date_add`                          :func:`is_json_scalar`                    :func:`repeat`                            st_relate
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
