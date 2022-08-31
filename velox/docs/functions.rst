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
    functions/json
    functions/url
    functions/aggregate
    functions/window
    functions/hyperloglog

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

    ======================================  ======================================  ======================================  ==  ======================================
    Scalar Functions                                                                                                            Aggregate Functions
    ======================================================================================================================  ==  ======================================
    :func:`abs`                             :func:`e`                               :func:`plus`                                :func:`approx_distinct`               
    :func:`acos`                            :func:`element_at`                      :func:`pow`                                 :func:`approx_most_frequent`          
    :func:`array_constructor`               :func:`empty_approx_set`                :func:`power`                               :func:`approx_percentile`             
    :func:`array_distinct`                  :func:`eq`                              :func:`quarter`                             :func:`approx_set`                    
    :func:`array_duplicates`                :func:`exp`                             :func:`radians`                             :func:`arbitrary`                     
    :func:`array_except`                    :func:`filter`                          :func:`rand`                                :func:`array_agg`                     
    :func:`array_intersect`                 :func:`floor`                           :func:`random`                              :func:`avg`                           
    :func:`array_join`                      :func:`format_datetime`                 :func:`reduce`                              :func:`bitwise_and_agg`               
    :func:`array_max`                       :func:`from_base`                       :func:`regexp_extract`                      :func:`bitwise_or_agg`                
    :func:`array_min`                       :func:`from_base64`                     :func:`regexp_extract_all`                  :func:`bool_and`                      
    :func:`array_position`                  :func:`from_hex`                        :func:`regexp_like`                         :func:`bool_or`                       
    :func:`array_sort`                      :func:`from_unixtime`                   :func:`regexp_replace`                      :func:`checksum`                      
    :func:`arrays_overlap`                  :func:`greatest`                        :func:`replace`                             :func:`corr`                          
    :func:`asin`                            :func:`gt`                              :func:`reverse`                             :func:`count`                         
    :func:`atan`                            :func:`gte`                             :func:`round`                               :func:`count_if`                      
    :func:`atan2`                           :func:`hour`                            :func:`rpad`                                :func:`covar_pop`                     
    :func:`between`                         in                                      :func:`rtrim`                               :func:`covar_samp`                    
    :func:`bit_count`                       :func:`infinity`                        :func:`second`                              :func:`every`                         
    :func:`bitwise_and`                     :func:`is_finite`                       :func:`sha256`                              :func:`histogram`                     
    :func:`bitwise_arithmetic_shift_right`  :func:`is_infinite`                     :func:`sha512`                              :func:`map_agg`                       
    :func:`bitwise_left_shift`              :func:`is_nan`                          :func:`sign`                                :func:`map_union`                     
    :func:`bitwise_logical_shift_right`     :func:`is_null`                         :func:`sin`                                 :func:`max`                           
    :func:`bitwise_not`                     :func:`json_extract_scalar`             :func:`slice`                               :func:`max_by`                        
    :func:`bitwise_or`                      :func:`least`                           :func:`split`                               :func:`max_data_size_for_stats`       
    :func:`bitwise_right_shift`             :func:`length`                          :func:`split_part`                          :func:`merge`                         
    :func:`bitwise_right_shift_arithmetic`  :func:`like`                            :func:`sqrt`                                :func:`min`                           
    :func:`bitwise_shift_left`              :func:`ln`                              :func:`strpos`                              :func:`min_by`                        
    :func:`bitwise_xor`                     :func:`log10`                           :func:`subscript`                           :func:`stddev`                        
    :func:`cardinality`                     :func:`log2`                            :func:`substr`                              :func:`stddev_pop`                    
    :func:`cbrt`                            :func:`lower`                           :func:`tan`                                 :func:`stddev_samp`                   
    :func:`ceil`                            :func:`lpad`                            :func:`tanh`                                :func:`sum`                           
    :func:`ceiling`                         :func:`lt`                              :func:`to_base`                             :func:`var_pop`                       
    :func:`chr`                             :func:`lte`                             :func:`to_base64`                           :func:`var_samp`                      
    :func:`clamp`                           :func:`ltrim`                           :func:`to_hex`                              :func:`variance`                      
    :func:`codepoint`                       :func:`map`                             :func:`to_unixtime`                                                               
    :func:`combinations`                    :func:`map_concat`                      :func:`to_utf8`                                                                   
    :func:`concat`                          :func:`map_concat_empty_nulls`          :func:`transform`                                                                 
    :func:`contains`                        :func:`map_entries`                     :func:`trim`                                                                      
    :func:`cos`                             :func:`map_filter`                      :func:`upper`                                                                     
    :func:`cosh`                            :func:`map_keys`                        :func:`url_decode`                                                                
    :func:`date_add`                        :func:`map_values`                      :func:`url_encode`                                                                
    :func:`date_diff`                       :func:`md5`                             :func:`url_extract_fragment`                                                      
    :func:`date_format`                     :func:`millisecond`                     :func:`url_extract_host`                                                          
    :func:`date_parse`                      :func:`minus`                           :func:`url_extract_parameter`                                                     
    :func:`date_trunc`                      :func:`minute`                          :func:`url_extract_path`                                                          
    :func:`day`                             :func:`mod`                             :func:`url_extract_port`                                                          
    :func:`day_of_month`                    :func:`month`                           :func:`url_extract_protocol`                                                      
    :func:`day_of_week`                     :func:`multiply`                        :func:`url_extract_query`                                                         
    :func:`day_of_year`                     :func:`nan`                             :func:`width_bucket`                                                              
    :func:`degrees`                         :func:`negate`                          :func:`xxhash64`                                                                  
    :func:`distinct_from`                   :func:`neq`                             :func:`year`                                                                      
    :func:`divide`                          not                                     :func:`year_of_week`                                                              
    :func:`dow`                             :func:`parse_datetime`                  :func:`yow`                                                                       
    :func:`doy`                             :func:`pi`                              :func:`zip`                                                                       
    ======================================  ======================================  ======================================  ==  ======================================
