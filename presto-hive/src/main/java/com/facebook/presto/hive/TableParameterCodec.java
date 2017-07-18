/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TableParameterCodec
{
    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";
    /**
     * Decode Hive table parameters into additional Presto table properties.
     */
    public Map<String, Object> decode(Map<String, String> tableParameters)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (tableParameters.containsKey(ORC_BLOOM_FILTER_COLUMNS_KEY)) {
            String colums = tableParameters.get(ORC_BLOOM_FILTER_COLUMNS_KEY);
            properties.put(HiveTableProperties.BLOOM_FILTER_COLUMNS, Arrays.asList(colums.split(",")));
        }
        return properties.build();
    }

    /**
     * Encode additional Presto table properties into Hive table parameters.
     */
    public Map<String, String> encode(Map<String, Object> tableProperties)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        List<String> colums = (List) tableProperties.get(HiveTableProperties.BLOOM_FILTER_COLUMNS);
        if (tableProperties.containsKey(HiveTableProperties.BLOOM_FILTER_COLUMNS) && colums.size() > 0) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(colums));
            properties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(tableProperties.get(HiveTableProperties.BLOOM_FILTER_FPP)));
        }
        return properties.build();
    }
}
