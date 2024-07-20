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
package com.facebook.presto.spi.statistics;

import java.util.List;

import static java.util.Collections.emptyList;

public enum ColumnStatisticType
{
    MAX_VALUE("max"),
    MAX_VALUE_SIZE_IN_BYTES("max_data_size_for_stats"),
    MIN_VALUE("min"),
    NUMBER_OF_DISTINCT_VALUES("approx_distinct"),
    NUMBER_OF_NON_NULL_VALUES("count"),
    NUMBER_OF_TRUE_VALUES("count_if"),
    TOTAL_SIZE_IN_BYTES("sum_data_size_for_stats"),
    HISTOGRAM("tdigest_agg");
    private final String functionName;

    ColumnStatisticType(String functionName)
    {
        this.functionName = functionName;
    }

    public ColumnStatisticMetadata getColumnStatisticMetadata(String columnName)
    {
        return new ColumnStatisticMetadata(columnName, this, this.functionName, emptyList(), false);
    }

    public ColumnStatisticMetadata getColumnStatisticMetadataWithCustomFunction(String columnName, String functionSql, List<String> columnArguments)
    {
        return new ColumnStatisticMetadata(columnName, this, functionSql, columnArguments, true);
    }
}
