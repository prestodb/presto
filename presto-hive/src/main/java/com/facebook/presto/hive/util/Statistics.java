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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.SUBTRACT;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

public final class Statistics
{
    private Statistics() {}

    private static final Set<String> STATISTICS_PARAMETERS = ImmutableSet.copyOf(createZeroStatistics().toPartitionParameters().keySet());

    public static Table updateStatistics(Table table, HiveBasicStatistics statistics, ReduceOperator operator)
    {
        Map<String, String> parameters = table.getParameters();
        Map<String, String> updatedParameters = updateStatistics(parameters, statistics, operator);
        return Table.builder(table)
                .setParameters(updatedParameters)
                .build();
    }

    public static Partition updateStatistics(Partition partition, HiveBasicStatistics statistics, ReduceOperator operator)
    {
        Map<String, String> parameters = partition.getParameters();
        Map<String, String> updatedParameters = updateStatistics(parameters, statistics, operator);
        return Partition.builder(partition)
                .setParameters(updatedParameters)
                .build();
    }

    public static Map<String, String> updateStatistics(Map<String, String> parameters, HiveBasicStatistics update, ReduceOperator operator)
    {
        HiveBasicStatistics currentStatistics = HiveBasicStatistics.createFromPartitionParameters(parameters);
        HiveBasicStatistics updatedStatistics = reduce(currentStatistics, update, operator);
        Map<String, String> updatedParameters = parameters.entrySet()
                .stream()
                .filter(entry -> !STATISTICS_PARAMETERS.contains(entry.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        updatedParameters.putAll(updatedStatistics.toPartitionParameters());
        return unmodifiableMap(updatedParameters);
    }

    public static HiveBasicStatistics add(HiveBasicStatistics first, HiveBasicStatistics second)
    {
        return reduce(first, second, ADD);
    }

    public static HiveBasicStatistics subtract(HiveBasicStatistics first, HiveBasicStatistics second)
    {
        return reduce(first, second, SUBTRACT);
    }

    public static HiveBasicStatistics reduce(HiveBasicStatistics first, HiveBasicStatistics second, ReduceOperator operator)
    {
        return new HiveBasicStatistics(
                reduce(first.getFileCount(), second.getFileCount(), operator),
                reduce(first.getRowCount(), second.getRowCount(), operator),
                reduce(first.getInMemoryDataSizeInBytes(), second.getInMemoryDataSizeInBytes(), operator),
                reduce(first.getOnDiskDataSizeInBytes(), second.getOnDiskDataSizeInBytes(), operator));
    }

    private static OptionalLong reduce(OptionalLong first, OptionalLong second, ReduceOperator operator)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT:
                    return OptionalLong.of(first.getAsLong() - second.getAsLong());
            }
        }
        return OptionalLong.empty();
    }

    public enum ReduceOperator
    {
        ADD,
        SUBTRACT;

        public ReduceOperator flip()
        {
            switch (this) {
                case SUBTRACT:
                    return ADD;
                case ADD:
                    return SUBTRACT;
                default:
                    throw new UnsupportedOperationException("flip is not implemented for operation type: " + this);
            }
        }
    }
}
