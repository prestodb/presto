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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Implementation of GlueStatisticsFetcher that is used when Glue column statistics are disabled.
 * Returns empty results for read operations and throws exceptions for write operations.
 */
public class DisabledGlueStatisticsFetcher
        implements GlueStatisticsFetcher
{
    @Override
    public List<ColumnStatistics> getTableColumnStatistics(Table table)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<Partition, List<ColumnStatistics>> getPartitionColumnStatistics(Set<Partition> partitions)
    {
        ImmutableMap.Builder<Partition, List<ColumnStatistics>> result = ImmutableMap.builder();
        for (Partition partition : partitions) {
            result.put(partition, ImmutableList.of());
        }
        return result.build();
    }

    @Override
    public void updateTableColumnStatistics(Table table, List<ColumnStatistics> columnStatistics)
    {
        if (!columnStatistics.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore column level statistics are disabled");
        }
    }

    @Override
    public void updatePartitionColumnStatistics(Map<Partition, List<ColumnStatistics>> updates)
    {
        boolean hasStatistics = updates.values().stream()
                .anyMatch(stats -> !stats.isEmpty());

        if (hasStatistics) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore column level statistics are disabled");
        }
    }
}
