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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public interface GlueColumnStatisticsProvider
{
    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    Map<String, HiveColumnStatistics> getTableColumnStatistics(MetastoreContext metastoreContext, Table table);

    Map<Partition, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(MetastoreContext metastoreContext, Collection<Partition> partitions);

    default Map<String, HiveColumnStatistics> getPartitionColumnStatistics(MetastoreContext metastoreContext, Partition partition)
    {
        return getPartitionColumnStatistics(metastoreContext, ImmutableSet.of(partition)).get(partition);
    }

    void updateTableColumnStatistics(MetastoreContext metastoreContext, Table table, Map<String, HiveColumnStatistics> columnStatistics);

    default void updatePartitionStatistics(MetastoreContext metastoreContext, Partition partition, Map<String, HiveColumnStatistics> columnStatistics)
    {
        updatePartitionStatistics(metastoreContext, ImmutableSet.of(new PartitionStatisticsUpdate(partition, columnStatistics)));
    }

    void updatePartitionStatistics(MetastoreContext metastoreContext, Set<PartitionStatisticsUpdate> partitionStatisticsUpdates);

    class PartitionStatisticsUpdate
    {
        private final Partition partition;
        private final Map<String, HiveColumnStatistics> columnStatistics;

        public PartitionStatisticsUpdate(Partition partition, Map<String, HiveColumnStatistics> columnStatistics)
        {
            this.partition = requireNonNull(partition, "partition is null");
            this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        }

        public Partition getPartition()
        {
            return partition;
        }

        public Map<String, HiveColumnStatistics> getColumnStatistics()
        {
            return columnStatistics;
        }
    }
}
