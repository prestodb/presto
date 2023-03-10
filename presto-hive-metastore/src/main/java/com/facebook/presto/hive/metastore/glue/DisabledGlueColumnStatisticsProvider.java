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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.UnaryOperator.identity;

public class DisabledGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider
{
    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ImmutableSet.of();
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(MetastoreContext metastoreContext, Table table)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<Partition, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(MetastoreContext metastoreContext, Collection<Partition> partitions)
    {
        return partitions.stream().collect(toImmutableMap(identity(), partition -> ImmutableMap.of()));
    }

    @Override
    public void updateTableColumnStatistics(MetastoreContext metastoreContext, Table table, Map<String, HiveColumnStatistics> columnStatistics)
    {
        if (!columnStatistics.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore column level statistics are disabled");
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, Set<PartitionStatisticsUpdate> partitionStatisticsUpdates)
    {
        if (partitionStatisticsUpdates.stream().anyMatch(update -> !update.getColumnStatistics().isEmpty())) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore column level statistics are disabled");
        }
    }
}
