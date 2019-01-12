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
package io.prestosql.plugin.hive.metastore;

import io.prestosql.plugin.hive.PartitionStatistics;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
import static java.util.Objects.requireNonNull;

public class PartitionWithStatistics
{
    private final Partition partition;
    private final String partitionName;
    private final PartitionStatistics statistics;

    public PartitionWithStatistics(Partition partition, String partitionName, PartitionStatistics statistics)
    {
        this.partition = requireNonNull(partition, "partition is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        checkArgument(toPartitionValues(partitionName).equals(partition.getValues()), "unexpected partition name: %s != %s", partitionName, partition.getValues());
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public Partition getPartition()
    {
        return partition;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public PartitionStatistics getStatistics()
    {
        return statistics;
    }
}
