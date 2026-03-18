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
package com.facebook.presto.hive.metastore;

import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.google.common.base.Preconditions.checkArgument;
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
        List<String> nameValues = toPartitionValues(partitionName);
        // Allow partition values to be longer than what the name produces.
        // This happens with partition spec evolution: old partitions have short
        // names but Metastore pads the values list with __HIVE_DEFAULT_PARTITION__
        // for keys added after the partition was created.
        checkArgument(
                partition.getValues().size() >= nameValues.size()
                        && partition.getValues().subList(0, nameValues.size()).equals(nameValues),
                "unexpected partition name %s: name-parsed values %s are not a prefix of partition values %s",
                partitionName, nameValues, partition.getValues());
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
