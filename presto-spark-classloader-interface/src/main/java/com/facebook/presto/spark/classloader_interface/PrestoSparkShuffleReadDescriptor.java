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
package com.facebook.presto.spark.classloader_interface;

import org.apache.spark.Partition;
import org.apache.spark.shuffle.ShuffleHandle;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * {@link PrestoSparkShuffleReadDescriptor} is used as a container to carry over Spark shuffle related meta information
 * (e.g. ShuffleHandle, number of partitions etc.) from Spark related structures (Spark Rdd, Spark Partition etc.)
 * to Presto-Spark land. These meta information are essential to delegate the shuffle operation from JVM to external
 * native execution process.
 */
public class PrestoSparkShuffleReadDescriptor
{
    private final Partition partition;
    private final ShuffleHandle shuffleHandle;
    private final List<String> blockIds;
    private final List<String> partitionIds;
    private final List<Long> partitionSizes;
    private final int numPartitions;

    public PrestoSparkShuffleReadDescriptor(
            Partition partition,
            ShuffleHandle shuffleHandle,
            int numPartitions,
            List<String> blockIds,
            List<String> partitionIds,
            List<Long> partitionSizes)
    {
        this.partition = requireNonNull(partition, "partition is null");
        this.shuffleHandle = requireNonNull(shuffleHandle, "shuffleHandle is null");
        this.blockIds = requireNonNull(blockIds, "blockIds is null");
        this.partitionIds = requireNonNull(partitionIds, "partitionIds is null");
        this.partitionSizes = requireNonNull(partitionSizes, "partitionSizes is null");
        checkArgument(numPartitions > 0, "numPartitions requires larger than 0");
        this.numPartitions = numPartitions;
    }

    public Partition getPartition()
    {
        return partition;
    }

    public List<String> getBlockIds()
    {
        return blockIds;
    }

    public List<String> getPartitionIds()
    {
        return partitionIds;
    }

    public List<Long> getPartitionSizes()
    {
        return partitionSizes;
    }

    public ShuffleHandle getShuffleHandle()
    {
        return shuffleHandle;
    }

    public int getNumPartitions()
    {
        return numPartitions;
    }
}
