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

public class PrestoSparkShuffleReadDescriptor
        extends PrestoSparkShuffleDescriptor
{
    private final Partition partition;
    private final ShuffleHandle shuffleHandle;
    private final List<String> blockIds;
    private final List<Long> partitionSizes;

    public PrestoSparkShuffleReadDescriptor(Partition partition, ShuffleHandle shuffleHandle, int numberOfPartitions, List<String> blockIds, List<Long> partitionSizes)
    {
        super(shuffleHandle, numberOfPartitions);
        this.partition = partition;
        this.shuffleHandle = shuffleHandle;
        this.blockIds = blockIds;
        this.partitionSizes = partitionSizes;
    }

    public Partition getPartition()
    {
        return partition;
    }

    public List<String> getBlockIds()
    {
        return blockIds;
    }

    public List<Long> getPartitionSizes()
    {
        return partitionSizes;
    }

    public ShuffleHandle getShuffleHandle()
    {
        return shuffleHandle;
    }
}
