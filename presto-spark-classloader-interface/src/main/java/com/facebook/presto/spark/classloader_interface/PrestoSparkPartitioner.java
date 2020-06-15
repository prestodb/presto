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

import org.apache.spark.Partitioner;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkPartitioner
        extends Partitioner
{
    private final int numPartitions;

    public PrestoSparkPartitioner(int numPartitions)
    {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions()
    {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key)
    {
        requireNonNull(key, "key is null");
        MutablePartitionId mutablePartitionId = (MutablePartitionId) key;
        int partition = mutablePartitionId.getPartition();
        if (!(partition >= 0 && partition < numPartitions)) {
            throw new IllegalArgumentException(format("Unexpected partition: %s. Total number of partitions: %s.", partition, numPartitions));
        }
        return partition;
    }
}
