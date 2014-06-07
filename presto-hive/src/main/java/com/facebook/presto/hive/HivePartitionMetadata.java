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
package com.facebook.presto.hive;

import org.apache.hadoop.hive.metastore.api.Partition;

import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartitionMetadata
{
    private final Partition partition;
    private final HivePartition hivePartition;

    HivePartitionMetadata(HivePartition hivePartition, Partition partition)
    {
        this.partition = checkNotNull(partition, "partition is null");
        this.hivePartition = checkNotNull(hivePartition, "hivePartition is null");
    }

    public HivePartition getHivePartition()
    {
        return hivePartition;
    }

    public Partition getPartition()
    {
        return partition;
    }
}
