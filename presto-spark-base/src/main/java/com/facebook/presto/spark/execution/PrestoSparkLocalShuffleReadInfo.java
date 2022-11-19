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
package com.facebook.presto.spark.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PrestoSparkLocalShuffleReadInfo
        implements PrestoSparkShuffleReadInfo
{
    private final long maxBytesPerPartition;
    private final int numPartitions;
    private final int partitionId;
    private final String rootPath;

    @JsonCreator
    public PrestoSparkLocalShuffleReadInfo(
            @JsonProperty("maxBytesPerPartition") long maxBytesPerPartition,
            @JsonProperty("numPartitions") int numPartitions,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("rootPath") String rootPath)
    {
        this.maxBytesPerPartition = maxBytesPerPartition;
        this.numPartitions = numPartitions;
        this.partitionId = partitionId;
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
    }

    @JsonProperty
    public long getMaxBytesPerPartition()
    {
        return maxBytesPerPartition;
    }

    @JsonProperty
    public int getNumPartitions()
    {
        return numPartitions;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public String getRootPath()
    {
        return rootPath;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxBytesPerPartition", maxBytesPerPartition)
                .add("numPartitions", numPartitions)
                .add("partitionId", partitionId)
                .add("rootPath", rootPath)
                .toString();
    }
}
