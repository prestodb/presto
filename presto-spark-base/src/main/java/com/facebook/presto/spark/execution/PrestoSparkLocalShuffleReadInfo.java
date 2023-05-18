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

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class is a 1:1 strict API mapping to LocalShuffleInfo in
 * presto-native-execution/presto_cpp/main/operators/LocalPersistentShuffle.h.
 * Please refrain changes to this API class. If any changes have to be made to
 * this class, one should make sure to make corresponding changes in the above
 * C++ struct and its corresponding serde functionalities.
 */
public class PrestoSparkLocalShuffleReadInfo
        implements PrestoSparkShuffleReadInfo
{
    private final int numPartitions;
    // Presto queryId which is unique to each query.
    private final String queryId;
    // Partition ids which are supposed to be read by given shuffle read.
    private final List<String> partitionIds;
    private final String rootPath;

    @JsonCreator
    public PrestoSparkLocalShuffleReadInfo(
            @JsonProperty("numPartitions") int numPartitions,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("partitionIds") List<String> partitionIds,
            @JsonProperty("rootPath") String rootPath)
    {
        this.numPartitions = numPartitions;
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.partitionIds = requireNonNull(partitionIds, "partitionIds is null");
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
    }

    @JsonProperty
    public int getNumPartitions()
    {
        return numPartitions;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getRootPath()
    {
        return rootPath;
    }

    @JsonProperty
    public List<String> getPartitionIds()
    {
        return partitionIds;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numPartitions", numPartitions)
                .add("queryId", queryId)
                .add("partitionIds", String.join(", ", partitionIds))
                .add("rootPath", rootPath)
                .toString();
    }
}
