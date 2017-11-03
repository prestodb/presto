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
package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TurboniumPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final List<String> bucketToNode;
    private final long splitsPerWorker;
    @JsonCreator
    public TurboniumPartitioningHandle(
            @JsonProperty("bucketToNode") List<String> bucketToNode,
            @JsonProperty("splitsPerWorker") long splitsPerWorker)
    {
        this.bucketToNode = bucketToNode;
        this.splitsPerWorker = splitsPerWorker;
    }

    @JsonProperty
    public List<String> getBucketToNode()
    {
        return bucketToNode;
    }

    @JsonProperty
    public long getSplitsPerWorker()
    {
        return splitsPerWorker;
    }
}
