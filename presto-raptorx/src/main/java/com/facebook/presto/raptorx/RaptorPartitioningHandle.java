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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RaptorPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final long distributionId;
    private final List<Long> bucketNodes;

    @JsonCreator
    public RaptorPartitioningHandle(
            @JsonProperty("distributionId") long distributionId,
            @JsonProperty("bucketNodes") List<Long> bucketNodes)
    {
        this.distributionId = distributionId;
        this.bucketNodes = ImmutableList.copyOf(requireNonNull(bucketNodes, "bucketNodes is null"));
    }

    @JsonProperty
    public long getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public List<Long> getBucketNodes()
    {
        return bucketNodes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        RaptorPartitioningHandle that = (RaptorPartitioningHandle) o;
        return (distributionId == that.distributionId) &&
                Objects.equals(bucketNodes, that.bucketNodes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(distributionId, bucketNodes);
    }

    @Override
    public String toString()
    {
        return String.valueOf(distributionId);
    }
}
