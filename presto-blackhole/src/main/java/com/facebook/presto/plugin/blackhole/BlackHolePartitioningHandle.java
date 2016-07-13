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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class BlackHolePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final List<Type> partitionChannelTypes;
    private final int bucketCount;

    @JsonCreator
    public BlackHolePartitioningHandle(@JsonProperty("partitionChannelTypes") List<Type> partitionChannelTypes, @JsonProperty("bucketCount") int bucketCount)
    {
        this.partitionChannelTypes = partitionChannelTypes;
        this.bucketCount = bucketCount;
    }

    @JsonProperty
    public List<Type> getPartitionChannelTypes()
    {
        return partitionChannelTypes;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlackHolePartitioningHandle that = (BlackHolePartitioningHandle) o;
        return bucketCount == that.bucketCount &&
                Objects.equals(partitionChannelTypes, that.partitionChannelTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionChannelTypes, bucketCount);
    }

    @Override
    public String toString()
    {
        return "blackhole_" + bucketCount;
    }
}
