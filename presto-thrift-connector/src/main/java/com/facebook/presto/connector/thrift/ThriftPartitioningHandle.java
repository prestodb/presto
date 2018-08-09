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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ThriftPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final int bucketCount;

    @JsonCreator
    public ThriftPartitioningHandle(
            @JsonProperty("bucketCount") int bucketCount)
    {
        this.bucketCount = bucketCount;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftPartitioningHandle other = (ThriftPartitioningHandle) obj;
        return bucketCount == other.bucketCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount);
    }
}
