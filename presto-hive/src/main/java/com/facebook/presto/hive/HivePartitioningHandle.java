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

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HivePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final String clientId;
    private final int bucketCount;
    private final List<HiveType> hiveTypes;

    @JsonCreator
    public HivePartitioningHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("hiveTypes") List<HiveType> hiveTypes)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.bucketCount = bucketCount;
        this.hiveTypes = requireNonNull(hiveTypes, "hiveTypes is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<HiveType> getHiveTypes()
    {
        return hiveTypes;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("clientId", clientId)
                .add("bucketCount", bucketCount)
                .add("hiveTypes", hiveTypes)
                .toString();
    }
}
