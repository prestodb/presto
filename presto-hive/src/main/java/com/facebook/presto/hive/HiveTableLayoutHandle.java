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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class HiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String clientId;
    private final List<HivePartition> partitions;

    @JsonCreator
    public HiveTableLayoutHandle(@JsonProperty("clientId") String clientId)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.partitions = null;
    }

    public HiveTableLayoutHandle(String clientId, List<HivePartition> partitions)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonIgnore
    public List<HivePartition> getPartitions()
    {
        checkState(partitions != null, "Partitions dropped by serialization");
        return partitions;
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
        HiveTableLayoutHandle that = (HiveTableLayoutHandle) o;
        return Objects.equals(clientId, that.clientId) &&
                Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clientId, partitions);
    }

    @Override
    public String toString()
    {
        return clientId.toString();
    }
}
