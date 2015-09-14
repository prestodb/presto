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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class HiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String clientId;
    private final List<HivePartition> partitions;
    private final TupleDomain<ColumnHandle> promisedPredicate;

    @JsonCreator
    public HiveTableLayoutHandle(@JsonProperty("clientId") String clientId, @JsonProperty("promisedPredicate") TupleDomain<ColumnHandle> promisedPredicate)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.partitions = null;
        this.promisedPredicate = requireNonNull(promisedPredicate, "promisedPredicate is null");
    }

    public HiveTableLayoutHandle(String clientId, List<HivePartition> partitions, TupleDomain<ColumnHandle> promisedPredicate)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.promisedPredicate = requireNonNull(promisedPredicate, "promisedPredicate is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if avaiable, Optional.empty() if dropped
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return Optional.ofNullable(partitions);
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPromisedPredicate()
    {
        return promisedPredicate;
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
