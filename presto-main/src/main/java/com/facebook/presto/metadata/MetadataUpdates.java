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
package com.facebook.presto.metadata;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.server.thrift.Any;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class MetadataUpdates
{
    public static final MetadataUpdates DEFAULT_METADATA_UPDATES = new MetadataUpdates(null, ImmutableList.of());

    private final ConnectorId connectorId;
    private List<ConnectorMetadataUpdateHandle> metadataUpdates;
    private List<Any> metadataUpdatesAny;
    private boolean dummy;

    @JsonCreator
    public MetadataUpdates(
            @JsonProperty("connectorId") @Nullable ConnectorId connectorId,
            @JsonProperty("metadataUpdates") List<ConnectorMetadataUpdateHandle> metadataUpdates)
    {
        this.connectorId = connectorId;
        this.metadataUpdates = ImmutableList.copyOf(requireNonNull(metadataUpdates, "metadataUpdates is null"));
    }

    /**
     * Thrift constructor
     *
     * @param connectorId id of the connector
     * @param metadataUpdatesAny Any representation of ConnectorMetadataUpdateHandle
     * @param dummy dummy boolean for disambiguating between the JSON constructor
     */
    @ThriftConstructor
    public MetadataUpdates(@Nullable ConnectorId connectorId, List<Any> metadataUpdatesAny, boolean dummy)
    {
        this.connectorId = connectorId;
        this.metadataUpdatesAny = ImmutableList.copyOf(requireNonNull(metadataUpdatesAny, "metadataUpdatesAny is null"));
        this.dummy = dummy;
    }

    @JsonProperty
    @ThriftField(1)
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public List<ConnectorMetadataUpdateHandle> getMetadataUpdates()
    {
        return metadataUpdates;
    }

    @ThriftField(2)
    public List<Any> getMetadataUpdatesAny()
    {
        return metadataUpdatesAny;
    }

    @ThriftField(3)
    public boolean getDummy()
    {
        return dummy;
    }
}
