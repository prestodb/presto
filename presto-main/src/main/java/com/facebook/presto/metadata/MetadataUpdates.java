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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MetadataUpdates
{
    public static final MetadataUpdates DEFAULT_METADATA_UPDATES = new MetadataUpdates(null, ImmutableList.of());

    private final ConnectorId connectorId;
    private final List<ConnectorMetadataUpdateHandle> metadataUpdates;

    @JsonCreator
    public MetadataUpdates(
            @JsonProperty("connectorId") @Nullable ConnectorId connectorId,
            @JsonProperty("metadataUpdates") List<ConnectorMetadataUpdateHandle> metadataUpdates)
    {
        this.connectorId = connectorId;
        this.metadataUpdates = ImmutableList.copyOf(requireNonNull(metadataUpdates, "metadataUpdates is null"));
    }

    @JsonProperty
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public List<ConnectorMetadataUpdateHandle> getMetadataUpdates()
    {
        return metadataUpdates;
    }
}
