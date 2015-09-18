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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalFileSplit
        implements ConnectorSplit
{
    private final LocalFileConnectorId connectorId;
    private final HostAddress address;
    private final URI uri;

    @JsonCreator
    public LocalFileSplit(
            @JsonProperty("connectorId") LocalFileConnectorId connectorId,
            @JsonProperty("address") HostAddress address,
            @JsonProperty("uri") URI uri)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.address = requireNonNull(address, "address is null");
        this.uri = requireNonNull(uri, "uri is null");
    }

    @JsonProperty
    public LocalFileConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("address", address)
                .add("uri", uri)
                .toString();
    }
}
