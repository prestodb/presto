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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.NullableValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InformationSchemaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final InformationSchemaTableHandle tableHandle;
    private final Map<String, NullableValue> filters;
    private final List<HostAddress> addresses;

    @JsonCreator
    public InformationSchemaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableHandle") InformationSchemaTableHandle tableHandle,
            @JsonProperty("filters") Map<String, NullableValue> filters,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.filters = requireNonNull(filters, "filters is null");

        requireNonNull(addresses, "hosts is null");
        checkArgument(!addresses.isEmpty(), "hosts is empty");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public InformationSchemaTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Map<String, NullableValue> getFilters()
    {
        return filters;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("tableHandle", tableHandle)
                .add("filters", filters)
                .add("addresses", addresses)
                .toString();
    }
}
