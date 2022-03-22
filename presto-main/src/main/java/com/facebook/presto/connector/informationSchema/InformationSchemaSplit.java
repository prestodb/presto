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

import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InformationSchemaSplit
        implements ConnectorSplit
{
    private final InformationSchemaTableHandle tableHandle;
    private final Set<QualifiedTablePrefix> prefixes;
    private final List<HostAddress> addresses;

    @JsonCreator
    public InformationSchemaSplit(
            @JsonProperty("tableHandle") InformationSchemaTableHandle tableHandle,
            @JsonProperty("prefixes") Set<QualifiedTablePrefix> prefixes,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.prefixes = ImmutableSet.copyOf(requireNonNull(prefixes, "prefixes is null"));

        requireNonNull(addresses, "hosts is null");
        checkArgument(!addresses.isEmpty(), "hosts is empty");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return addresses;
    }

    @JsonProperty
    public InformationSchemaTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Set<QualifiedTablePrefix> getPrefixes()
    {
        return prefixes;
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
                .add("tableHandle", tableHandle)
                .add("prefixes", prefixes)
                .add("addresses", addresses)
                .toString();
    }
}
