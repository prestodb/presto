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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class ClpSplit
        implements ConnectorSplit
{
    private final String path;
    private final SplitType type;
    private final Optional<String> kqlQuery;

    @JsonCreator
    public ClpSplit(
            @JsonProperty("path") String path,
            @JsonProperty("type") SplitType type,
            @JsonProperty("kqlQuery") Optional<String> kqlQuery)
    {
        this.path = requireNonNull(path, "Split path is null");
        this.type = requireNonNull(type, "Split type is null");
        this.kqlQuery = kqlQuery;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public SplitType getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getKqlQuery()
    {
        return kqlQuery;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, type, kqlQuery);
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
        ClpSplit other = (ClpSplit) obj;
        return this.type == other.type && this.path.equals(other.path) && this.kqlQuery.equals(other.kqlQuery);
    }

    @Override
    public Map<String, String> getInfo()
    {
        return ImmutableMap.of("path", path, "type", type.toString(), "kqlQuery", kqlQuery.orElse("<null>"));
    }

    public enum SplitType
    {
        ARCHIVE,
        IR,
    }
}
