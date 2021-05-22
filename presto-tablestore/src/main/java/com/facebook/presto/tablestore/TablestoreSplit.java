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
package com.facebook.presto.tablestore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TablestoreSplit
        implements ConnectorSplit
{
    protected final TablestoreTableHandle tableHandle;
    protected final TupleDomain<ColumnHandle> mergedTupleDomain;
    protected final String tupleDomainString;
    protected final Map<String, Set<String>> indexCandidates;
    private Integer maxParallel;
    /**
     * The current parallelId
     */
    private Integer currentParallel;

    /**
     * The related sessionId
     */
    private byte[] sessionId;

    public TablestoreSplit(@JsonProperty("tableHandle") TablestoreTableHandle tableHandle,
            @JsonProperty("mergedTupleDomain") TupleDomain<ColumnHandle> mergedTupleDomain,
            @JsonProperty("tupleDomainString") String tupleDomainString,
            @JsonProperty("indexCandidates") Map<String, Set<String>> indexCandidates)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.mergedTupleDomain = requireNonNull(mergedTupleDomain, "mergedTupleDomain is null");
        this.tupleDomainString = requireNonNull(tupleDomainString, "tupleDomainString is null");
        this.indexCandidates = requireNonNull(indexCandidates, "indexCandidates is null");
    }

    @JsonCreator
    public TablestoreSplit(@JsonProperty("tableHandle") TablestoreTableHandle tableHandle,
            @JsonProperty("mergedTupleDomain") TupleDomain<ColumnHandle> mergedTupleDomain,
            @JsonProperty("tupleDomainString") String tupleDomainString,
            @JsonProperty("indexCandidates") Map<String, Set<String>> indexCandidates,
            @JsonProperty("maxParallel") Integer maxParallel,
            @JsonProperty("currentParallel") Integer currentParallel,
            @JsonProperty("sessionId") byte[] sessionId)
    {
        this(tableHandle, mergedTupleDomain, tupleDomainString, indexCandidates);
        this.maxParallel = maxParallel;
        this.currentParallel = currentParallel;
        this.sessionId = sessionId;
    }

    @JsonProperty
    public TablestoreTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public Object getInfo()
    {
        return "table:" + tableHandle.getPrintableStn() + " ,mergedTupleDomain:" + tupleDomainString;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getMergedTupleDomain()
    {
        return mergedTupleDomain;
    }

    @JsonProperty
    public String getTupleDomainString()
    {
        return tupleDomainString;
    }

    @JsonProperty
    public Map<String, Set<String>> getIndexCandidates()
    {
        return indexCandidates;
    }

    @JsonProperty
    public Integer getMaxParallel()
    {
        return maxParallel;
    }

    @JsonProperty
    public Integer getCurrentParallel()
    {
        return currentParallel;
    }

    @JsonProperty
    public byte[] getSessionId()
    {
        return sessionId;
    }
}
