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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IndexSourceNode
        extends PlanNode
{
    private final IndexHandle indexHandle;
    private final TableHandle tableHandle;
    private final Optional<TableLayoutHandle> tableLayout; // only necessary for event listeners
    private final Set<Symbol> lookupSymbols;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column
    private final TupleDomain<ColumnHandle> effectiveTupleDomain; // general summary of how the output columns will be constrained

    @JsonCreator
    public IndexSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("indexHandle") IndexHandle indexHandle,
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("tableLayout") Optional<TableLayoutHandle> tableLayout,
            @JsonProperty("lookupSymbols") Set<Symbol> lookupSymbols,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("effectiveTupleDomain") TupleDomain<ColumnHandle> effectiveTupleDomain)
    {
        super(id);
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableLayout = requireNonNull(tableLayout, "tableLayout is null");
        this.lookupSymbols = ImmutableSet.copyOf(requireNonNull(lookupSymbols, "lookupSymbols is null"));
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        this.effectiveTupleDomain = requireNonNull(effectiveTupleDomain, "effectiveTupleDomain is null");
        checkArgument(!lookupSymbols.isEmpty(), "lookupSymbols is empty");
        checkArgument(!outputSymbols.isEmpty(), "outputSymbols is empty");
        checkArgument(assignments.keySet().containsAll(lookupSymbols), "Assignments do not include all lookup symbols");
        checkArgument(outputSymbols.containsAll(lookupSymbols), "Lookup symbols need to be part of the output symbols");
    }

    @JsonProperty
    public IndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Optional<TableLayoutHandle> getLayout()
    {
        return tableLayout;
    }

    @JsonProperty
    public Set<Symbol> getLookupSymbols()
    {
        return lookupSymbols;
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getEffectiveTupleDomain()
    {
        return effectiveTupleDomain;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitIndexSource(this, context);
    }
}
