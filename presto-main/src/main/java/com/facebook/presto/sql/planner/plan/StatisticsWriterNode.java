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

import com.facebook.presto.metadata.AnalyzeTableHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StatisticsWriterNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Symbol rowCountSymbol;
    private final WriteStatisticsTarget target;
    private final boolean rowCountEnabled;
    private final StatisticAggregationsDescriptor<Symbol> descriptor;

    @JsonCreator
    public StatisticsWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") WriteStatisticsTarget target,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol,
            @JsonProperty("rowCountEnabled") boolean rowCountEnabled,
            @JsonProperty("descriptor") StatisticAggregationsDescriptor<Symbol> descriptor)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountSymbol = requireNonNull(rowCountSymbol, "rowCountSymbol is null");
        this.rowCountEnabled = rowCountEnabled;
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public WriteStatisticsTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public StatisticAggregationsDescriptor<Symbol> getDescriptor()
    {
        return descriptor;
    }

    @JsonProperty
    public Symbol getRowCountSymbol()
    {
        return rowCountSymbol;
    }

    @JsonProperty
    public boolean isRowCountEnabled()
    {
        return rowCountEnabled;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(rowCountSymbol);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new StatisticsWriterNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                target,
                rowCountSymbol,
                rowCountEnabled,
                descriptor);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatisticsWriterNode(this, context);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = WriteStatisticsHandle.class, name = "WriteStatisticsHandle")})
    @SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
    public abstract static class WriteStatisticsTarget
    {
        @Override
        public abstract String toString();
    }

    public static class WriteStatisticsHandle
            extends WriteStatisticsTarget
    {
        private final AnalyzeTableHandle handle;

        @JsonCreator
        public WriteStatisticsHandle(@JsonProperty("handle") AnalyzeTableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        @JsonProperty
        public AnalyzeTableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    // only used during planning -- will not be serialized
    public static class WriteStatisticsReference
            extends WriteStatisticsTarget
    {
        private final TableHandle handle;

        public WriteStatisticsReference(TableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }
}
