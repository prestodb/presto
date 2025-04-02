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

import com.facebook.presto.metadata.TableFunctionHandle;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableFunctionNode
        extends InternalPlanNode
{
    private final String name;
    private final Map<String, Argument> arguments;
    private final List<VariableReferenceExpression> outputVariables;
    private final List<PlanNode> sources;
    private final List<TableArgumentProperties> tableArgumentProperties;
    private final List<List<String>> copartitioningLists;
    private final TableFunctionHandle handle;

    @JsonCreator
    public TableFunctionNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("arguments") Map<String, Argument> arguments,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("tableArgumentProperties") List<TableArgumentProperties> tableArgumentProperties,
            @JsonProperty("copartitioningLists") List<List<String>> copartitioningLists,
            @JsonProperty("handle") TableFunctionHandle handle)
    {
        this(Optional.empty(), id, Optional.empty(), name, arguments, outputVariables, sources, tableArgumentProperties, copartitioningLists, handle);
    }

    public TableFunctionNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            String name,
            Map<String, Argument> arguments,
            List<VariableReferenceExpression> outputVariables,
            List<PlanNode> sources,
            List<TableArgumentProperties> tableArgumentProperties,
            List<List<String>> copartitioningLists,
            TableFunctionHandle handle)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.name = requireNonNull(name, "name is null");
        this.arguments = ImmutableMap.copyOf(arguments);
        this.outputVariables = ImmutableList.copyOf(outputVariables);
        this.sources = ImmutableList.copyOf(sources);
        this.tableArgumentProperties = ImmutableList.copyOf(tableArgumentProperties);
        this.copartitioningLists = copartitioningLists.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
        this.handle = requireNonNull(handle, "handle is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Map<String, Argument> getArguments()
    {
        return arguments;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();

        variables.addAll(outputVariables);

        for (int i = 0; i < sources.size(); i++) {
            TableArgumentProperties sourceProperties = tableArgumentProperties.get(i);
            if (sourceProperties.passThroughColumns()) {
                variables.addAll(sources.get(i).getOutputVariables());
            }
            else {
                sourceProperties.specification()
                        .map(DataOrganizationSpecification::getPartitionBy)
                        .ifPresent(variables::addAll);
            }
        }

        return variables.build();
    }

    public List<VariableReferenceExpression> getProperOutput()
    {
        return outputVariables;
    }

    @JsonProperty
    public List<TableArgumentProperties> getTableArgumentProperties()
    {
        return tableArgumentProperties;
    }

    @JsonProperty
    public List<List<String>> getCopartitioningLists()
    {
        return copartitioningLists;
    }

    @JsonProperty
    public TableFunctionHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    @Override
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunction(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newSources)
    {
        checkArgument(sources.size() == newSources.size(), "wrong number of new children");
        return new TableFunctionNode(getId(), name, arguments, outputVariables, newSources, tableArgumentProperties, copartitioningLists, handle);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new TableFunctionNode(getSourceLocation(), getId(), statsEquivalentPlanNode, name, arguments, outputVariables, sources, tableArgumentProperties, copartitioningLists, handle);
    }

    public static class TableArgumentProperties
    {
        private final String argumentName;
        private final Multimap<String, VariableReferenceExpression> columnMapping;
        private final boolean rowSemantics;
        private final boolean pruneWhenEmpty;
        private final boolean passThroughColumns;
        private final Optional<DataOrganizationSpecification> specification;

        @JsonCreator
        public TableArgumentProperties(
                @JsonProperty("argumentName") String argumentName,
                @JsonProperty("columnMapping") Multimap<String, VariableReferenceExpression> columnMapping,
                @JsonProperty("rowSemantics") boolean rowSemantics,
                @JsonProperty("pruneWhenEmpty") boolean pruneWhenEmpty,
                @JsonProperty("passThroughColumns") boolean passThroughColumns,
                @JsonProperty("specification") Optional<DataOrganizationSpecification> specification)
        {
            this.argumentName = requireNonNull(argumentName, "argumentName is null");
            this.columnMapping = ImmutableMultimap.copyOf(columnMapping);
            this.rowSemantics = rowSemantics;
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.passThroughColumns = passThroughColumns;
            this.specification = requireNonNull(specification, "specification is null");
        }

        @JsonProperty
        public String getArgumentName()
        {
            return argumentName;
        }

        @JsonProperty
        public Multimap<String, VariableReferenceExpression> getColumnMapping()
        {
            return columnMapping;
        }

        @JsonProperty
        public boolean rowSemantics()
        {
            return rowSemantics;
        }

        @JsonProperty
        public boolean pruneWhenEmpty()
        {
            return pruneWhenEmpty;
        }

        @JsonProperty
        public boolean passThroughColumns()
        {
            return passThroughColumns;
        }

        @JsonProperty
        public Optional<DataOrganizationSpecification> specification()
        {
            return specification;
        }
    }
}
