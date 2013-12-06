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

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;

@Immutable
public class WindowNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> partitionBy;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;
    private final Map<Symbol, FunctionCall> windowFunctions;
    private final Map<Symbol, FunctionHandle> functionHandles;

    @JsonCreator
    public WindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("orderBy") List<Symbol> orderBy,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("windowFunctions") Map<Symbol, FunctionCall> windowFunctions,
            @JsonProperty("functionHandles") Map<Symbol, FunctionHandle> functionHandles)
    {
        super(id);

        checkNotNull(source, "source is null");
        checkNotNull(partitionBy, "partitionBy is null");
        checkNotNull(orderBy, "orderBy is null");
        checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");
        checkNotNull(windowFunctions, "windowFunctions is null");
        checkNotNull(functionHandles, "functionHandles is null");
        checkArgument(windowFunctions.keySet().equals(functionHandles.keySet()), "windowFunctions does not match functionHandles");

        this.source = source;
        this.partitionBy = ImmutableList.copyOf(partitionBy);
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
        this.functionHandles = ImmutableMap.copyOf(functionHandles);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(concat(source.getOutputSymbols(), windowFunctions.keySet()));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public Map<Symbol, FunctionCall> getWindowFunctions()
    {
        return windowFunctions;
    }

    @JsonProperty
    public Map<Symbol, FunctionHandle> getFunctionHandles()
    {
        return functionHandles;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }
}
