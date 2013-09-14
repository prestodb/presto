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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.concat;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> groupByKeys;
    private final Map<Symbol, FunctionCall> aggregations;
    private final Map<Symbol, FunctionHandle> functions;
    private final Step step;

    public enum Step
    {
        PARTIAL,
        FINAL,
        SINGLE
    }

    public AggregationNode(PlanNodeId id, PlanNode source, List<Symbol> groupByKeys, Map<Symbol, FunctionCall> aggregations, Map<Symbol, FunctionHandle> functions)
    {
        this(id, source, groupByKeys, aggregations, functions, Step.SINGLE);
    }

    @JsonCreator
    public AggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupBy") List<Symbol> groupByKeys,
            @JsonProperty("aggregations") Map<Symbol, FunctionCall> aggregations,
            @JsonProperty("functions") Map<Symbol, FunctionHandle> functions,
            @JsonProperty("step") Step step)
    {
        super(id);

        this.source = source;
        this.groupByKeys = groupByKeys;
        this.aggregations = aggregations;
        this.functions = functions;
        this.step = step;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(concat(groupByKeys, aggregations.keySet()));
    }

    @JsonProperty("aggregations")
    public Map<Symbol, FunctionCall> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty("functions")
    public Map<Symbol, FunctionHandle> getFunctions()
    {
        return functions;
    }

    @JsonProperty("groupBy")
    public List<Symbol> getGroupBy()
    {
        return groupByKeys;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }
}
