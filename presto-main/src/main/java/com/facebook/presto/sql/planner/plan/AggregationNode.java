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

import com.facebook.presto.metadata.Signature;
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
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> groupByKeys;
    private final Map<Symbol, FunctionCall> aggregations;
    // Map from function symbol, to the mask symbol
    private final Map<Symbol, Symbol> masks;
    private final Map<Symbol, Signature> functions;
    private final Step step;

    public enum Step
    {
        PARTIAL,
        FINAL,
        SINGLE
    }

    public AggregationNode(PlanNodeId id, PlanNode source, List<Symbol> groupByKeys, Map<Symbol, FunctionCall> aggregations, Map<Symbol, Signature> functions, Map<Symbol, Symbol> masks)
    {
        this(id, source, groupByKeys, aggregations, functions, masks, Step.SINGLE);
    }

    @JsonCreator
    public AggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupBy") List<Symbol> groupByKeys,
            @JsonProperty("aggregations") Map<Symbol, FunctionCall> aggregations,
            @JsonProperty("functions") Map<Symbol, Signature> functions,
            @JsonProperty("masks") Map<Symbol, Symbol> masks,
            @JsonProperty("step") Step step)
    {
        super(id);

        this.source = source;
        this.groupByKeys = ImmutableList.copyOf(checkNotNull(groupByKeys, "groupByKeys is null"));
        this.aggregations = ImmutableMap.copyOf(checkNotNull(aggregations, "aggregations is null"));
        this.functions = ImmutableMap.copyOf(checkNotNull(functions, "functions is null"));
        this.masks = ImmutableMap.copyOf(checkNotNull(masks, "masks is null"));
        for (Symbol mask: masks.keySet()) {
            checkArgument(aggregations.containsKey(mask), "mask does not match any aggregations");
        }
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
    public Map<Symbol, Signature> getFunctions()
    {
        return functions;
    }

    @JsonProperty("masks")
    public Map<Symbol, Symbol> getMasks()
    {
        return masks;
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
