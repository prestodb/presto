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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> groupByKeys;
    private final Map<Symbol, FunctionCall> aggregations;
    // Map from function symbol, to the mask symbol
    private final Map<Symbol, Symbol> masks;
    private final List<List<Symbol>> groupingSets;
    private final Map<Symbol, Signature> functions;
    private final Step step;
    private final Optional<Symbol> sampleWeight;
    private final double confidence;
    private final Optional<Symbol> hashSymbol;

    public enum Step
    {
        PARTIAL(true, true),
        FINAL(false, false),
        INTERMEDIATE(false, true),
        SINGLE(true, false);

        private final boolean inputRaw;
        private final boolean outputPartial;

        Step(boolean inputRaw, boolean outputPartial)
        {
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        public boolean isInputRaw()
        {
            return inputRaw;
        }

        public boolean isOutputPartial()
        {
            return outputPartial;
        }
    }

    @JsonCreator
    public AggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupBy") List<Symbol> groupByKeys,
            @JsonProperty("aggregations") Map<Symbol, FunctionCall> aggregations,
            @JsonProperty("functions") Map<Symbol, Signature> functions,
            @JsonProperty("masks") Map<Symbol, Symbol> masks,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("step") Step step,
            @JsonProperty("sampleWeight") Optional<Symbol> sampleWeight,
            @JsonProperty("confidence") double confidence,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id);

        this.source = source;
        this.groupByKeys = ImmutableList.copyOf(requireNonNull(groupByKeys, "groupByKeys is null"));
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.functions = ImmutableMap.copyOf(requireNonNull(functions, "functions is null"));
        this.masks = ImmutableMap.copyOf(requireNonNull(masks, "masks is null"));
        for (Symbol mask : masks.keySet()) {
            checkArgument(aggregations.containsKey(mask), "mask does not match any aggregations");
        }
        this.groupingSets = ImmutableList.copyOf(requireNonNull(groupingSets, "groupingSets is null"));
        this.step = step;
        this.sampleWeight = requireNonNull(sampleWeight, "sampleWeight is null");
        checkArgument(confidence >= 0 && confidence <= 1, "confidence must be in [0, 1]");
        this.confidence = confidence;
        this.hashSymbol = hashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
        symbols.addAll(groupByKeys);
        // output hashSymbol if present
        if (hashSymbol.isPresent()) {
            symbols.add(hashSymbol.get());
        }
        symbols.addAll(aggregations.keySet());
        return symbols.build();
    }

    @JsonProperty("confidence")
    public double getConfidence()
    {
        return confidence;
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

    @JsonProperty("groupingSets")
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
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

    @JsonProperty("sampleWeight")
    public Optional<Symbol> getSampleWeight()
    {
        return sampleWeight;
    }

    @JsonProperty("hashSymbol")
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }
}
