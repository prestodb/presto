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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, Aggregation> aggregations;
    private final List<List<Symbol>> groupingSets;
    private final List<Symbol> preGroupedSymbols;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;
    private final List<Symbol> outputs;

    @JsonCreator
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") Map<Symbol, Aggregation> aggregations,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("preGroupedSymbols") List<Symbol> preGroupedSymbols,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol)
    {
        super(id);

        this.source = source;
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        requireNonNull(groupingSets, "groupingSets is null");
        checkArgument(!groupingSets.isEmpty(), "grouping sets list cannot be empty");
        this.groupingSets = listOfListsCopy(groupingSets);

        boolean noOrderBy = aggregations.values().stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .noneMatch(Optional::isPresent);
        checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = step;
        this.hashSymbol = hashSymbol;
        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
        checkArgument(preGroupedSymbols.isEmpty() || getGroupingKeys().containsAll(preGroupedSymbols), "Pre-grouped symbols must be a subset of the grouping keys");
        this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        outputs.addAll(getGroupingKeys());
        hashSymbol.ifPresent(outputs::add);
        outputs.addAll(aggregations.keySet());

        this.outputs = outputs.build();
    }

    public List<Symbol> getGroupingKeys()
    {
        List<Symbol> symbols = new ArrayList<>(groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList()));

        groupIdSymbol.ifPresent(symbols::add);
        return symbols;
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public boolean hasDefaultOutput()
    {
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step.equals(SINGLE));
    }

    public boolean hasEmptyGroupingSet()
    {
        return groupingSets.stream().anyMatch(List::isEmpty);
    }

    public boolean hasNonEmptyGroupingSet()
    {
        return groupingSets.stream().anyMatch(symbols -> !symbols.isEmpty());
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public Map<Symbol, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty("groupingSets")
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty("preGroupedSymbols")
    public List<Symbol> getPreGroupedSymbols()
    {
        return preGroupedSymbols;
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

    @JsonProperty("hashSymbol")
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty("groupIdSymbol")
    public Optional<Symbol> getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    public boolean hasOrderings()
    {
        return aggregations.values().stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new AggregationNode(getId(), Iterables.getOnlyElement(newChildren), aggregations, groupingSets, preGroupedSymbols, step, hashSymbol, groupIdSymbol);
    }

    public boolean isDecomposable(FunctionRegistry functionRegistry)
    {
        boolean hasOrderBy = getAggregations().values().stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = getAggregations().values().stream()
                .map(Aggregation::getCall)
                .anyMatch(FunctionCall::isDistinct);

        boolean decomposableFunctions = getAggregations().values().stream()
                .map(Aggregation::getSignature)
                .map(functionRegistry::getAggregateFunctionImplementation)
                .allMatch(InternalAggregationFunction::isDecomposable);

        return !hasOrderBy && !hasDistinct && decomposableFunctions;
    }

    public boolean hasSingleNodeExecutionPreference(FunctionRegistry functionRegistry)
    {
        // There are two kinds of aggregations the have single node execution preference:
        //
        // 1. aggregations with only empty grouping sets like
        //
        // SELECT count(*) FROM lineitem;
        //
        // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
        // since all input have to be aggregated into one line output.
        //
        // 2. aggregations that must produce default output and are not decomposable, we can not distribute them.
        return (hasEmptyGroupingSet() && !hasNonEmptyGroupingSet()) || (hasDefaultOutput() && !isDecomposable(functionRegistry));
    }

    public boolean isStreamable()
    {
        return !preGroupedSymbols.isEmpty() && groupingSets.size() == 1 && !Iterables.getOnlyElement(groupingSets).isEmpty();
    }

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

        public static Step partialOutput(Step step)
        {
            if (step.isInputRaw()) {
                return Step.PARTIAL;
            }
            else {
                return Step.INTERMEDIATE;
            }
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return Step.INTERMEDIATE;
            }
            else {
                return Step.FINAL;
            }
        }
    }

    public static class Aggregation
    {
        private final FunctionCall call;
        private final Signature signature;
        private final Optional<Symbol> mask;

        @JsonCreator
        public Aggregation(
                @JsonProperty("call") FunctionCall call,
                @JsonProperty("signature") Signature signature,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.call = call;
            this.signature = signature;
            this.mask = mask;
        }

        @JsonProperty
        public FunctionCall getCall()
        {
            return call;
        }

        @JsonProperty
        public Signature getSignature()
        {
            return signature;
        }

        @JsonProperty
        public Optional<Symbol> getMask()
        {
            return mask;
        }
    }
}
