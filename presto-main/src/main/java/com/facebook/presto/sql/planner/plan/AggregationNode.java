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
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Aggregation> aggregations;
    private final List<List<Symbol>> groupingSets;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;
    private final Optional<Symbol> rowTypeSymbol;
    private final List<Symbol> outputSymbols;
    private final Set<Symbol> inputSymbols;
    private final List<Symbol> passThroughSymbols;

    @JsonCreator
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") List<Aggregation> aggregations,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol,
            @JsonProperty("rowTypeSymbol") Optional<Symbol> rowTypeSymbol)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.aggregations = ImmutableList.copyOf(requireNonNull(aggregations, "aggregations is null"));
        requireNonNull(groupingSets, "groupingSets is null");
        checkArgument(!groupingSets.isEmpty(), "grouping sets list cannot be empty");
        this.groupingSets = listOfListsCopy(groupingSets);

        boolean hasOrderBy = aggregations.stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .noneMatch(Optional::isPresent);
        checkArgument(hasOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = requireNonNull(step, "step is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.groupIdSymbol = requireNonNull(groupIdSymbol, "groupIdSymbol is null");
        this.rowTypeSymbol = requireNonNull(rowTypeSymbol, "rowTypeSymbol is null");
        checkArgument(step.isOutputPartial() || !rowTypeSymbol.isPresent(), "rowTypeSymbol is required with partial output");

        if (step.isOutputPartial() && getGroupingKeys().isEmpty()) {
            this.passThroughSymbols = aggregations.stream()
                    .map(Aggregation::getCall)
                    .filter(FunctionCall::isDistinct)
                    .map(SymbolsExtractor::extractUnique)
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(toImmutableList());
        }
        else {
            this.passThroughSymbols = ImmutableList.of();
        }

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        if (step.isOutputPartial() && getGroupingKeys().isEmpty()) {
            rowTypeSymbol.ifPresent(outputSymbols::add);
        }
        outputSymbols.addAll(getGroupingKeys());
        hashSymbol.ifPresent(outputSymbols::add);
        aggregations.stream()
                .map(Aggregation::getOutputSymbol)
                .forEach(outputSymbols::add);
        outputSymbols.addAll(passThroughSymbols);
        this.outputSymbols = outputSymbols.build();

        ImmutableSet.Builder<Symbol> inputSymbols = ImmutableSet.builder();
        inputSymbols.addAll(getGroupingKeys());
        hashSymbol.ifPresent(inputSymbols::add);
        aggregations.stream()
                .map(Aggregation::getInputSymbols)
                .forEach(inputSymbols::addAll);
        this.inputSymbols = inputSymbols.build();
    }

    public List<Symbol> getGroupingKeys()
    {
        List<Symbol> symbols = groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());

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
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step == SINGLE);
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
        return outputSymbols;
    }

    public Set<Symbol> getInputSymbols()
    {
        return inputSymbols;
    }

    public List<Symbol> getPassThroughSymbols()
    {
        return passThroughSymbols;
    }

    @JsonProperty
    public List<Aggregation> getAggregations()
    {
        return aggregations;
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

    @JsonProperty("rowTypeSymbol")
    public Optional<Symbol> getRowTypeSymbol()
    {
        return rowTypeSymbol;
    }

    public boolean hasOrderings()
    {
        return aggregations.stream()
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
        return new AggregationNode(getId(), Iterables.getOnlyElement(newChildren), aggregations, groupingSets, step, hashSymbol, groupIdSymbol, rowTypeSymbol);
    }

    public boolean isDecomposable(FunctionRegistry functionRegistry)
    {
        boolean hasOrderBy = getAggregations().stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = getAggregations().stream()
                .map(Aggregation::getCall)
                .anyMatch(FunctionCall::isDistinct);

        boolean decomposableFunctions = getAggregations().stream()
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
                return PARTIAL;
            }
            else {
                return INTERMEDIATE;
            }
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return INTERMEDIATE;
            }
            else {
                return FINAL;
            }
        }
    }

    public static class Aggregation
    {
        private final Symbol outputSymbol;
        private final FunctionCall call;
        private final Signature signature;
        private final Optional<Symbol> mask;
        private final Set<Symbol> inputSymbols;

        @JsonCreator
        public Aggregation(
                @JsonProperty("outputSymbol") Symbol outputSymbol,
                @JsonProperty("call") FunctionCall call,
                @JsonProperty("signature") Signature signature,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.outputSymbol = requireNonNull(outputSymbol, "outputSymbol is null");
            this.call = requireNonNull(call, "call is null");
            this.signature = requireNonNull(signature, "signature is null");
            this.mask = requireNonNull(mask, "mask is null");

            ImmutableSet.Builder<Symbol> inputSymbols = ImmutableSet.builder();
            inputSymbols.addAll(SymbolsExtractor.extractUnique(call));
            mask.ifPresent(inputSymbols::add);
            this.inputSymbols = inputSymbols.build();
        }

        @JsonProperty
        public Symbol getOutputSymbol()
        {
            return outputSymbol;
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

        public Set<Symbol> getInputSymbols()
        {
            return inputSymbols;
        }
    }
}
