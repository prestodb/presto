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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.SpecificationProvider.matchSpecification;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Optionally validates each of the non-function fields of the node.
 */
public final class WindowMatcher
        implements Matcher
{
    private final Optional<Set<SymbolAlias>> prePartitionedInputs;
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    private final Optional<Integer> preSortedOrderPrefix;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private WindowMatcher(
            Optional<Set<SymbolAlias>> prePartitionedInputs,
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Optional<Integer> preSortedOrderPrefix,
            Optional<Optional<SymbolAlias>> hashSymbol)
    {
        this.prePartitionedInputs = requireNonNull(prePartitionedInputs, "prePartitionedInputs is null");
        this.specification = requireNonNull(specification, "specification is null");
        this.preSortedOrderPrefix = requireNonNull(preSortedOrderPrefix, "preSortedOrderPrefix is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof WindowNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        WindowNode windowNode = (WindowNode) node;

        if (!prePartitionedInputs
                .map(expectedInputs -> expectedInputs.stream()
                        .map(alias -> alias.toSymbol(symbolAliases))
                        .collect(toImmutableSet())
                        .equals(windowNode.getPrePartitionedInputs().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableSet())))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!specification
                .map(expectedSpecification -> matchSpecification(windowNode.getSpecification(), expectedSpecification.getExpectedValue(symbolAliases)) ||
                        (expectedSpecification instanceof SpecificationProvider && matchSpecification(windowNode.getSpecification(), (SpecificationProvider) expectedSpecification)))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!preSortedOrderPrefix
                .map(Integer.valueOf(windowNode.getPreSortedOrderPrefix())::equals)
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!hashSymbol
                .map(expectedHashSymbol -> expectedHashSymbol
                        .map(alias -> alias.toSymbol(symbolAliases))
                        .map(Symbol::getName)
                        .equals(windowNode.getHashVariable().map(VariableReferenceExpression::getName)))
                .orElse(true)) {
            return NO_MATCH;
        }

        /*
         * Window functions produce a symbol (the result of the function call) that we might
         * want to bind to an alias so we can reference it further up the tree. As such,
         * they need to be matched with an Alias matcher so we can bind the symbol if desired.
         */
        return match();
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("prePartitionedInputs", prePartitionedInputs.orElse(null))
                .add("specification", specification.orElse(null))
                .add("preSortedOrderPrefix", preSortedOrderPrefix.orElse(null))
                .add("hashSymbol", hashSymbol.orElse(null))
                .toString();
    }

    /**
     * By default, matches any WindowNode.  Users add additional constraints by
     * calling the various member functions of the Builder, typically named according
     * to the field names of WindowNode.
     */
    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<Set<SymbolAlias>> prePartitionedInputs = Optional.empty();
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
        private Optional<Integer> preSortedOrderPrefix = Optional.empty();
        private List<AliasMatcher> windowFunctionMatchers = new LinkedList<>();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder prePartitionedInputs(Set<String> prePartitionedInputs)
        {
            requireNonNull(prePartitionedInputs, "prePartitionedInputs is null");
            this.prePartitionedInputs = Optional.of(
                    prePartitionedInputs.stream()
                            .map(SymbolAlias::new)
                            .collect(toImmutableSet()));
            return this;
        }

        public Builder specification(
                List<String> partitionBy,
                List<String> orderBy,
                Map<String, SortOrder> orderings)
        {
            return specification(PlanMatchPattern.specification(partitionBy, orderBy, orderings));
        }

        public Builder specification(ExpectedValueProvider<DataOrganizationSpecification> specification)
        {
            requireNonNull(specification, "specification is null");
            this.specification = Optional.of(specification);
            return this;
        }

        public Builder preSortedOrderPrefix(int preSortedOrderPrefix)
        {
            this.preSortedOrderPrefix = Optional.of(preSortedOrderPrefix);
            return this;
        }

        public Builder addFunction(String outputAlias, ExpectedValueProvider<FunctionCall> functionCall)
        {
            return addFunction(Optional.of(outputAlias), functionCall);
        }

        public Builder addFunction(ExpectedValueProvider<FunctionCall> functionCall)
        {
            return addFunction(Optional.empty(), functionCall);
        }

        private Builder addFunction(Optional<String> outputAlias, ExpectedValueProvider<FunctionCall> functionCall)
        {
            windowFunctionMatchers.add(new AliasMatcher(outputAlias, new WindowFunctionMatcher(functionCall, Optional.empty(), Optional.empty())));
            return this;
        }

        public Builder addFunction(
                String outputAlias,
                ExpectedValueProvider<FunctionCall> functionCall,
                FunctionHandle functionHandle,
                ExpectedValueProvider<WindowNode.Frame> frame)
        {
            windowFunctionMatchers.add(
                    new AliasMatcher(
                            Optional.of(outputAlias),
                            new WindowFunctionMatcher(functionCall, Optional.of(functionHandle), Optional.of(frame))));
            return this;
        }

        /**
         * Matches only if WindowNode.getHashVariable() is a non-empty option containing hashVariable.
         */
        public Builder hashSymbol(String hashSymbol)
        {
            requireNonNull(hashSymbol, "hashSymbol is null");
            this.hashSymbol = Optional.of(Optional.of(new SymbolAlias(hashSymbol)));
            return this;
        }

        PlanMatchPattern build()
        {
            PlanMatchPattern result = node(WindowNode.class, source).with(
                    new WindowMatcher(
                            prePartitionedInputs,
                            specification,
                            preSortedOrderPrefix,
                            hashSymbol));
            windowFunctionMatchers.forEach(result::with);
            return result;
        }
    }
}
