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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.FunctionCall;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

/**
 * Optionally validates each of the non-function fields of the node.
 */
public final class WindowMatcher
        implements Matcher
{
    private final Optional<Set<SymbolAlias>> prePartitionedInputs;
    private final Optional<ExpectedValueProvider<WindowNode.Specification>> specification;
    private final Optional<Integer> preSortedOrderPrefix;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private WindowMatcher(
            Optional<Set<SymbolAlias>> prePartitionedInputs,
            Optional<ExpectedValueProvider<WindowNode.Specification>> specification,
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
                        .equals(windowNode.getPrePartitionedInputs()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!specification
                .map(expectedSpecification ->
                        expectedSpecification.getExpectedValue(symbolAliases)
                                .equals(windowNode.getSpecification()))
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
                        .equals(windowNode.getHashSymbol()))
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
        private Optional<ExpectedValueProvider<WindowNode.Specification>> specification = Optional.empty();
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

        public Builder specification(ExpectedValueProvider<WindowNode.Specification> specification)
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
                Signature signature,
                ExpectedValueProvider<WindowNode.Frame> frame)
        {
            windowFunctionMatchers.add(
                    new AliasMatcher(
                            Optional.of(outputAlias),
                            new WindowFunctionMatcher(functionCall, Optional.of(signature), Optional.of(frame))));
            return this;
        }

        /**
         * Matches only if WindowNode.getHashSymbol() is an empty option.
         */
        public Builder hashSymbol()
        {
            this.hashSymbol = Optional.of(Optional.empty());
            return this;
        }

        /**
         * Matches only if WindowNode.getHashSymbol() is a non-empty option containing hashSymbol.
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
