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
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.SpecificationProvider.matchSpecification;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TopNRowNumberMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    private final Optional<SymbolAlias> rowNumberSymbol;
    private final Optional<Integer> maxRowCountPerPartition;
    private final Optional<Boolean> partial;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private TopNRowNumberMatcher(
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Optional<SymbolAlias> rowNumberSymbol,
            Optional<Integer> maxRowCountPerPartition,
            Optional<Boolean> partial,
            Optional<Optional<SymbolAlias>> hashSymbol)
    {
        this.specification = requireNonNull(specification, "specification is null");
        this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        this.maxRowCountPerPartition = requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        this.partial = requireNonNull(partial, "partial is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashVariable is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TopNRowNumberNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TopNRowNumberNode topNRowNumberNode = (TopNRowNumberNode) node;

        if (!specification
                .map(expectedSpecification -> matchSpecification(topNRowNumberNode.getSpecification(), expectedSpecification.getExpectedValue(symbolAliases)))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!rowNumberSymbol
                .map(expectedRowNumberSymbol ->
                        expectedRowNumberSymbol.toSymbol(symbolAliases).getName()
                                .equals(topNRowNumberNode.getRowNumberVariable().getName()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!maxRowCountPerPartition
                .map(expectedMaxRowCountPerPartition -> expectedMaxRowCountPerPartition.equals(topNRowNumberNode.getMaxRowCountPerPartition()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!partial
                .map(expectedPartial -> expectedPartial.equals(topNRowNumberNode.isPartial()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!hashSymbol
                .map(expectedHashSymbol ->
                        expectedHashSymbol
                                .map(symbolAlias -> symbolAlias.toSymbol(symbolAliases))
                                .map(Symbol::getName)
                                .equals(topNRowNumberNode.getHashVariable().map(VariableReferenceExpression::getName)))
                .orElse(true)) {
            return NO_MATCH;
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .add("rowNumberSymbol", rowNumberSymbol)
                .add("maxRowCountPerPartition", maxRowCountPerPartition)
                .add("partial", partial)
                .add("hashSymbol", hashSymbol)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
        private Optional<SymbolAlias> rowNumberSymbol = Optional.empty();
        private Optional<Integer> maxRowCountPerPartition = Optional.empty();
        private Optional<Boolean> partial = Optional.empty();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder specification(
                List<String> partitionBy,
                List<String> orderBy,
                Map<String, SortOrder> orderings)
        {
            this.specification = Optional.of(PlanMatchPattern.specification(partitionBy, orderBy, orderings));
            return this;
        }

        public Builder rowNumberSymbol(SymbolAlias rowNumberSymbol)
        {
            this.rowNumberSymbol = Optional.of(requireNonNull(rowNumberSymbol, "rowNumberSymbol is null"));
            return this;
        }

        public Builder maxRowCountPerPartition(int maxRowCountPerPartition)
        {
            this.maxRowCountPerPartition = Optional.of(maxRowCountPerPartition);
            return this;
        }

        public Builder partial(boolean partial)
        {
            this.partial = Optional.of(partial);
            return this;
        }

        public Builder hashSymbol(Optional<SymbolAlias> hashSymbol)
        {
            this.hashSymbol = Optional.of(requireNonNull(hashSymbol, "hashSymbol is null"));
            return this;
        }

        PlanMatchPattern build()
        {
            return node(TopNRowNumberNode.class, source).with(
                    new TopNRowNumberMatcher(
                            specification,
                            rowNumberSymbol,
                            maxRowCountPerPartition,
                            partial,
                            hashSymbol));
        }
    }
}
