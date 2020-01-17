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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.RowNumberNode;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RowNumberMatcher
        implements Matcher
{
    private final Optional<List<SymbolAlias>> partitionBy;
    private final Optional<Optional<Integer>> maxRowCountPerPartition;
    private final Optional<SymbolAlias> rowNumberSymbol;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private RowNumberMatcher(
            Optional<List<SymbolAlias>> partitionBy,
            Optional<Optional<Integer>> maxRowCountPerPartition,
            Optional<SymbolAlias> rowNumberSymbol,
            Optional<Optional<SymbolAlias>> hashSymbol)
    {
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.maxRowCountPerPartition = requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashVariable is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof RowNumberNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        RowNumberNode rowNumberNode = (RowNumberNode) node;

        if (!partitionBy
                .map(expectedPartitionBy -> expectedPartitionBy.stream()
                        .map(alias -> alias.toSymbol(symbolAliases))
                        .map(Symbol::getName)
                        .collect(toImmutableList())
                        .equals(rowNumberNode.getPartitionBy().stream().map(VariableReferenceExpression::getName).collect(toImmutableList())))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!maxRowCountPerPartition
                .map(expectedMaxRowCountPerPartition -> expectedMaxRowCountPerPartition.equals(rowNumberNode.getMaxRowCountPerPartition()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!rowNumberSymbol
                .map(expectedRowNumberSymbol ->
                        expectedRowNumberSymbol.toSymbol(symbolAliases).getName()
                                .equals(rowNumberNode.getRowNumberVariable().getName()))
                .orElse(true)) {
            return NO_MATCH;
        }

        if (!hashSymbol
                .map(expectedHashSymbol ->
                        expectedHashSymbol
                                .map(symbolAlias -> symbolAlias.toSymbol(symbolAliases))
                                .map(Symbol::getName)
                                .equals(rowNumberNode.getHashVariable().map(VariableReferenceExpression::getName)))
                .orElse(true)) {
            return NO_MATCH;
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("maxRowCountPerPartition", maxRowCountPerPartition)
                .add("rowNumberSymbol", rowNumberSymbol)
                .add("hashSymbol", hashSymbol)
                .toString();
    }

    /**
     * By default, matches any RowNumberNode.  Users add additional constraints by
     * calling the various member functions of the Builder, typically named according
     * to the field names of RowNumberNode.
     */
    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<List<SymbolAlias>> partitionBy = Optional.empty();
        private Optional<Optional<Integer>> maxRowCountPerPartition = Optional.empty();
        private Optional<SymbolAlias> rowNumberSymbol = Optional.empty();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder partitionBy(List<String> partitionBy)
        {
            requireNonNull(partitionBy, "partitionBy is null");
            this.partitionBy = Optional.of(partitionBy.stream()
                    .map(SymbolAlias::new)
                    .collect(toImmutableList()));
            return this;
        }

        public Builder maxRowCountPerPartition(Optional<Integer> maxRowCountPerPartition)
        {
            this.maxRowCountPerPartition = Optional.of(requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null"));
            return this;
        }

        public Builder rowNumberSymbol(SymbolAlias rowNumberSymbol)
        {
            this.rowNumberSymbol = Optional.of(requireNonNull(rowNumberSymbol, "rowNumberSymbol is null"));
            return this;
        }

        public Builder hashSymbol(Optional<SymbolAlias> hashSymbol)
        {
            this.hashSymbol = Optional.of(requireNonNull(hashSymbol, "hashSymbol is null"));
            return this;
        }

        PlanMatchPattern build()
        {
            return node(RowNumberNode.class, source).with(
                    new RowNumberMatcher(
                            partitionBy,
                            maxRowCountPerPartition,
                            rowNumberSymbol,
                            hashSymbol));
        }
    }
}
