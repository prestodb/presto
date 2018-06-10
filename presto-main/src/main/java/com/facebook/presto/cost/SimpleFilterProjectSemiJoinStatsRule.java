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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * It is not yet proven whether this heuristic is any better or worse. Either this rule will be enhanced
 * in the future or it will be dropped altogether.
 */
public class SimpleFilterProjectSemiJoinStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public SimpleFilterProjectSemiJoinStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator can not be null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNode nodeSource = lookup.resolve(node.getSource());
        SemiJoinNode semiJoinNode;
        if (nodeSource instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) nodeSource;
            if (!projectNode.isIdentity()) {
                return Optional.empty();
            }
            PlanNode projectNodeSource = lookup.resolve(projectNode.getSource());
            if (!(projectNodeSource instanceof SemiJoinNode)) {
                return Optional.empty();
            }
            semiJoinNode = (SemiJoinNode) projectNodeSource;
        }
        else if (nodeSource instanceof SemiJoinNode) {
            semiJoinNode = (SemiJoinNode) nodeSource;
        }
        else {
            return Optional.empty();
        }

        return calculate(node, semiJoinNode, sourceStats, session, types);
    }

    private Optional<PlanNodeStatsEstimate> calculate(FilterNode filterNode, SemiJoinNode semiJoinNode, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(semiJoinNode.getSource());
        PlanNodeStatsEstimate filteringSourceStats = statsProvider.getStats(semiJoinNode.getFilteringSource());
        Symbol filteringSourceJoinSymbol = semiJoinNode.getFilteringSourceJoinSymbol();
        Symbol sourceJoinSymbol = semiJoinNode.getSourceJoinSymbol();

        Optional<SemiJoinOutputFilter> semiJoinOutputFilter = extractSemiJoinOutputFilter(filterNode.getPredicate(), semiJoinNode.getSemiJoinOutput());

        if (!semiJoinOutputFilter.isPresent()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate semiJoinStats;
        if (semiJoinOutputFilter.get().isNegated()) {
            semiJoinStats = computeAntiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }
        else {
            semiJoinStats = computeSemiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }

        // apply remaining predicate
        return Optional.of(filterStatsCalculator.filterStats(semiJoinStats, semiJoinOutputFilter.get().getRemainingPredicate(), session, types));
    }

    private static Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(Expression predicate, Symbol semiJoinOutput)
    {
        List<Expression> conjuncts = extractConjuncts(predicate);
        List<Expression> semiJoinOutputReferences = conjuncts.stream()
                .filter(conjunct -> isSemiJoinOutputReference(conjunct, semiJoinOutput))
                .collect(toImmutableList());

        if (semiJoinOutputReferences.size() != 1) {
            return Optional.empty();
        }

        Expression semiJoinOutputReference = Iterables.getOnlyElement(semiJoinOutputReferences);
        Expression remainingPredicate = combineConjuncts(conjuncts.stream()
                .filter(conjunct -> conjunct != semiJoinOutputReference)
                .collect(toImmutableList()));
        boolean negated = semiJoinOutputReference instanceof NotExpression;
        return Optional.of(new SemiJoinOutputFilter(negated, remainingPredicate));
    }

    private static boolean isSemiJoinOutputReference(Expression conjunct, Symbol semiJoinOutput)
    {
        SymbolReference semiJoinOuputSymbolReference = semiJoinOutput.toSymbolReference();
        return conjunct.equals(semiJoinOuputSymbolReference) ||
                (conjunct instanceof NotExpression && ((NotExpression) conjunct).getValue().equals(semiJoinOuputSymbolReference));
    }

    private static class SemiJoinOutputFilter
    {
        private final boolean negated;
        private final Expression remainingPredicate;

        public SemiJoinOutputFilter(boolean negated, Expression remainingPredicate)
        {
            this.negated = negated;
            this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate can not be null");
        }

        public boolean isNegated()
        {
            return negated;
        }

        public Expression getRemainingPredicate()
        {
            return remainingPredicate;
        }
    }
}
