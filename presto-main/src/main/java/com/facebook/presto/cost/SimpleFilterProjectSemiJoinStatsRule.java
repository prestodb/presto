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
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.facebook.presto.sql.relational.ProjectNodeUtils.isIdentity;
import static com.google.common.base.Preconditions.checkState;
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
    private final LogicalRowExpressions logicalRowExpressions;
    private final FunctionResolution functionResolution;

    public SimpleFilterProjectSemiJoinStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator, FunctionAndTypeManager functionAndTypeManager)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator can not be null");
        requireNonNull(functionAndTypeManager, "functionManager can not be null");
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
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
            if (!isIdentity(projectNode)) {
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
        VariableReferenceExpression filteringSourceJoinVariable = semiJoinNode.getFilteringSourceJoinVariable();
        VariableReferenceExpression sourceJoinVariable = semiJoinNode.getSourceJoinVariable();

        Optional<SemiJoinOutputFilter> semiJoinOutputFilter;
        VariableReferenceExpression semiJoinOutput = semiJoinNode.getSemiJoinOutput();
        if (isExpression(filterNode.getPredicate())) {
            semiJoinOutputFilter = extractSemiJoinOutputFilter(castToExpression(filterNode.getPredicate()), semiJoinOutput);
        }
        else {
            semiJoinOutputFilter = extractSemiJoinOutputFilter(filterNode.getPredicate(), semiJoinOutput);
        }

        if (!semiJoinOutputFilter.isPresent()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate semiJoinStats;
        if (semiJoinOutputFilter.get().isNegated()) {
            semiJoinStats = computeAntiJoin(sourceStats, filteringSourceStats, sourceJoinVariable, filteringSourceJoinVariable);
        }
        else {
            semiJoinStats = computeSemiJoin(sourceStats, filteringSourceStats, sourceJoinVariable, filteringSourceJoinVariable);
        }

        if (semiJoinStats.isOutputRowCountUnknown()) {
            return Optional.of(PlanNodeStatsEstimate.unknown());
        }

        // apply remaining predicate
        PlanNodeStatsEstimate filteredStats;
        if (isExpression(filterNode.getPredicate())) {
            filteredStats = filterStatsCalculator.filterStats(semiJoinStats, castToExpression(semiJoinOutputFilter.get().getRemainingPredicate()), session, types);
        }
        else {
            filteredStats = filterStatsCalculator.filterStats(semiJoinStats, semiJoinOutputFilter.get().getRemainingPredicate(), session);
        }

        if (filteredStats.isOutputRowCountUnknown()) {
            return Optional.of(semiJoinStats.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT));
        }
        return Optional.of(filteredStats);
    }

    private Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(Expression predicate, VariableReferenceExpression semiJoinOutput)
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
        return Optional.of(new SemiJoinOutputFilter(negated, castToRowExpression(remainingPredicate)));
    }

    private Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(RowExpression predicate, RowExpression input)
    {
        checkState(!isExpression(predicate));
        List<RowExpression> conjuncts = LogicalRowExpressions.extractConjuncts(predicate);
        List<RowExpression> semiJoinOutputReferences = conjuncts.stream()
                .filter(conjunct -> isSemiJoinOutputReference(conjunct, input))
                .collect(toImmutableList());

        if (semiJoinOutputReferences.size() != 1) {
            return Optional.empty();
        }

        RowExpression semiJoinOutputReference = Iterables.getOnlyElement(semiJoinOutputReferences);
        RowExpression remainingPredicate = logicalRowExpressions.combineConjuncts(conjuncts.stream()
                .filter(conjunct -> conjunct != semiJoinOutputReference)
                .collect(toImmutableList()));
        boolean negated = isNotFunction(semiJoinOutputReference);
        return Optional.of(new SemiJoinOutputFilter(negated, remainingPredicate));
    }

    private boolean isSemiJoinOutputReference(RowExpression conjunct, RowExpression input)
    {
        return conjunct.equals(input) || (isNotFunction(conjunct) && ((CallExpression) conjunct).getArguments().get(0).equals(input));
    }

    private static boolean isSemiJoinOutputReference(Expression conjunct, VariableReferenceExpression semiJoinOutput)
    {
        SymbolReference semiJoinOuputSymbolReference = new SymbolReference(semiJoinOutput.getName());
        return conjunct.equals(semiJoinOuputSymbolReference) ||
                (conjunct instanceof NotExpression && ((NotExpression) conjunct).getValue().equals(semiJoinOuputSymbolReference));
    }

    private boolean isNotFunction(RowExpression expression)
    {
        return expression instanceof CallExpression && functionResolution.isNotFunction(((CallExpression) expression).getFunctionHandle());
    }

    private static class SemiJoinOutputFilter
    {
        private final boolean negated;
        private final RowExpression remainingPredicate;

        public SemiJoinOutputFilter(boolean negated, RowExpression remainingPredicate)
        {
            this.negated = negated;
            this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate can not be null");
        }

        public boolean isNegated()
        {
            return negated;
        }

        public RowExpression getRemainingPredicate()
        {
            return remainingPredicate;
        }
    }
}
