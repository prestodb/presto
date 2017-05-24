package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.planner.DynamicFilter;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DynamicFilterMatcher
{
    private final String source;
    private final List<ExpectedValueProvider<EquiJoinClause>> criteria;

    private Map<String, String> expectedMappings;

    public DynamicFilterMatcher(String source, List<ExpectedValueProvider<EquiJoinClause>> criteria)
    {
        this.source = requireNonNull(source, "source is null");
        this.criteria = ImmutableList.copyOf(requireNonNull(criteria, "criteria is null"));
    }

    public MatchResult match(JoinNode joinNode)
    {
        DynamicFilter dynamicFilter = joinNode.getDynamicFilter();
        if (!source.equals(dynamicFilter.getSource())) {
            return new MatchResult(false);
        }
    }

    public MatchResult match(FilterNode filterNode)
    {
        List<Expression> conjunts = extractConjuncts(filterNode.getPredicate());
        List<ComparisonExpression> equalityPredicates = conjunts.stream()
                .filter(expression -> expression instanceof ComparisonExpression)
                .map(ComparisonExpression.class::cast)
                .filter(comparisonExpression -> comparisonExpression.getType() == EQUAL)
                .collect(toImmutableList());
        List<ComparisonExpression> relatedPredicates = equalityPredicates.stream()
                .filter(predicate -> predicate.getLeft() instanceof DeferredSymbolReference)
                .filter(relatedPredicates(source))
                .collect(toImmutableList());
    }

    private static Predicate<ComparisonExpression> relatedPredicates(String source)
    {
        return comparisonExpression -> {
            Expression right = comparisonExpression.getRight();
            if (!(right instanceof DeferredSymbolReference)) {
                return false;
            }
            DeferredSymbolReference reference = (DeferredSymbolReference) right;
            if (!reference.getSource().equals(source)) {
                return false;
            }

            return;
        };
    }

    private boolean matchMappings(Map<String, String> expectedMappings)
    {

    }
}
