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
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DynamicFilterMatcher
        implements Matcher
{
    // LEFT_SYMBOL -> RIGHT_SYMBOL
    private final Map<SymbolAlias, SymbolAlias> expectedDynamicFilters;
    private final Map<String, String> joinExpectedMappings;
    private final Map<String, String> filterExpectedMappings;

    private JoinNode joinNode;
    private SymbolAliases symbolAliases;
    private FilterNode filterNode;

    public DynamicFilterMatcher(Map<SymbolAlias, SymbolAlias> expectedDynamicFilters)
    {
        this.expectedDynamicFilters = requireNonNull(expectedDynamicFilters, "expectedDynamicFilters is null");
        this.joinExpectedMappings = expectedDynamicFilters.values().stream()
                .collect(toImmutableMap(rightSymbol -> rightSymbol.toString() + "_alias", SymbolAlias::toString));
        this.filterExpectedMappings = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString() + "_alias"));
    }

    public MatchResult match(JoinNode joinNode, SymbolAliases symbolAliases)
    {
        checkState(this.joinNode == null, "joinNode must be null at this point");
        this.joinNode = joinNode;
        this.symbolAliases = symbolAliases;
        return new MatchResult(match());
    }

    public MatchResult match(FilterNode filterNode, SymbolAliases symbolAliases)
    {
        checkState(this.filterNode == null, "filterNode must be null at this point");
        this.filterNode = filterNode;
        this.symbolAliases = symbolAliases;
        return new MatchResult(match());
    }

    private boolean match()
    {
        checkState(symbolAliases != null, "symbolAliases is null");

        // both nodes must be provided to do the matching
        if (filterNode == null || joinNode == null) {
            return true;
        }

        Map<Symbol, Symbol> leftToRightAliasMap = extractDynamicFilters(filterNode, joinNode.getId().toString());
        Map<Symbol, Symbol> rightAliasToRightMap = extractDynamicFilters(joinNode);

        if (leftToRightAliasMap == null) {
            return false;
        }

        if (leftToRightAliasMap.size() != expectedDynamicFilters.size()) {
            return false;
        }

        Map<Symbol, Symbol> actual = new HashMap<>();
        for (Map.Entry<Symbol, Symbol> leftToRightAlias : leftToRightAliasMap.entrySet()) {
            Symbol left = leftToRightAlias.getKey();
            Symbol alias = leftToRightAlias.getValue();
            Symbol right = rightAliasToRightMap.get(alias);
            if (right == null) {
                return false;
            }
            actual.put(left, right);
        }

        Map<Symbol, Symbol> expected = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toSymbol(symbolAliases), entry -> entry.getValue().toSymbol(symbolAliases)));

        return expected.equals(actual);
    }

    /*
     * LEFT_SYMBOL -> RIGHT_SYMBOL_DYNAMIC_FILTER_ALIAS
     */
    @Nullable
    private static Map<Symbol, Symbol> extractDynamicFilters(FilterNode filterNode, String expectedSourceId)
    {
        List<Expression> conjuncts = extractConjuncts(filterNode.getPredicate());

        List<ComparisonExpression> equalityComparisons = conjuncts.stream()
                .filter(expression -> expression instanceof ComparisonExpression)
                .map(ComparisonExpression.class::cast)
                .filter(comparisonExpression -> comparisonExpression.getType() == EQUAL)
                .collect(toImmutableList());

        ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();
        for (ComparisonExpression comparison : equalityComparisons) {
            if (!(comparison.getLeft() instanceof SymbolReference) || !(comparison.getRight() instanceof DeferredSymbolReference)) {
                continue;
            }

            Symbol left = Symbol.from(comparison.getLeft());
            DeferredSymbolReference right = (DeferredSymbolReference) comparison.getRight();

            if (!right.getSourceId().equals(expectedSourceId)) {
                return null;
            }

            mappings.put(left, new Symbol(right.getSymbol()));
        }

        return mappings.build();
    }

    /*
     * RIGHT_SYMBOL_DYNAMIC_FILTER_ALIAS -> RIGHT_SYMBOL
     */
    private static Map<Symbol, Symbol> extractDynamicFilters(JoinNode node)
    {
        return node.getDynamicFilterAssignments()
                .getMap()
                .entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> Symbol.from(entry.getValue())));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeCost planNodeCost, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof FilterNode)) {
            return new MatchResult(false);
        }
        return match((FilterNode) node, symbolAliases);
    }

    public Map<String, String> getJoinExpectedMappings()
    {
        return joinExpectedMappings;
    }

    @Override
    public String toString()
    {
        String predicate = Joiner.on("AND")
                .join(filterExpectedMappings.entrySet().stream()
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(toImmutableList()));
        return toStringHelper(this)
                .add("predicate", predicate)
                .toString();
    }
}
