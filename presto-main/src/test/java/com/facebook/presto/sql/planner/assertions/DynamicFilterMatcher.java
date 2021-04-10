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
import com.facebook.presto.expressions.DynamicFilters;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
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
    private final Optional<Expression> expectedStaticFilter;

    private JoinNode joinNode;
    private SymbolAliases symbolAliases;
    private FilterNode filterNode;

    public DynamicFilterMatcher(Map<SymbolAlias, SymbolAlias> expectedDynamicFilters, Optional<Expression> expectedStaticFilter)
    {
        this.expectedDynamicFilters = requireNonNull(expectedDynamicFilters, "expectedDynamicFilters is null");
        this.joinExpectedMappings = expectedDynamicFilters.values().stream()
                .collect(toImmutableMap(rightSymbol -> rightSymbol.toString() + "_alias", SymbolAlias::toString));
        this.filterExpectedMappings = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString() + "_alias"));
        this.expectedStaticFilter = requireNonNull(expectedStaticFilter, "expectedStaticFilter is null");
    }

    public MatchResult match(JoinNode joinNode, SymbolAliases symbolAliases)
    {
        checkState(this.joinNode == null, "joinNode must be null at this point");
        this.joinNode = joinNode;
        this.symbolAliases = symbolAliases;
        return new MatchResult(match());
    }

    private MatchResult match(FilterNode filterNode, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(this.filterNode == null, "filterNode must be null at this point");
        this.filterNode = filterNode;
        this.symbolAliases = symbolAliases;

        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FunctionResolution(metadata.getFunctionAndTypeManager()),
                metadata.getFunctionAndTypeManager());
        boolean staticFilterMatches = expectedStaticFilter.map(filter -> {
            RowExpressionVerifier verifier = new RowExpressionVerifier(symbolAliases, metadata, session);
            RowExpression staticFilter = logicalRowExpressions.combineConjuncts(extractDynamicFilters(filterNode.getPredicate()).getStaticConjuncts());
            return verifier.process(filter, staticFilter);
        }).orElse(true);

        return new MatchResult(match() && staticFilterMatches);
    }

    private boolean match()
    {
        checkState(symbolAliases != null, "symbolAliases is null");

        // both nodes must be provided to do the matching
        if (filterNode == null || joinNode == null) {
            return true;
        }

        Map<String, VariableReferenceExpression> idToProbeSymbolMap = extractDynamicFilters(filterNode.getPredicate())
                .getDynamicConjuncts().stream()
                .collect(toImmutableMap(DynamicFilters.DynamicFilterPlaceholder::getId, filter -> (VariableReferenceExpression) filter.getInput()));
        Map<String, VariableReferenceExpression> idToBuildSymbolMap = joinNode.getDynamicFilters();

        if (idToProbeSymbolMap == null) {
            return false;
        }

        if (idToProbeSymbolMap.size() != expectedDynamicFilters.size()) {
            return false;
        }

        Map<Symbol, Symbol> actual = new HashMap<>();
        for (Map.Entry<String, VariableReferenceExpression> idToProbeSymbol : idToProbeSymbolMap.entrySet()) {
            String id = idToProbeSymbol.getKey();
            VariableReferenceExpression probe = idToProbeSymbol.getValue();
            VariableReferenceExpression build = idToBuildSymbolMap.get(id);
            if (build == null) {
                return false;
            }
            actual.put(new Symbol(probe.getName()), new Symbol(build.getName()));
        }

        Map<Symbol, Symbol> expected = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toSymbol(symbolAliases), entry -> entry.getValue().toSymbol(symbolAliases)));

        return expected.equals(actual);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof FilterNode)) {
            return new MatchResult(false);
        }
        return match((FilterNode) node, session, metadata, symbolAliases);
    }

    public Map<String, String> getJoinExpectedMappings()
    {
        return joinExpectedMappings;
    }

    @Override
    public String toString()
    {
        String predicate = Joiner.on(" AND ")
                .join(filterExpectedMappings.entrySet().stream()
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(toImmutableList()));
        return toStringHelper(this)
                .add("dynamicPredicate", predicate)
                .add("staticPredicate", expectedStaticFilter)
                .toString();
    }
}
