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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DynamicFilter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ApplyDynamicFilter
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) node;

        List<EquiJoinClause> criteria = join.getCriteria();
        DynamicFilter dynamicFilter = join.getDynamicFilter();

        Map<Symbol, Symbol> existingMappings = dynamicFilter.getMappings();
        List<EquiJoinClause> unprocessedClauses = criteria.stream()
                .filter(c -> !existingMappings.containsKey(c.getRight()))
                .collect(toImmutableList());

        if (unprocessedClauses.isEmpty()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();
        mappings.putAll(existingMappings);
        ImmutableList.Builder<Expression> predicates = ImmutableList.builder();

        for (EquiJoinClause clause : unprocessedClauses) {
            Symbol probeSymbol = clause.getLeft();
            Symbol buildSymbol = clause.getRight();
            Type buildSymbolType = requireNonNull(symbolAllocator.getTypes().get(buildSymbol));
            Symbol linkingSymbol = symbolAllocator.newSymbol("dynamic_filter_" + buildSymbol.getName(), buildSymbolType);
            DeferredSymbolReference dynamicFilterReference = new DeferredSymbolReference(dynamicFilter.getSource(), linkingSymbol.getName());
            predicates.add(new ComparisonExpression(EQUAL, probeSymbol.toSymbolReference(), dynamicFilterReference));
            mappings.put(buildSymbol, linkingSymbol);
        }

        DynamicFilter updatedDynamicFilter = new DynamicFilter(dynamicFilter.getSource(), mappings.build());

        return Optional.of(
                new JoinNode(
                        join.getId(),
                        join.getType(),
                        new FilterNode(idAllocator.getNextId(), join.getLeft(), combineConjuncts(predicates.build())),
                        join.getRight(),
                        join.getCriteria(),
                        join.getOutputSymbols(),
                        join.getFilter(),
                        join.getLeftHashSymbol(),
                        join.getRightHashSymbol(),
                        join.getDistributionType(),
                        updatedDynamicFilter));
    }
}
