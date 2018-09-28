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
import com.facebook.presto.sql.DynamicFilters;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.DynamicFilters.extractDynamicFilters;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.collect.Maps.filterKeys;

/**
 * Dynamic filters are supported only right after TableScan
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new RemoveUnsupportedDynamicFilters.Rewriter(), plan, new HashSet<>());
    }

    private class Rewriter
            extends SimplePlanRewriter<Set<String>>
    {
        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<String>> context)
        {
            JoinNode join = (JoinNode) context.defaultRewrite(node, context.get());

            Optional<Expression> filterOptional = join.getFilter().map(filter -> {
                Expression modified = removeDynamicFilters(filter, context.get());
                if (TRUE_LITERAL.equals(modified)) {
                    return null;
                }
                else if (!filter.equals(modified)) {
                    return modified;
                }
                return filter;
            });

            Map<String, Symbol> dynamicFilters = ImmutableMap.copyOf(filterKeys(node.getDynamicFilters(), key -> !context.get().contains(key)));

            if (!filterOptional.equals(join.getFilter()) || !dynamicFilters.equals(join.getDynamicFilters())) {
                return new JoinNode(
                        join.getId(),
                        join.getType(),
                        join.getLeft(),
                        join.getRight(),
                        join.getCriteria(),
                        join.getOutputSymbols(),
                        filterOptional,
                        join.getLeftHashSymbol(),
                        join.getRightHashSymbol(),
                        join.getDistributionType(),
                        dynamicFilters);
            }
            return join;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<String>> context)
        {
            FilterNode filter = (FilterNode) context.defaultRewrite(node, context.get());
            PlanNode source = filter.getSource();
            if (source instanceof TableScanNode) {
                return filter;
            }

            Expression original = node.getPredicate();
            Expression modified = removeDynamicFilters(original, context.get());

            if (original.equals(modified)) {
                return filter;
            }

            if (TRUE_LITERAL.equals(modified)) {
                return source;
            }

            return new FilterNode(node.getId(), node.getSource(), modified);
        }

        private Expression removeDynamicFilters(Expression expression, Set<String> removedDynamicFilters)
        {
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(expression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return expression;
            }
            extractResult.getDynamicConjuncts().forEach(filter -> removedDynamicFilters.add(filter.getId()));
            return combineConjuncts(extractResult.getStaticConjuncts());
        }
    }
}
