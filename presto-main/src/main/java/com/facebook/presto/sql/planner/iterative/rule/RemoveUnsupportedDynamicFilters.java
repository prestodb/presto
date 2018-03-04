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
import com.facebook.presto.sql.DynamicFilterUtils.ExtractDynamicFiltersResult;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.DynamicFilterExpression;
import com.facebook.presto.sql.tree.Expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.DynamicFilterUtils.extractDynamicFilters;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkState;

/**
 * Dynamic filters are supported only right after TableScan
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new RemoveUnsupportedDynamicFilters.Rewriter(), plan, new HashSet<>());
    }

    private class Rewriter
            extends SimplePlanRewriter<Set<DynamicFilterExpression>>
    {
        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<DynamicFilterExpression>> context)
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

            Assignments assignments = join.getDynamicFilterAssignments();
            Set<DynamicFilterExpression> removedFilters = context.get();
            if (!removedFilters.isEmpty()) {
                Assignments originalAssignments = join.getDynamicFilterAssignments();
                Assignments modifiedAssignments = cleanupAssignments(join.getId(), originalAssignments, removedFilters);
                if (!originalAssignments.equals(modifiedAssignments)) {
                    assignments = modifiedAssignments;
                }
            }

            if (!filterOptional.equals(join.getFilter()) || !assignments.equals(join.getDynamicFilterAssignments())) {
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
                        assignments);
            }
            return join;
        }

        private Assignments cleanupAssignments(PlanNodeId joinId, Assignments assignments, Set<DynamicFilterExpression> removedFilters)
        {
            Map<Symbol, Expression> assignmentsMap = new HashMap<>(assignments.getMap());
            Iterator<DynamicFilterExpression> filtersIterator = removedFilters.iterator();
            while (filtersIterator.hasNext()) {
                DynamicFilterExpression removedFilter = filtersIterator.next();
                if (removedFilter.getSourceId().equals(joinId.toString())) {
                    Symbol filterSymbol = new Symbol(removedFilter.getDfSymbol());
                    checkState(assignmentsMap.containsKey(filterSymbol), "Assignments map doesn't contain key '%s': %s", filterSymbol, assignmentsMap);
                    assignmentsMap.remove(filterSymbol);
                    filtersIterator.remove();
                }
            }
            return new Assignments(assignmentsMap);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<DynamicFilterExpression>> context)
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

        private Expression removeDynamicFilters(Expression expression, Set<DynamicFilterExpression> removedFilters)
        {
            ExtractDynamicFiltersResult extractResult = extractDynamicFilters(expression);
            if (extractResult.getDynamicFilters().isEmpty()) {
                return expression;
            }
            removedFilters.addAll(extractResult.getDynamicFilters());
            return extractResult.getStaticFilters();
        }
    }
}
