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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.function.Function.identity;

public final class ScalarQueryUtil
{
    private ScalarQueryUtil() {}

    /**
     * @return true when subquery is scalar and its output symbols are directly mapped to the ApplyNode's output symbols
     */
    public static boolean isResolvedScalarSubquery(ApplyNode applyNode)
    {
        return isScalar(applyNode.getSubquery(), identity()) && applyNode.getSubqueryAssignments().getExpressions().stream()
                .allMatch(expression -> expression instanceof SymbolReference);
    }

    public static boolean isResolvedScalarSubquery(ApplyNode applyNode, Function<PlanNode, PlanNode> lookup)
    {
        PlanNode subquery = lookup.apply(applyNode.getSubquery());
        return isScalar(subquery, lookup) && applyNode.getSubqueryAssignments().getExpressions().stream()
                .allMatch(expression -> expression instanceof SymbolReference);
    }

    public static boolean isScalar(PlanNode node)
    {
        return isScalar(node, identity());
    }

    public static boolean isScalar(PlanNode node, Function<PlanNode, PlanNode> lookupFunction)
    {
        return node.accept(new IsScalarPlanVisitor(lookupFunction), null);
    }

    private static final class IsScalarPlanVisitor
            extends PlanVisitor<Void, Boolean>
    {
        /*
        With the iterative optimizer, plan nodes are replaced with
        GroupReference nodes. This requires the rules that look deeper
        in the tree than the rewritten node and its immediate sources
        to use Lookup for resolving the nodes corresponding to the
        GroupReferences.
        */
        private final Function<PlanNode, PlanNode> lookupFunction;

        public IsScalarPlanVisitor(Function<PlanNode, PlanNode> lookupFunction)
        {
            this.lookupFunction = lookupFunction;
        }

        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Void context)
        {
            return node.getGroupingSets().equals(ImmutableList.of(ImmutableList.of()));
        }

        @Override
        public Boolean visitExchange(ExchangeNode node, Void context)
        {
            return (node.getSources().size() == 1) &&
                    lookupFunction.apply(getOnlyElement(node.getSources())).accept(this, null);
        }

        @Override
        public Boolean visitProject(ProjectNode node, Void context)
        {
            return lookupFunction.apply(node.getSource()).accept(this, null);
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            return lookupFunction.apply(node.getSource()).accept(this, null);
        }

        public Boolean visitValues(ValuesNode node, Void context)
        {
            return node.getRows().size() == 1;
        }
    }
}
