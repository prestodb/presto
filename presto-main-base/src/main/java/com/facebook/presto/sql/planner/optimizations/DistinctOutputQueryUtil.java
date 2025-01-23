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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;

import java.util.function.Function;

import static java.util.function.Function.identity;

public final class DistinctOutputQueryUtil
{
    private DistinctOutputQueryUtil() {}

    public static boolean isDistinct(PlanNode node)
    {
        return node.accept(new IsDistinctPlanVisitor(identity()), null);
    }

    public static boolean isDistinct(PlanNode node, Function<PlanNode, PlanNode> lookupFunction)
    {
        return node.accept(new IsDistinctPlanVisitor(lookupFunction), null);
    }

    private static final class IsDistinctPlanVisitor
            extends InternalPlanVisitor<Boolean, Void>
    {
        /*
        With the iterative optimizer, plan nodes are replaced with
        GroupReference nodes. This requires the rules that look deeper
        in the tree than the rewritten node and its immediate sources
        to use Lookup for resolving the nodes corresponding to the
        GroupReferences.
        */
        private final Function<PlanNode, PlanNode> lookupFunction;

        private IsDistinctPlanVisitor(Function<PlanNode, PlanNode> lookupFunction)
        {
            this.lookupFunction = lookupFunction;
        }

        @Override
        public Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitExcept(ExceptNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            return lookupFunction.apply(node.getSource()).accept(this, null);
        }

        @Override
        public Boolean visitIntersect(IntersectNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitValues(ValuesNode node, Void context)
        {
            return node.getRows().size() == 1;
        }

        @Override
        public Boolean visitLimit(LimitNode node, Void context)
        {
            return node.getCount() <= 1;
        }

        @Override
        public Boolean visitTopN(TopNNode node, Void context)
        {
            return node.getCount() <= 1;
        }
    }
}
