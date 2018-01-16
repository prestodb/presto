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

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;

/*
 * For delete queries, the TableScan node that corresponds to the table being deleted
 * must be collocated with the Delete node, so any semi-join in between must be replicated
 */
// TODO migrate to traits
public class ReplicateSemiJoinUnderDelete
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Boolean>
    {
        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Boolean> context)
        {
            return context.defaultRewrite(node, true);
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Boolean> context)
        {
            boolean isUnderDelete = context.get();
            if (!isUnderDelete) {
                return context.defaultRewrite(node, isUnderDelete);
            }

            PlanNode source = context.rewrite(node.getSource(), isUnderDelete);
            PlanNode filteringSource = context.rewrite(node.getFilteringSource(), false);

            return node
                    .replaceChildren(ImmutableList.of(source, filteringSource))
                    .withDistributionType(REPLICATED);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Boolean> context)
        {
            throw new IllegalStateException("Must be run before adding exchanges");
        }
    }
}
