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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDistributedJoinEnabled;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = session;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft(), context.get());
            PlanNode rightRewritten = context.rewrite(node.getRight(), context.get());
            JoinNode.DistributionType targetJoinDistributionType = getTargetJoinDistributionType(node);
            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    leftRewritten,
                    rightRewritten,
                    node.getCriteria(),
                    node.getOutputSymbols(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    Optional.of(targetJoinDistributionType));
        }

        private JoinNode.DistributionType getTargetJoinDistributionType(JoinNode node)
        {
            // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions
            JoinNode.Type type = node.getType();
            if (type == RIGHT || type == FULL || (isDistributedJoinEnabled(session) && !mustBroadcastJoin(node))) {
                return JoinNode.DistributionType.PARTITIONED;
            }

            return JoinNode.DistributionType.REPLICATED;
        }

        private static boolean mustBroadcastJoin(JoinNode node)
        {
            return isAtMostScalar(node.getRight()) || isCrossJoin(node);
        }

        private static boolean isCrossJoin(JoinNode node)
        {
            return node.getType() == INNER && node.getCriteria().isEmpty();
        }
    }
}
