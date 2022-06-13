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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.preferMergeJoin;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeJoinOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser parser;

    public MergeJoinOptimizer(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider type, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (preferMergeJoin(session)) {
            return SimplePlanRewriter.rewriteWith(new MergeJoinOptimizer.Rewriter(variableAllocator, idAllocator, metadata, session), plan, null);
        }
        return plan;
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;

        private Rewriter(PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.types = variableAllocator.getTypes();
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            // As of now, we only support inner join for merge join
            if (node.getType() != INNER) {
                return node;
            }

            // For example: when we have a plan that looks like:
            // JoinNode
            //- TableScanA
            //- TableScanB

            // We check the data properties of TableScanA and TableScanB to see if they meet requirements for merge join:
            // 1. If so, we replace the JoinNode to MergeJoinNode
            // MergeJoinNode
            //- TableScanA
            //- TableScanB

            // 2. If not, we don't optimize

            if (isMergeJoinEligible(node.getLeft(), node.getRight(), node)) {
                return new MergeJoinNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        node.getLeft(),
                        node.getRight(),
                        node.getCriteria(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable());
            }
            return node;
        }

        private boolean isMergeJoinEligible(PlanNode left, PlanNode right, JoinNode node)
        {
            // Acquire data properties for both left and right side
            StreamPropertyDerivations.StreamProperties leftProperties = StreamPropertyDerivations.derivePropertiesRecursively(left, metadata, session, types, parser);
            StreamPropertyDerivations.StreamProperties rightProperties = StreamPropertyDerivations.derivePropertiesRecursively(right, metadata, session, types, parser);

            List<VariableReferenceExpression> leftJoinColumns = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(toImmutableList());
            List<VariableReferenceExpression> rightJoinColumns = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .collect(toImmutableList());

            // Check if both the left side and right side's partitioning columns (bucketed-by columns [B]) are a subset of join columns [J]
            // B = subset (J)
            if (!verifyStreamProperties(leftProperties, leftJoinColumns) || !verifyStreamProperties(rightProperties, rightJoinColumns)) {
                return false;
            }

            // Check if the left side and right side are both ordered by the join columns
            return !LocalProperties.match(rightProperties.getLocalProperties(), LocalProperties.sorted(rightJoinColumns, ASC_NULLS_FIRST)).get(0).isPresent() &&
                    !LocalProperties.match(leftProperties.getLocalProperties(), LocalProperties.sorted(leftJoinColumns, ASC_NULLS_FIRST)).get(0).isPresent();
        }

        private boolean verifyStreamProperties(StreamPropertyDerivations.StreamProperties streamProperties, List<VariableReferenceExpression> joinColumns)
        {
            if (!streamProperties.getPartitioningColumns().isPresent()) {
                return false;
            }
            List<VariableReferenceExpression> partitioningColumns = streamProperties.getPartitioningColumns().get();
            return partitioningColumns.size() <= joinColumns.size() && joinColumns.containsAll(partitioningColumns);
        }
    }
}
