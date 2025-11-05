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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isSingleNodeExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.preferMergeJoinForSortedInputs;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeJoinForSortedInputOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final boolean nativeExecution;
    private final boolean prestoOnSpark;
    private boolean isEnabledForTesting;

    public MergeJoinForSortedInputOptimizer(Metadata metadata, boolean nativeExecution, boolean prestoOnSpark)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nativeExecution = nativeExecution;
        this.prestoOnSpark = prestoOnSpark;
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || nativeExecution && (isGroupedExecutionEnabled(session) || prestoOnSpark) && preferMergeJoinForSortedInputs(session) && !isSingleNodeExecutionEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider type, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (isEnabled(session)) {
            Rewriter rewriter = new MergeJoinForSortedInputOptimizer.Rewriter(idAllocator, metadata, session, prestoOnSpark);
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private final boolean prestoOnSpark;
        private boolean planChanged;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session, boolean prestoOnSpark)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.prestoOnSpark = prestoOnSpark;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenLeft = node.getLeft().accept(this, context);
            PlanNode rewrittenRight = node.getRight().accept(this, context);

            boolean leftInputSorted = isPlanOutputSortedByColumns(rewrittenLeft, node.getCriteria().stream().map(EquiJoinClause::getLeft).collect(toImmutableList()));
            boolean rightInputSorted = isPlanOutputSortedByColumns(rewrittenRight, node.getCriteria().stream().map(EquiJoinClause::getRight).collect(toImmutableList()));

            if ((!leftInputSorted && !rightInputSorted) || (!prestoOnSpark && (!leftInputSorted || !rightInputSorted))) {
                return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
            }

            if (!leftInputSorted) {
                List<Ordering> leftOrdering = node.getCriteria().stream()
                        .map(criterion -> new Ordering(criterion.getLeft(), ASC_NULLS_FIRST))
                        .collect(toImmutableList());
                rewrittenLeft = new SortNode(
                        Optional.empty(),
                        idAllocator.getNextId(),
                        rewrittenLeft,
                        new OrderingScheme(leftOrdering),
                        true,
                        ImmutableList.of());
            }
            if (!rightInputSorted) {
                List<Ordering> rightOrdering = node.getCriteria().stream()
                        .map(criterion -> new Ordering(criterion.getRight(), ASC_NULLS_FIRST))
                        .collect(toImmutableList());
                rewrittenRight = new SortNode(
                        Optional.empty(),
                        idAllocator.getNextId(),
                        rewrittenRight,
                        new OrderingScheme(rightOrdering),
                        true,
                        ImmutableList.of());
            }
            planChanged = true;
            return new MergeJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getType(),
                    rewrittenLeft,
                    rewrittenRight,
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    Optional.empty(),
                    Optional.empty());
        }

        private boolean isPlanOutputSortedByColumns(PlanNode plan, List<VariableReferenceExpression> columns)
        {
            StreamPropertyDerivations.StreamProperties properties = StreamPropertyDerivations.derivePropertiesRecursively(plan, metadata, session, nativeExecution);

            if (!verifyStreamProperties(properties, columns)) {
                return false;
            }

            return !LocalProperties.match(properties.getLocalProperties(), LocalProperties.sorted(columns, ASC_NULLS_FIRST)).get(0).isPresent();
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
