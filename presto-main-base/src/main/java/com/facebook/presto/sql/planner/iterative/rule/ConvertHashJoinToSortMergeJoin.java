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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.LocalProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.preferSortMergeJoin;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ConvertHashJoinToSortMergeJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> MergeJoinNode.isMergeJoinEligible(joinNode) &&
                    // it's not worth doing a sort merge join for a broadcast join which will have a small build side
                    !joinNode.getDistributionType().orElse(PARTITIONED).equals(REPLICATED));

    private final Metadata metadata;
    private final boolean nativeExecution;

    public ConvertHashJoinToSortMergeJoin(Metadata metadata, boolean nativeExecution)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nativeExecution = nativeExecution;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return nativeExecution && preferSortMergeJoin(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();

        List<VariableReferenceExpression> leftJoinColumns = node.getCriteria().stream().map(EquiJoinClause::getLeft).collect(toImmutableList());

        if (!isPlanOutputSortedByColumns(left, leftJoinColumns, context.getSession())) {
            List<Ordering> leftOrdering = node.getCriteria().stream()
                    .map(criterion -> new Ordering(criterion.getLeft(), ASC_NULLS_FIRST))
                    .collect(toImmutableList());
            left = new SortNode(
                    Optional.empty(),
                    context.getIdAllocator().getNextId(),
                    left,
                    new OrderingScheme(leftOrdering),
                    true,
                    ImmutableList.of());
        }

        List<VariableReferenceExpression> rightJoinColumns = node.getCriteria().stream()
                .map(EquiJoinClause::getRight)
                .collect(toImmutableList());
        if (!isPlanOutputSortedByColumns(right, rightJoinColumns, context.getSession())) {
            List<Ordering> rightOrdering = node.getCriteria().stream()
                    .map(criterion -> new Ordering(criterion.getRight(), ASC_NULLS_FIRST))
                    .collect(toImmutableList());
            right = new SortNode(
                    Optional.empty(),
                    context.getIdAllocator().getNextId(),
                    right,
                    new OrderingScheme(rightOrdering),
                    true,
                    ImmutableList.of());
        }

        return Result.ofPlanNode(
                new MergeJoinNode(
                        Optional.empty(),
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable()));
    }

    private boolean isPlanOutputSortedByColumns(PlanNode plan, List<VariableReferenceExpression> columns, Session session)
    {
        StreamPropertyDerivations.StreamProperties properties = StreamPropertyDerivations.derivePropertiesRecursively(plan, metadata, session, nativeExecution);

        // Check if partitioning columns (bucketed-by columns [B]) are a subset of join columns [J]
        // B = subset (J)
        if (!verifyStreamProperties(properties, columns)) {
            return false;
        }

        // Check if the output of the subplan is ordered by the join columns
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
