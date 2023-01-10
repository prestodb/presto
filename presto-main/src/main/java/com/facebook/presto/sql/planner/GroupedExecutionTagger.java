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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.isGroupedExecutionEnabled;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PLAN_ERROR;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_REWINDABLE_SPLIT_SOURCE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.determineReplicatedReadsJoinAllowed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class GroupedExecutionTagger
        extends InternalPlanVisitor<GroupedExecutionTagger.GroupedExecutionProperties, Void>
{
    private final Session session;
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final boolean groupedExecutionEnabled;

    public GroupedExecutionTagger(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.groupedExecutionEnabled = isGroupedExecutionEnabled(session);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitPlan(PlanNode node, Void context)
    {
        if (node.getSources().isEmpty()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        return processChildren(node);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitJoin(JoinNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties left = node.getLeft().accept(this, null);
        GroupedExecutionTagger.GroupedExecutionProperties right = node.getRight().accept(this, null);

        if (!node.getDistributionType().isPresent() || !groupedExecutionEnabled) {
            // This is possible when the optimizers is invoked with `forceSingleNode` set to true.
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        if ((node.getType() == JoinNode.Type.RIGHT || node.getType() == JoinNode.Type.FULL) && !right.currentNodeCapable) {
            // For a plan like this, if the fragment participates in grouped execution,
            // the LookupOuterOperator corresponding to the RJoin will not work execute properly.
            //
            // * The operator has to execute as not-grouped because it can only look at the "used" flags in
            //   join build after all probe has finished.
            // * The operator has to execute as grouped the subsequent LJoin expects that incoming
            //   operators are grouped. Otherwise, the LJoin won't be able to throw out the build side
            //   for each group as soon as the group completes.
            //
            //       LJoin
            //       /   \
            //   RJoin   Scan
            //   /   \
            // Scan Remote
            //
            // TODO:
            // The RJoin can still execute as grouped if there is no subsequent operator that depends
            // on the RJoin being executed in a grouped manner. However, this is not currently implemented.
            // Support for this scenario is already implemented in the execution side.
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        // If replicated reads participates in grouped execution, the build and probe are planned together within the same stage.
        // This is a problem in a broadcast join case because the right side of join is not capable of grouped execution when it replicates a bucketed table.
        // In local execution time, the PerBucket implementation maintains one AsyncQueue for each bucket. replicated reads cannot create multiple AsyncQueue tasks
        // for all buckets which could potential cause `HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT` issue. Hence, replicated reads does not support grouped execution
        if (determineReplicatedReadsJoinAllowed(node)) {
            return GroupedExecutionProperties.notCapable();
        }

        switch (node.getDistributionType().get()) {
            case REPLICATED:
                // Broadcast join maintains partitioning for the left side.
                // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                checkState(!right.currentNodeCapable);
                return left;
            case PARTITIONED:
                if (left.currentNodeCapable && right.currentNodeCapable) {
                    checkState(left.totalLifespans == right.totalLifespans, format("Mismatched number of lifespans on left(%s) and right(%s) side of join", left.totalLifespans, right.totalLifespans));
                    return new GroupedExecutionTagger.GroupedExecutionProperties(
                            true,
                            true,
                            ImmutableList.<PlanNodeId>builder()
                                    .addAll(left.capableTableScanNodes)
                                    .addAll(right.capableTableScanNodes)
                                    .build(),
                            left.totalLifespans,
                            left.recoveryEligible && right.recoveryEligible);
                }
                // right.subTreeUseful && !left.currentNodeCapable:
                //   It's not particularly helpful to do grouped execution on the right side
                //   because the benefit is likely cancelled out due to required buffering for hash build.
                //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                //   However, this is not currently implemented. JoinBridgeManager need to support such a lifecycle.
                // !right.currentNodeCapable:
                //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                //
                return left;
            default:
                throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
        }
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitMergeJoin(MergeJoinNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties left = node.getLeft().accept(this, null);
        GroupedExecutionTagger.GroupedExecutionProperties right = node.getRight().accept(this, null);

        if (groupedExecutionEnabled && left.currentNodeCapable && right.currentNodeCapable) {
            checkState(left.totalLifespans == right.totalLifespans, format("Mismatched number of lifespans on left(%s) and right(%s) side of join", left.totalLifespans, right.totalLifespans));
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true,
                    true,
                    ImmutableList.<PlanNodeId>builder()
                            .addAll(left.capableTableScanNodes)
                            .addAll(right.capableTableScanNodes)
                            .build(),
                    left.totalLifespans,
                    left.recoveryEligible && right.recoveryEligible);
        }
        throw new PrestoException(
                INVALID_PLAN_ERROR,
                format("When grouped execution can't be enabled, merge join plan is not valid." +
                                "%s is currently set to %s; left node grouped execution capable is %s and " +
                                "right node grouped execution capable is %s.",
                        GROUPED_EXECUTION,
                        groupedExecutionEnabled,
                        left.currentNodeCapable,
                        right.currentNodeCapable));
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitAggregation(AggregationNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = node.getSource().accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            switch (node.getStep()) {
                case SINGLE:
                case FINAL:
                    return new GroupedExecutionTagger.GroupedExecutionProperties(true, true, properties.capableTableScanNodes, properties.totalLifespans, properties.recoveryEligible);
                case PARTIAL:
                case INTERMEDIATE:
                    return properties;
            }
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitWindow(WindowNode node, Void context)
    {
        return processWindowFunction(node);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitRowNumber(RowNumberNode node, Void context)
    {
        return processWindowFunction(node);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTopNRowNumber(TopNRowNumberNode node, Void context)
    {
        return processWindowFunction(node);
    }

    private GroupedExecutionTagger.GroupedExecutionProperties processWindowFunction(PlanNode node)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            return new GroupedExecutionTagger.GroupedExecutionProperties(true, true, properties.capableTableScanNodes, properties.totalLifespans, properties.recoveryEligible);
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitMarkDistinct(MarkDistinctNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            return new GroupedExecutionTagger.GroupedExecutionProperties(true, true, properties.capableTableScanNodes, properties.totalLifespans, properties.recoveryEligible);
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTableWriter(TableWriterNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = node.getSource().accept(this, null);
        boolean recoveryEligible = properties.isRecoveryEligible();
        TableWriterNode.WriterTarget target = node.getTarget().orElseThrow(() -> new VerifyException("target is absent"));
        if (target instanceof TableWriterNode.CreateName || target instanceof TableWriterNode.InsertReference || target instanceof TableWriterNode.RefreshMaterializedViewReference) {
            recoveryEligible &= metadata.getConnectorCapabilities(session, target.getConnectorId()).contains(SUPPORTS_PAGE_SINK_COMMIT);
        }
        else {
            recoveryEligible = false;
        }
        return new GroupedExecutionTagger.GroupedExecutionProperties(
                properties.isCurrentNodeCapable(),
                properties.isSubTreeUseful(),
                properties.getCapableTableScanNodes(),
                properties.getTotalLifespans(),
                recoveryEligible);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTableScan(TableScanNode node, Void context)
    {
        Optional<TableLayout.TablePartitioning> tablePartitioning = metadata.getLayout(session, node.getTable(), false).getTablePartitioning();
        if (!tablePartitioning.isPresent()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
        if (ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles)) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        else {
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true,
                    false,
                    ImmutableList.of(node.getId()),
                    partitionHandles.size(),
                    metadata.getConnectorCapabilities(session, node.getTable().getConnectorId()).contains(SUPPORTS_REWINDABLE_SPLIT_SOURCE));
        }
    }

    private GroupedExecutionTagger.GroupedExecutionProperties processChildren(PlanNode node)
    {
        // Each fragment has a partitioning handle, which is derived from leaf nodes in the fragment.
        // Leaf nodes with different partitioning handle are not allowed to share a single fragment
        // (except for special cases as detailed in addSourceDistribution).
        // As a result, it is not necessary to check the compatibility between node.getSources because
        // they are guaranteed to be compatible.

        // * If any child is "not capable", return "not capable"
        // * When all children are capable ("capable and useful" or "capable but not useful")
        //   * Usefulness:
        //     * if any child is "useful", this node is "useful"
        //     * if no children is "useful", this node is "not useful"
        //   * Recovery Eligibility:
        //     * if all children is "recovery eligible", this node is "recovery eligible"
        //     * if any child is "not recovery eligible", this node is "not recovery eligible"
        boolean anyUseful = false;
        OptionalInt totalLifespans = OptionalInt.empty();
        boolean allRecoveryEligible = true;
        ImmutableList.Builder<PlanNodeId> capableTableScanNodes = ImmutableList.builder();
        for (PlanNode source : node.getSources()) {
            GroupedExecutionTagger.GroupedExecutionProperties properties = source.accept(this, null);
            if (!properties.isCurrentNodeCapable()) {
                return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
            }
            anyUseful |= properties.isSubTreeUseful();
            allRecoveryEligible &= properties.isRecoveryEligible();
            if (!totalLifespans.isPresent()) {
                totalLifespans = OptionalInt.of(properties.totalLifespans);
            }
            else {
                checkState(totalLifespans.getAsInt() == properties.totalLifespans, format("Mismatched number of lifespans among children nodes. Expected: %s, actual: %s", totalLifespans.getAsInt(), properties.totalLifespans));
            }

            capableTableScanNodes.addAll(properties.capableTableScanNodes);
        }
        return new GroupedExecutionTagger.GroupedExecutionProperties(true, anyUseful, capableTableScanNodes.build(), totalLifespans.getAsInt(), allRecoveryEligible);
    }

    static class GroupedExecutionProperties
    {
        // currentNodeCapable:
        //   Whether grouped execution is possible with the current node.
        //   For example, a table scan is capable iff it supports addressable split discovery.
        // subTreeUseful:
        //   Whether grouped execution is beneficial in the current node, or any node below it.
        //   For example, a JOIN can benefit from grouped execution because build can be flushed early, reducing peak memory requirement.
        //
        // In the current implementation, subTreeUseful implies currentNodeCapable.
        // In theory, this doesn't have to be the case. Take an example where a GROUP BY feeds into the build side of a JOIN.
        // Even if JOIN cannot take advantage of grouped execution, it could still be beneficial to execute the GROUP BY with grouped execution
        // (e.g. when the underlying aggregation's intermediate group state may be larger than aggregation output).

        private final boolean currentNodeCapable;
        private final boolean subTreeUseful;
        private final List<PlanNodeId> capableTableScanNodes;
        private final int totalLifespans;
        private final boolean recoveryEligible;

        public GroupedExecutionProperties(boolean currentNodeCapable, boolean subTreeUseful, List<PlanNodeId> capableTableScanNodes, int totalLifespans, boolean recoveryEligible)
        {
            this.currentNodeCapable = currentNodeCapable;
            this.subTreeUseful = subTreeUseful;
            this.capableTableScanNodes = ImmutableList.copyOf(requireNonNull(capableTableScanNodes, "capableTableScanNodes is null"));
            this.totalLifespans = totalLifespans;
            this.recoveryEligible = recoveryEligible;
            // Verify that `subTreeUseful` implies `currentNodeCapable`
            checkArgument(!subTreeUseful || currentNodeCapable);
            // Verify that `recoveryEligible` implies `currentNodeCapable`
            checkArgument(!recoveryEligible || currentNodeCapable);
            checkArgument(currentNodeCapable == !capableTableScanNodes.isEmpty());
        }

        public static GroupedExecutionProperties notCapable()
        {
            return new GroupedExecutionProperties(false, false, ImmutableList.of(), 1, false);
        }

        public boolean isCurrentNodeCapable()
        {
            return currentNodeCapable;
        }

        public boolean isSubTreeUseful()
        {
            return subTreeUseful;
        }

        public List<PlanNodeId> getCapableTableScanNodes()
        {
            return capableTableScanNodes;
        }

        public int getTotalLifespans()
        {
            return totalLifespans;
        }

        public boolean isRecoveryEligible()
        {
            return recoveryEligible;
        }
    }
}
