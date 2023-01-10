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
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxStageCount;
import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.SystemSessionProperties.isRecoverableGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isTableWriterMergeOperatorEnabled;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static com.facebook.presto.spi.StandardWarningCode.TOO_MANY_STAGES;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.determineReplicatedReadsJoinAllowed;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;

public class PlanFragmenterUtils
{
    public static final int ROOT_FRAGMENT_ID = 0;
    public static final String TOO_MANY_STAGES_MESSAGE = "If the query contains multiple DISTINCTs, please set the 'use_mark_distinct' session property to false. " +
            "If the query contains multiple CTEs that are referenced more than once, please create temporary table(s) for one or more of the CTEs.";

    private PlanFragmenterUtils() {}

    /**
     * Perform any additional transformations and validations on the SubPlan after it has been fragmented
     *
     * @param subPlan the SubPlan to finalize
     * @param config
     * @param metadata
     * @param nodePartitioningManager
     * @param session
     * @param forceSingleNode
     * @param warningCollector
     * @return the final SubPlan for execution
     */
    public static SubPlan finalizeSubPlan(
            SubPlan subPlan,
            QueryManagerConfig config,
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            Session session,
            boolean forceSingleNode,
            WarningCollector warningCollector)
    {
        subPlan = reassignPartitioningHandleIfNecessary(metadata, session, subPlan);
        if (!forceSingleNode) {
            // grouped execution is not supported for SINGLE_DISTRIBUTION
            subPlan = analyzeGroupedExecution(session, subPlan, false, metadata, nodePartitioningManager);
        }

        checkState(subPlan.getFragment().getId().getId() != ROOT_FRAGMENT_ID || !isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(
                subPlan,
                warningCollector,
                getExchangeMaterializationStrategy(session),
                getQueryMaxStageCount(session),
                config.getStageCountWarningThreshold());

        return subPlan;
    }

    private static void sanityCheckFragmentedPlan(
            SubPlan subPlan,
            WarningCollector warningCollector,
            QueryManagerConfig.ExchangeMaterializationStrategy exchangeMaterializationStrategy,
            int maxStageCount,
            int stageCountSoftLimit)
    {
        subPlan.sanityCheck();

        int fragmentCount = subPlan.getAllFragments().size();
        if (fragmentCount > maxStageCount) {
            throw new PrestoException(QUERY_HAS_TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the allowed maximum (%s). " + TOO_MANY_STAGES_MESSAGE,
                    fragmentCount, maxStageCount));
        }

        // When exchange materialization is enabled, only a limited number of stages will be executed concurrently
        //  (controlled by session property max_concurrent_materializations)
        if (exchangeMaterializationStrategy != QueryManagerConfig.ExchangeMaterializationStrategy.ALL) {
            if (fragmentCount > stageCountSoftLimit) {
                warningCollector.add(new PrestoWarning(TOO_MANY_STAGES, format(
                        "Number of stages in the query (%s) exceeds the soft limit (%s). " + TOO_MANY_STAGES_MESSAGE,
                        fragmentCount, stageCountSoftLimit)));
            }
        }
    }

    /*
     * In theory, recoverable grouped execution should be decided at query section level (i.e. a connected component of stages connected by remote exchanges).
     * This is because supporting mixed recoverable execution and non-recoverable execution within a query section adds unnecessary complications but provides little benefit,
     * because a single task failure is still likely to fail the non-recoverable stage.
     * However, since the concept of "query section" is not introduced until execution time as of now, it needs significant hacks to decide at fragmenting time.

     * TODO: We should introduce "query section" and make recoverability analysis done at query section level.
     */
    private static SubPlan analyzeGroupedExecution(Session session, SubPlan subPlan, boolean parentContainsTableFinish, Metadata metadata, NodePartitioningManager nodePartitioningManager)
    {
        PlanFragment fragment = subPlan.getFragment();
        GroupedExecutionTagger.GroupedExecutionProperties properties = fragment.getRoot().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager), null);
        if (properties.isSubTreeUseful()) {
            boolean preferDynamic = fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE);
            BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, fragment.getPartitioning(), preferDynamic);
            if (bucketNodeMap.isDynamic()) {
                /*
                 * We currently only support recoverable grouped execution if the following statements hold true:
                 *   - Current session enables recoverable grouped execution and table writer merge operator
                 *   - Parent sub plan contains TableFinishNode
                 *   - Current sub plan's root is TableWriterMergeNode or TableWriterNode
                 *   - Input connectors supports split source rewind
                 *   - Output connectors supports partition commit
                 *   - Bucket node map uses dynamic scheduling
                 *   - One table writer per task
                 */
                boolean recoverable = isRecoverableGroupedExecutionEnabled(session) &&
                        isTableWriterMergeOperatorEnabled(session) &&
                        parentContainsTableFinish &&
                        (fragment.getRoot() instanceof TableWriterMergeNode || fragment.getRoot() instanceof TableWriterNode) &&
                        properties.isRecoveryEligible();
                if (recoverable) {
                    fragment = fragment.withRecoverableGroupedExecution(properties.getCapableTableScanNodes(), properties.getTotalLifespans());
                }
                else {
                    fragment = fragment.withDynamicLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes(), properties.getTotalLifespans());
                }
            }
            else {
                fragment = fragment.withFixedLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes(), properties.getTotalLifespans());
            }
        }
        ImmutableList.Builder<SubPlan> result = ImmutableList.builder();
        boolean containsTableFinishNode = containsTableFinishNode(fragment);
        for (SubPlan child : subPlan.getChildren()) {
            result.add(analyzeGroupedExecution(session, child, containsTableFinishNode, metadata, nodePartitioningManager));
        }
        return new SubPlan(fragment, result.build());
    }

    private static boolean containsTableFinishNode(PlanFragment planFragment)
    {
        PlanNode root = planFragment.getRoot();
        return root instanceof OutputNode && getOnlyElement(root.getSources()) instanceof TableFinishNode;
    }

    private static SubPlan reassignPartitioningHandleIfNecessary(Metadata metadata, Session session, SubPlan subPlan)
    {
        return reassignPartitioningHandleIfNecessaryHelper(metadata, session, subPlan, subPlan.getFragment().getPartitioning());
    }

    private static SubPlan reassignPartitioningHandleIfNecessaryHelper(Metadata metadata, Session session, SubPlan subPlan, PartitioningHandle newOutputPartitioningHandle)
    {
        PlanFragment fragment = subPlan.getFragment();

        PlanNode newRoot = fragment.getRoot();
        // If the fragment's partitioning is SINGLE or COORDINATOR_ONLY, leave the sources as is (this is for single-node execution)
        if (!fragment.getPartitioning().isSingleNode()) {
            PartitioningHandleReassigner partitioningHandleReassigner = new PartitioningHandleReassigner(fragment.getPartitioning(), metadata, session);
            newRoot = SimplePlanRewriter.rewriteWith(partitioningHandleReassigner, newRoot, new PartitioningHandleReassigner.Context());
        }
        PartitioningScheme outputPartitioningScheme = fragment.getPartitioningScheme();
        Partitioning newOutputPartitioning = outputPartitioningScheme.getPartitioning();
        if (outputPartitioningScheme.getPartitioning().getHandle().getConnectorId().isPresent()) {
            // Do not replace the handle if the source's output handle is a system one, e.g. broadcast.
            newOutputPartitioning = newOutputPartitioning.withAlternativePartitioningHandle(newOutputPartitioningHandle);
        }
        PlanFragment newFragment = new PlanFragment(
                fragment.getId(),
                newRoot,
                fragment.getVariables(),
                fragment.getPartitioning(),
                fragment.getTableScanSchedulingOrder(),
                new PartitioningScheme(
                        newOutputPartitioning,
                        outputPartitioningScheme.getOutputLayout(),
                        outputPartitioningScheme.getHashColumn(),
                        outputPartitioningScheme.isReplicateNullsAndAny(),
                        outputPartitioningScheme.getBucketToPartition()),
                fragment.getStageExecutionDescriptor(),
                fragment.isOutputTableWriterFragment(),
                fragment.getStatsAndCosts(),
                fragment.getJsonRepresentation());

        ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            childrenBuilder.add(reassignPartitioningHandleIfNecessaryHelper(metadata, session, child, fragment.getPartitioning()));
        }
        return new SubPlan(newFragment, childrenBuilder.build());
    }

    public static Set<PlanNodeId> getTableWriterNodeIds(PlanNode plan)
    {
        return stream(forTree(PlanNode::getSources).depthFirstPreOrder(plan))
                .filter(node -> node instanceof TableWriterNode)
                .map(PlanNode::getId)
                .collect(toImmutableSet());
    }

    private static final class PartitioningHandleReassigner
            extends SimplePlanRewriter<PartitioningHandleReassigner.Context>
    {
        private final PartitioningHandle fragmentPartitioningHandle;
        private final Metadata metadata;
        private final Session session;

        public PartitioningHandleReassigner(PartitioningHandle fragmentPartitioningHandle, Metadata metadata, Session session)
        {
            this.fragmentPartitioningHandle = fragmentPartitioningHandle;
            this.metadata = metadata;
            this.session = session;
        }

        private static final class Context
        {
            private boolean replicatedReadsAllowed;

            public boolean isReplicatedReadsAllowed()
            {
                return replicatedReadsAllowed;
            }

            public void setReplicatedReadsAllowed(boolean replicatedReadsAllowed)
            {
                this.replicatedReadsAllowed = replicatedReadsAllowed;
            }
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<PartitioningHandleReassigner.Context> context)
        {
            if (determineReplicatedReadsJoinAllowed(node)) {
                context.get().setReplicatedReadsAllowed(true);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            boolean isReplicatedReadsAllowed = context.get().isReplicatedReadsAllowed();
            PartitioningHandle partitioning = metadata.getLayout(session, node.getTable(), isReplicatedReadsAllowed)
                    .getTablePartitioning()
                    .map(TableLayout.TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);

            // if there's a replicated read join and fragmentPartitioningHandle is Source Distribution, need to use alternative table handle.
            // that's the case of non-bucket join nonpart table, need to create a virtual bucket handle for the table partitioning
            if (partitioning.equals(fragmentPartitioningHandle)) {
                if (!isReplicatedReadsAllowed) {
                    // do nothing if the current scan node's partitioning matches the fragment's
                    return node;
                }
            }

            TableHandle newTableHandle = metadata.getAlternativeTableHandle(session, node.getTable(), fragmentPartitioningHandle, isReplicatedReadsAllowed);
            return new TableScanNode(
                    node.getSourceLocation(),
                    node.getId(),
                    newTableHandle,
                    node.getOutputVariables(),
                    node.getAssignments(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }
    }

    public static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }
}
