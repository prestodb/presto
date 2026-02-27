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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.scheduler.DynamicFilterService;
import com.facebook.presto.execution.scheduler.JoinDynamicFilter;
import com.facebook.presto.execution.scheduler.TableScanDynamicFilter;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.SampledSplitSource;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSourceProvider;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterMaxSize;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterMaxWaitTime;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterExtendedMetrics;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.REWINDABLE_GROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class SplitSourceFactory
{
    private static final Logger log = Logger.get(SplitSourceFactory.class);

    private final SplitSourceProvider splitSourceProvider;
    private final WarningCollector warningCollector;
    private final DynamicFilterService dynamicFilterService;
    private final Metadata metadata;

    public SplitSourceFactory(
            SplitSourceProvider splitSourceProvider,
            WarningCollector warningCollector,
            DynamicFilterService dynamicFilterService,
            Metadata metadata)
    {
        this.splitSourceProvider = requireNonNull(splitSourceProvider, "splitSourceProvider is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    /**
     * Must be called before {@link #createSplitSources}.
     */
    public void registerDynamicFilters(PlanFragment fragment, Session session)
    {
        if (!isDistributedDynamicFilterEnabled(session)) {
            return;
        }
        QueryId queryId = session.getQueryId();
        Duration waitTimeout = getDistributedDynamicFilterMaxWaitTime(session);

        // Step 1: Register filters from this fragment's JoinNodes and SemiJoinNodes.
        // Same-fragment filters are matched from the JoinNode's probe child (not the
        // fragment root), because the fragment root's ProjectNode may strip the probe
        // column (it's consumed by the join but not needed downstream). Matching from
        // the probe child guarantees the probe variable is in scope.
        Set<PlanFragmentId> probeChildFragmentIds = new HashSet<>();

        List<JoinNode> joinNodes = PlanNodeSearcher.searchFrom(fragment.getRoot())
                .where(node -> node instanceof JoinNode)
                .findAll();

        for (JoinNode joinNode : joinNodes) {
            if (joinNode.getDynamicFilters().isEmpty()) {
                continue;
            }

            Map<VariableReferenceExpression, VariableReferenceExpression> buildToProbe = new HashMap<>();
            for (EquiJoinClause clause : joinNode.getCriteria()) {
                buildToProbe.put(clause.getRight(), clause.getLeft());
            }

            Map<String, Set<String>> localFilterColumns = new HashMap<>();
            Set<String> joinFilterIds = joinNode.getDynamicFilters().keySet();
            for (Map.Entry<String, VariableReferenceExpression> entry : joinNode.getDynamicFilters().entrySet()) {
                VariableReferenceExpression probeVar = buildToProbe.get(entry.getValue());
                String columnName = probeVar != null ? probeVar.getName() : "";
                registerFilterIfAbsent(queryId, entry.getKey(), columnName, waitTimeout, session);
                if (!columnName.isEmpty()) {
                    localFilterColumns.computeIfAbsent(columnName, k -> new HashSet<>()).add(entry.getKey());
                }
            }

            if (!localFilterColumns.isEmpty()) {
                matchFiltersToScans(joinNode.getLeft(), localFilterColumns, queryId);
            }

            // Register probe-side fragment targets for cross-fragment matching.
            // Follow the probe chain through nested JoinNodes/SemiJoinNodes to find
            // RemoteSourceNodes on the probe path only (never the build path).
            registerProbeFragmentTargets(queryId, joinNode.getLeft(), joinFilterIds, probeChildFragmentIds);
        }

        List<SemiJoinNode> semiJoinNodes = PlanNodeSearcher.searchFrom(fragment.getRoot())
                .where(node -> node instanceof SemiJoinNode)
                .findAll();

        for (SemiJoinNode semiJoinNode : semiJoinNodes) {
            if (semiJoinNode.getDynamicFilters().isEmpty()) {
                continue;
            }

            String columnName = semiJoinNode.getSourceJoinVariable().getName();
            Map<String, Set<String>> localFilterColumns = new HashMap<>();
            Set<String> semiJoinFilterIds = semiJoinNode.getDynamicFilters().keySet();
            for (String filterId : semiJoinFilterIds) {
                registerFilterIfAbsent(queryId, filterId, columnName, waitTimeout, session);
                if (!columnName.isEmpty()) {
                    localFilterColumns.computeIfAbsent(columnName, k -> new HashSet<>()).add(filterId);
                }
            }

            if (!localFilterColumns.isEmpty()) {
                matchFiltersToScans(semiJoinNode.getSource(), localFilterColumns, queryId);
            }

            // Register probe-side (source) fragment targets for SemiJoinNodes.
            registerProbeFragmentTargets(queryId, semiJoinNode.getSource(), semiJoinFilterIds, probeChildFragmentIds);
        }

        // Step 2: Collect cross-fragment filters that were registered for this fragment
        // by a parent fragment's JoinNode/SemiJoinNode (probe-side matching only).
        // Cross-fragment filters are matched from the fragment root because the probe
        // column is guaranteed to be in the fragment root output (the parent fragment's
        // RemoteSourceNode references it).
        Map<String, Set<String>> crossFragmentFilterColumns = new HashMap<>();
        Set<String> crossFragmentFilterIds = dynamicFilterService.getProbeFragmentFilterIds(queryId, fragment.getId());
        for (String filterId : crossFragmentFilterIds) {
            dynamicFilterService.getFilter(queryId, filterId).ifPresent(joinFilter -> {
                String probeColumn = joinFilter.getColumnName();
                if (!probeColumn.isEmpty()) {
                    crossFragmentFilterColumns.computeIfAbsent(probeColumn, k -> new HashSet<>()).add(filterId);
                }
            });
        }

        // Step 3: Propagate cross-fragment filters to this fragment's probe-side
        // child fragments for transitive matching across fragment boundaries.
        // This enables filters from grandparent+ fragments to reach deep probe-side
        // scans (e.g., star schema: outer join filter reaches fact scan through an
        // intermediate JoinNode fragment). Since fragments are processed pre-order
        // (parent first), each level propagates to the next.
        if (!crossFragmentFilterIds.isEmpty() && !probeChildFragmentIds.isEmpty()) {
            for (PlanFragmentId probeChildFragId : probeChildFragmentIds) {
                dynamicFilterService.registerProbeFragmentFilter(queryId, probeChildFragId, crossFragmentFilterIds);
            }
        }

        if (!crossFragmentFilterColumns.isEmpty()) {
            matchFiltersToScans(fragment.getRoot(), crossFragmentFilterColumns, queryId);
        }
    }

    private void registerFilterIfAbsent(QueryId queryId, String filterId, String columnName, Duration waitTimeout, Session session)
    {
        if (!dynamicFilterService.hasFilter(queryId, filterId)) {
            JoinDynamicFilter filter = new JoinDynamicFilter(
                    filterId,
                    columnName,
                    waitTimeout,
                    getDistributedDynamicFilterMaxSize(session).toBytes(),
                    dynamicFilterService.getStats(),
                    session.getRuntimeStats(),
                    isDistributedDynamicFilterExtendedMetrics(session));
            dynamicFilterService.registerFilter(queryId, filterId, filter);
        }
    }

    private void matchFiltersToScans(PlanNode root, Map<String, Set<String>> filterColumnToFilterIds, QueryId queryId)
    {
        Map<PlanNodeId, Set<String>> scanToFilterIds = root.accept(new FilterToScanMatcher(), filterColumnToFilterIds);
        for (Map.Entry<PlanNodeId, Set<String>> entry : scanToFilterIds.entrySet()) {
            dynamicFilterService.registerScanFilterMapping(queryId, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Follows the probe chain from {@code probeRoot} to find RemoteSourceNodes,
     * then registers the given filter IDs for each target fragment. Also adds
     * the target fragment IDs to {@code probeChildFragmentIds} for transitive
     * propagation in Step 3.
     */
    private void registerProbeFragmentTargets(
            QueryId queryId,
            PlanNode probeRoot,
            Set<String> filterIds,
            Set<PlanFragmentId> probeChildFragmentIds)
    {
        for (RemoteSourceNode remoteSource : collectProbeChainRemoteSources(probeRoot)) {
            for (PlanFragmentId probeFragId : remoteSource.getSourceFragmentIds()) {
                probeChildFragmentIds.add(probeFragId);
                dynamicFilterService.registerProbeFragmentFilter(queryId, probeFragId, filterIds);
            }
        }
    }

    /**
     * Follows the probe chain through nested JoinNodes and SemiJoinNodes to find
     * RemoteSourceNodes that are on the probe path. At join boundaries, only the
     * probe child (left for JoinNode, source for SemiJoinNode) is traversed.
     * Build-side children are skipped to prevent registering cross-fragment filters
     * for fragments that feed into the build side (which could create circular
     * dependencies where a scan waits for a filter that needs that scan's data).
     */
    private static List<RemoteSourceNode> collectProbeChainRemoteSources(PlanNode node)
    {
        if (node instanceof RemoteSourceNode) {
            return ImmutableList.of((RemoteSourceNode) node);
        }
        if (node instanceof JoinNode) {
            return collectProbeChainRemoteSources(((JoinNode) node).getLeft());
        }
        if (node instanceof SemiJoinNode) {
            return collectProbeChainRemoteSources(((SemiJoinNode) node).getSource());
        }
        ImmutableList.Builder<RemoteSourceNode> result = ImmutableList.builder();
        for (PlanNode child : node.getSources()) {
            result.addAll(collectProbeChainRemoteSources(child));
        }
        return result.build();
    }

    public Map<PlanNodeId, SplitSource> createSplitSources(PlanFragment fragment, Session session, TableWriteInfo tableWriteInfo)
    {
        ImmutableList.Builder<SplitSource> splitSources = ImmutableList.builder();
        try {
            return fragment.getRoot().accept(new Visitor(session, fragment.getStageExecutionDescriptor(), splitSources), new Context(tableWriteInfo));
        }
        catch (Throwable t) {
            splitSources.build().forEach(SplitSourceFactory::closeSplitSource);
            throw t;
        }
    }

    private static void closeSplitSource(SplitSource source)
    {
        try {
            source.close();
        }
        catch (Throwable t) {
            log.warn(t, "Error closing split source");
        }
    }

    private static SplitSchedulingStrategy getSplitSchedulingStrategy(StageExecutionDescriptor stageExecutionDescriptor, PlanNodeId scanNodeId)
    {
        if (stageExecutionDescriptor.isRecoverableGroupedExecution()) {
            return REWINDABLE_GROUPED_SCHEDULING;
        }
        if (stageExecutionDescriptor.isScanGroupedExecution(scanNodeId)) {
            return GROUPED_SCHEDULING;
        }
        return UNGROUPED_SCHEDULING;
    }

    private final class Visitor
            extends InternalPlanVisitor<Map<PlanNodeId, SplitSource>, Context>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final ImmutableList.Builder<SplitSource> splitSources;

        private Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor, ImmutableList.Builder<SplitSource> allSplitSources)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
            this.splitSources = allSplitSources;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExplainAnalyze(ExplainAnalyzeNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableScan(TableScanNode node, Context context)
        {
            TableHandle table = node.getTable();

            DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
            if (isDistributedDynamicFilterEnabled(session)) {
                Set<String> filterIds = dynamicFilterService.getFilterIdsForScan(session.getQueryId(), node.getId());
                if (!filterIds.isEmpty()) {
                    Map<String, ColumnHandle> variableNameToHandle = new HashMap<>();
                    for (Map.Entry<VariableReferenceExpression, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                        variableNameToHandle.put(assignment.getKey().getName(), assignment.getValue());
                    }

                    List<JoinDynamicFilter> matchingFilters = new ArrayList<>();
                    Map<String, ColumnHandle> columnNameToHandle = new HashMap<>();
                    for (String filterId : filterIds) {
                        dynamicFilterService.getFilter(session.getQueryId(), filterId).ifPresent(joinFilter -> {
                            String probeColumn = joinFilter.getColumnName();
                            matchingFilters.add(joinFilter);
                            columnNameToHandle.put(probeColumn, variableNameToHandle.get(probeColumn));
                        });
                    }

                    if (!matchingFilters.isEmpty()) {
                        // Set probe-side column domain for runtime short-circuit detection
                        if (table.getLayout().isPresent()) {
                            TableLayout layout = metadata.getLayout(session, table);
                            TupleDomain<ColumnHandle> predicate = layout.getPredicate();
                            if (predicate.getDomains().isPresent()) {
                                for (JoinDynamicFilter joinFilter : matchingFilters) {
                                    ColumnHandle handle = columnNameToHandle.get(joinFilter.getColumnName());
                                    if (handle != null) {
                                        Domain columnDomain = predicate.getDomains().get().get(handle);
                                        if (columnDomain != null) {
                                            joinFilter.setProbeColumnDomain(columnDomain);
                                        }
                                    }
                                }
                            }
                        }
                        dynamicFilter = new TableScanDynamicFilter(matchingFilters, columnNameToHandle);
                    }
                }
            }

            DynamicFilter finalDynamicFilter = dynamicFilter;
            Supplier<SplitSource> splitSourceSupplier = () -> splitSourceProvider.getSplits(
                    session,
                    table,
                    getSplitSchedulingStrategy(stageExecutionDescriptor, node.getId()),
                    warningCollector,
                    finalDynamicFilter);

            SplitSource splitSource = new LazySplitSource(splitSourceSupplier);

            splitSources.add(splitSource);

            return ImmutableMap.of(node.getId(), splitSource);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Context context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMergeJoin(MergeJoinNode node, Context context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSemiJoin(SemiJoinNode node, Context context)
        {
            Map<PlanNodeId, SplitSource> sourceSplits = node.getSource().accept(this, context);
            Map<PlanNodeId, SplitSource> filteringSourceSplits = node.getFilteringSource().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(sourceSplits)
                    .putAll(filteringSourceSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSpatialJoin(SpatialJoinNode node, Context context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitIndexJoin(IndexJoinNode node, Context context)
        {
            return node.getProbeSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Context context)
        {
            // remote source node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Context context)
        {
            // values node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitFilter(FilterNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSample(SampleNode node, Context context)
        {
            switch (node.getSampleType()) {
                case BERNOULLI:
                    return node.getSource().accept(this, context);
                case SYSTEM:
                    Map<PlanNodeId, SplitSource> nodeSplits = node.getSource().accept(this, context);
                    // TODO: when this happens we should switch to either BERNOULLI or page sampling
                    if (nodeSplits.size() == 1) {
                        PlanNodeId planNodeId = getOnlyElement(nodeSplits.keySet());
                        SplitSource sampledSplitSource = new SampledSplitSource(nodeSplits.get(planNodeId), node.getSampleRatio());
                        return ImmutableMap.of(planNodeId, sampledSplitSource);
                    }
                    // table sampling on a sub query without splits is meaningless
                    return nodeSplits;

                default:
                    throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
            }
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAggregation(AggregationNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitGroupId(GroupIdNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMarkDistinct(MarkDistinctNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitWindow(WindowNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRowNumber(RowNumberNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFunctionProcessor(TableFunctionProcessorNode node, Context context)
        {
            if (!node.getSource().isPresent()) {
                // this is a source node, so produce splits
                SplitSource splitSource = splitSourceProvider.getSplits(
                        session,
                        node.getHandle());
                splitSources.add(splitSource);
                return ImmutableMap.of(node.getId(), splitSource);
            }

            return node.getSource().orElseThrow(NoSuchElementException::new).accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopNRowNumber(TopNRowNumberNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitProject(ProjectNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnnest(UnnestNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopN(TopNNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitOutput(OutputNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitEnforceSingleRow(EnforceSingleRowNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAssignUniqueId(AssignUniqueId node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitLimit(LimitNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDistinctLimit(DistinctLimitNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSort(SortNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableWriter(TableWriterNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitCallDistributedProcedure(CallDistributedProcedureNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableWriteMerge(TableWriterMergeNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFinish(TableFinishNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitStatisticsWriterNode(StatisticsWriterNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDelete(DeleteNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUpdate(UpdateNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMetadataDelete(MetadataDeleteNode node, Context context)
        {
            // MetadataDelete node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnion(UnionNode node, Context context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExchange(ExchangeNode node, Context context)
        {
            return processSources(node.getSources(), context);
        }

        private Map<PlanNodeId, SplitSource> processSources(List<PlanNode> sources, Context context)
        {
            ImmutableMap.Builder<PlanNodeId, SplitSource> result = ImmutableMap.builder();
            for (PlanNode child : sources) {
                result.putAll(child.accept(this, context));
            }

            return result.build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitPlan(PlanNode node, Context context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMergeWriter(MergeWriterNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMergeProcessor(MergeProcessorNode node, Context context)
        {
            return node.getSource().accept(this, context);
        }
    }

    private static class FilterToScanMatcher
            extends InternalPlanVisitor<Map<PlanNodeId, Set<String>>, Map<String, Set<String>>>
    {
        @Override
        public Map<PlanNodeId, Set<String>> visitTableScan(TableScanNode node, Map<String, Set<String>> filterColumnToFilterIds)
        {
            Set<String> matchingFilterIds = new HashSet<>();
            for (VariableReferenceExpression var : node.getAssignments().keySet()) {
                Set<String> filterIds = filterColumnToFilterIds.get(var.getName());
                if (filterIds != null) {
                    matchingFilterIds.addAll(filterIds);
                }
            }
            if (matchingFilterIds.isEmpty()) {
                return ImmutableMap.of();
            }
            return ImmutableMap.of(node.getId(), matchingFilterIds);
        }

        @Override
        public Map<PlanNodeId, Set<String>> visitProject(ProjectNode node, Map<String, Set<String>> filterColumnToFilterIds)
        {
            Map<String, Set<String>> childContext = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : node.getAssignments().getMap().entrySet()) {
                String outputName = assignment.getKey().getName();
                Set<String> filterIds = filterColumnToFilterIds.get(outputName);
                if (filterIds != null && assignment.getValue() instanceof VariableReferenceExpression) {
                    String inputName = ((VariableReferenceExpression) assignment.getValue()).getName();
                    childContext.computeIfAbsent(inputName, k -> new HashSet<>()).addAll(filterIds);
                }
            }
            return node.getSource().accept(this, childContext);
        }

        @Override
        public Map<PlanNodeId, Set<String>> visitPlan(PlanNode node, Map<String, Set<String>> filterColumnToFilterIds)
        {
            ImmutableMap.Builder<PlanNodeId, Set<String>> result = ImmutableMap.builder();
            for (PlanNode child : node.getSources()) {
                result.putAll(child.accept(this, filterColumnToFilterIds));
            }
            return result.build();
        }
    }

    private static class Context
    {
        private final TableWriteInfo tableWriteInfo;

        public Context(TableWriteInfo tableWriteInfo)
        {
            this.tableWriteInfo = tableWriteInfo;
        }

        public TableWriteInfo getTableWriteInfo()
        {
            return tableWriteInfo;
        }
    }
}
