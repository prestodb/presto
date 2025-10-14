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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.google.common.collect.ImmutableList;

public class CopyPlanVisitor
        extends InternalPlanVisitor<PlanNode, Memo>
{
    @Override
    public PlanNode visitPlan(PlanNode node, Memo memo)
    {
        return node.accept(this, memo);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Memo memo)
    {
        PlanNode newSource = node.getSource().accept(this, memo);
        PlanNodeId newPLanNodeId = memo.getIdAllocator().getNextId();
        return new ProjectNode(
            node.getSourceLocation(),
                newPLanNodeId,
                newSource,
                node.getAssignments(),
                node.getLocality());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Memo memo)
    {
        PlanNode newSource = node.getSource().accept(this, memo);
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        return new FilterNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getPredicate());
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, Memo memo)
    {
        PlanNode newSource = node.getSource().accept(this, memo);
        PlanNode newFilterSource = node.getSources().get(1).accept(this, memo);
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        return new SemiJoinNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                newFilterSource,
                node.getSourceJoinVariable(),
                node.getFilteringSourceJoinVariable(),
                node.getSemiJoinOutput(),
                node.getSourceHashVariable(),
                node.getFilteringSourceHashVariable(),
                node.getDistributionType(),
                node.getDynamicFilters());
    }

    @Override
    public PlanNode visitUnion(UnionNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        ImmutableList<PlanNode> newSources = node.getSources().stream()
                .map(source -> source.accept(this, memo))
                .collect(ImmutableList.toImmutableList());
        return new UnionNode(
                node.getSourceLocation(),
                newPlanNodeId,
                newSources,
                node.getOutputVariables(),
                node.getVariableMapping());
    }

    @Override
    public PlanNode visitOutput(OutputNode node, Memo memo)
    {
        PlanNode newSource = node.getSource().accept(this, memo);
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        return new OutputNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getColumnNames(),
                node.getOutputVariables());
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new AggregationNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getAggregations(),
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable(),
                node.getAggregationId()); // ToDo Check if the aggregationId also need to be unique
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new TableScanNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getTable(),
                node.getOutputVariables(),
                node.getAssignments(),
                node.getCurrentConstraint(),
                node.getEnforcedConstraint(),
                node.getCteMaterializationInfo());
    }

    @Override
    public PlanNode visitSort(SortNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new SortNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getOrderingScheme(),
                node.isPartial(),
                node.getPartitionBy());
    }

    @Override
    public PlanNode visitIntersect(IntersectNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        ImmutableList<PlanNode> newSources = node.getSources().stream()
                .map(source -> source.accept(this, memo))
                .collect(ImmutableList.toImmutableList());

        return new IntersectNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSources,
                node.getOutputVariables(),
                node.getVariableMapping());
    }

    @Override
    public PlanNode visitExcept(ExceptNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        ImmutableList<PlanNode> newSources = node.getSources().stream()
                .map(source -> source.accept(this, memo))
                .collect(ImmutableList.toImmutableList());

        return new ExceptNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSources,
                node.getOutputVariables(),
                node.getVariableMapping());
    }

    @Override
    public PlanNode visitValues(ValuesNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new ValuesNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getOutputVariables(),
                node.getRows(),
                node.getValuesNodeLabel());
    }

    @Override
    public PlanNode visitLimit(LimitNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new LimitNode(
                node.getSourceLocation(),
                newPlanNodeId,
                newSource,
                node.getCount(),
                node.getStep());
    }

    @Override
    public PlanNode visitMarkDistinct(MarkDistinctNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new MarkDistinctNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getMarkerVariable(),
                node.getDistinctVariables(),
                node.getHashVariable());
    }

    @Override
    public PlanNode visitTopN(TopNNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new TopNNode(
                node.getSourceLocation(),
                newPlanNodeId,
                newSource,
                node.getCount(),
                node.getOrderingScheme(),
                node.getStep());
    }

    @Override
    public PlanNode visitDistinctLimit(DistinctLimitNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new DistinctLimitNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getLimit(),
                node.isPartial(),
                node.getDistinctVariables(),
                node.getHashVariable(),
                node.getTimeoutMillis());
    }

    @Override
    public PlanNode visitCteReference(CteReferenceNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new CteReferenceNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getCteId());
    }

    @Override
    public PlanNode visitCteProducer(CteProducerNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new CteProducerNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getCteId(),
                node.getRowCountVariable(),
                node.getOutputVariables());
    }

    @Override
    public PlanNode visitCteConsumer(CteConsumerNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new CteConsumerNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getOutputVariables(),
                node.getCteId(),
                node.getOriginalSource());
    }

    @Override
    public PlanNode visitWindow(WindowNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new WindowNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getSpecification(),
                node.getWindowFunctions(),
                node.getHashVariable(),
                node.getPrePartitionedInputs(),
                node.getPreSortedOrderPrefix());
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Memo memo)
    {
        PlanNodeId newPLanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newLeft = node.getLeft().accept(this, memo);
        PlanNode newRight = node.getRight().accept(this, memo);

        return new JoinNode(
                node.getSourceLocation(),
                newPLanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                newLeft,
                newRight,
                node.getCriteria(),
                node.getOutputVariables(),
                node.getFilter(),
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType(),
                node.getDynamicFilters());
    }

    @Override
    public PlanNode visitSpatialJoin(SpatialJoinNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newLeft = node.getLeft().accept(this, memo);
        PlanNode newRight = node.getRight().accept(this, memo);

        return new SpatialJoinNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                newLeft,
                newRight,
                node.getOutputVariables(),
                node.getFilter(),
                node.getLeftPartitionVariable(),
                node.getRightPartitionVariable(),
                node.getKdbTree());
    }

    @Override
    public PlanNode visitMergeJoin(MergeJoinNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newLeft = node.getLeft().accept(this, memo);
        PlanNode newRight = node.getRight().accept(this, memo);

        return new MergeJoinNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                newLeft,
                newRight,
                node.getCriteria(),
                node.getOutputVariables(),
                node.getFilter(),
                node.getLeftHashVariable(),
                node.getRightHashVariable());
    }

    @Override
    public PlanNode visitDelete(DeleteNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new DeleteNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getRowId(),
                node.getOutputVariables(),
                node.getInputDistribution());
    }

    @Override
    public PlanNode visitTableWriter(TableWriterNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new TableWriterNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getTarget(),
                node.getRowCountVariable(),
                node.getFragmentVariable(),
                node.getTableCommitContextVariable(),
                node.getColumns(),
                node.getColumnNames(),
                node.getNotNullColumnVariables(),
                node.getTablePartitioningScheme(),
                node.getStatisticsAggregation(),
                node.getTaskCountIfScaledWriter(),
                node.getIsTemporaryTableWriter());
    }

    @Override
    public PlanNode visitTableFinish(TableFinishNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new TableFinishNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getTarget(),
                node.getRowCountVariable(),
                node.getStatisticsAggregation(),
                node.getStatisticsAggregationDescriptor(),
                node.getCteMaterializationInfo());
    }

    @Override
    public PlanNode visitIndexSource(IndexSourceNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new IndexSourceNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getIndexHandle(),
                node.getTableHandle(),
                node.getLookupVariables(),
                node.getOutputVariables(),
                node.getAssignments(),
                node.getCurrentConstraint());
    }

    @Override
    public PlanNode visitUnnest(UnnestNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new UnnestNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getReplicateVariables(),
                node.getUnnestVariables(),
                node.getOrdinalityVariable());
    }

    @Override
    public PlanNode visitMetadataDelete(MetadataDeleteNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new MetadataDeleteNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getTableHandle(),
                node.getOutput());
    }

    @Override
    public PlanNode visitRemoteSource(RemoteSourceNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new RemoteSourceNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getSourceFragmentIds(),
                node.getOutputVariables(),
                node.isEnsureSourceOrdering(),
                node.getOrderingScheme(),
                node.getExchangeType(),
                node.getEncoding());
    }

    @Override
    public PlanNode visitSample(SampleNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new SampleNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getSampleRatio(),
                node.getSampleType());
    }

    @Override
    public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new ExplainAnalyzeNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getOutputVariable(),
                node.isVerbose(),
                node.getFormat());
    }

    @Override
    public PlanNode visitIndexJoin(IndexJoinNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newProbeSource = node.getProbeSource().accept(this, memo);
        PlanNode newIndexSource = node.getIndexSource().accept(this, memo);

        return new IndexJoinNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                newProbeSource,
                newIndexSource,
                node.getCriteria(),
                node.getFilter(),
                node.getProbeHashVariable(),
                node.getIndexHashVariable(),
                node.getLookupVariables());
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new OffsetNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getCount());
    }

    @Override
    public PlanNode visitTableWriteMerge(TableWriterMergeNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new TableWriterMergeNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getRowCountVariable(),
                node.getFragmentVariable(),
                node.getTableCommitContextVariable(),
                node.getStatisticsAggregation());
    }

    @Override
    public PlanNode visitUpdate(UpdateNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new UpdateNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getRowId(),
                node.getColumnValueAndRowIdSymbols(),
                node.getOutputVariables());
    }

    @Override
    public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new StatisticsWriterNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getTableHandle(),
                node.getRowCountVariable(),
                node.isRowCountEnabled(),
                node.getDescriptor());
    }

    @Override
    public PlanNode visitGroupId(GroupIdNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new GroupIdNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getGroupingSets(),
                node.getGroupingColumns(),
                node.getAggregationArguments(),
                node.getGroupIdVariable());
    }

    @Override
    public PlanNode visitRowNumber(RowNumberNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new RowNumberNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getPartitionBy(),
                node.getRowNumberVariable(),
                node.getMaxRowCountPerPartition(),
                node.isPartial(),
                node.getHashVariable());
    }

    @Override
    public PlanNode visitTopNRowNumber(TopNRowNumberNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new TopNRowNumberNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getSpecification(),
                node.getRowNumberVariable(),
                node.getMaxRowCountPerPartition(),
                node.isPartial(),
                node.getHashVariable());
    }

    @Override
    public PlanNode visitExchange(ExchangeNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        ImmutableList<PlanNode> newSources = node.getSources().stream()
                .map(source -> source.accept(this, memo))
                .collect(ImmutableList.toImmutableList());

        return new ExchangeNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                node.getScope(),
                node.getPartitioningScheme(),
                newSources,
                node.getInputs(),
                node.isEnsureSourceOrdering(),
                node.getOrderingScheme());
    }

    @Override
    public PlanNode visitEnforceSingleRow(EnforceSingleRowNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new EnforceSingleRowNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource);
    }

    @Override
    public PlanNode visitApply(ApplyNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newInput = node.getInput().accept(this, memo);
        PlanNode newSubquery = node.getSubquery().accept(this, memo);

        return new ApplyNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newInput,
                newSubquery,
                node.getSubqueryAssignments(),
                node.getCorrelation(),
                node.getOriginSubqueryError(),
                node.getMayParticipateInAntiJoin());
    }

    @Override
    public PlanNode visitAssignUniqueId(AssignUniqueId node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newSource = node.getSource().accept(this, memo);

        return new AssignUniqueId(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newSource,
                node.getIdVariable());
    }

    @Override
    public PlanNode visitGroupReference(GroupReference node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode oldReferenceNode = memo.getNode(node.getGroupId());
        PlanNode newReferenceNode = oldReferenceNode.accept(this, memo);
        int newGroupId = memo.replicateGroup(node.getGroupId(), newReferenceNode);
        return new GroupReference(
                node.getSourceLocation(),
                newPlanNodeId,
                newGroupId,
                node.getOutputVariables(),
                node.getLogicalProperties());
    }

    @Override
    public PlanNode visitLateralJoin(LateralJoinNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newInput = node.getInput().accept(this, memo);
        PlanNode newSubquery = node.getSubquery().accept(this, memo);

        return new LateralJoinNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newInput,
                newSubquery,
                node.getCorrelation(),
                node.getType(),
                node.getOriginSubqueryError());
    }

    @Override
    public PlanNode visitCanonicalTableScan(CanonicalTableScanNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();

        return new CanonicalTableScanNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                node.getTable(),
                node.getOutputVariables(),
                node.getAssignments());
    }

    @Override
    public PlanNode visitStatsEquivalentPlanNodeWithLimit(StatsEquivalentPlanNodeWithLimit node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        PlanNode newPlan = node.getPlan().accept(this, memo);
        PlanNode newlimit = node.getLimit().accept(this, memo);

        return new StatsEquivalentPlanNodeWithLimit(
                newPlanNodeId,
                newPlan,
                newlimit);
    }

    @Override
    public PlanNode visitSequence(SequenceNode node, Memo memo)
    {
        PlanNodeId newPlanNodeId = memo.getIdAllocator().getNextId();
        ImmutableList<PlanNode> newCteProducerList = node.getCteProducers().stream()
                .map(source -> source.accept(this, memo))
                .collect(ImmutableList.toImmutableList());
        PlanNode newPrimarySource = node.getPrimarySource().accept(this, memo);

        return new SequenceNode(
                node.getSourceLocation(),
                newPlanNodeId,
                node.getStatsEquivalentPlanNode(),
                newCteProducerList,
                newPrimarySource,
                node.getCteDependencyGraph());
    }
}
