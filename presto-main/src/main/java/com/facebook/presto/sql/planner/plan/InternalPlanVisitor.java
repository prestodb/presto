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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.sql.planner.CanonicalJoinNode;
import com.facebook.presto.sql.planner.CanonicalTableScanNode;
import com.facebook.presto.sql.planner.StatsEquivalentPlanNodeWithLimit;
import com.facebook.presto.sql.planner.iterative.GroupReference;

public abstract class InternalPlanVisitor<R, C>
        extends PlanVisitor<R, C>
{
    public R visitRemoteSource(RemoteSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOutput(OutputNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSample(SampleNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExplainAnalyze(ExplainAnalyzeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexSource(IndexSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitJoin(JoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStarJoin(StarJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSemiJoin(SemiJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSpatialJoin(SpatialJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexJoin(IndexJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeJoin(MergeJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOffset(OffsetNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSort(SortNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitWindow(WindowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableWriter(TableWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableWriteMerge(TableWriterMergeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitDelete(DeleteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMetadataDelete(MetadataDeleteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFinish(TableFinishNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStatisticsWriterNode(StatisticsWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUnnest(UnnestNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitGroupId(GroupIdNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitRowNumber(RowNumberNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopNRowNumber(TopNRowNumberNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExchange(ExchangeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitEnforceSingleRow(EnforceSingleRowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitApply(ApplyNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAssignUniqueId(AssignUniqueId node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitGroupReference(GroupReference node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLateralJoin(LateralJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCanonicalTableScan(CanonicalTableScanNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCanonicalJoinNode(CanonicalJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStatsEquivalentPlanNodeWithLimit(StatsEquivalentPlanNodeWithLimit node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitNativeExecution(NativeExecutionNode node, C context)
    {
        return visitPlan(node, context);
    }
}
