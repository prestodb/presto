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

import com.facebook.presto.spi.plan.MergeJoinNode;
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

    public R visitSample(SampleNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExplainAnalyze(ExplainAnalyzeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeJoin(MergeJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeWriter(MergeWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeProcessor(MergeProcessorNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOffset(OffsetNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableWriteMerge(TableWriterMergeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUpdate(UpdateNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStatisticsWriterNode(StatisticsWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCallDistributedProcedure(CallDistributedProcedureNode node, C context)
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

    public R visitSequence(SequenceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFunction(TableFunctionNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFunctionProcessor(TableFunctionProcessorNode node, C context)
    {
        return visitPlan(node, context);
    }
}
