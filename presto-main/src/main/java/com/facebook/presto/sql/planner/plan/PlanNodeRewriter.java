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

public class PlanNodeRewriter<C>
{
    public PlanNode rewriteNode(PlanNode node, C context, PlanRewriter<C> planRewriter)
    {
        return null;
    }

    public PlanNode rewriteLimit(LimitNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteDistinctLimit(DistinctLimitNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteExchange(ExchangeNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTopN(TopNNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTableScan(TableScanNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteValues(ValuesNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteProject(ProjectNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteFilter(FilterNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSample(SampleNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteIndexSource(IndexSourceNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteJoin(JoinNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSemiJoin(SemiJoinNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteIndexJoin(IndexJoinNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteAggregation(AggregationNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteMarkDistinct(MarkDistinctNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteMaterializeSample(MaterializeSampleNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteWindow(WindowNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteOutput(OutputNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSort(SortNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTableWriter(TableWriterNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTableCommit(TableCommitNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteUnion(UnionNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }
}
