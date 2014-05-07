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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public final class PlanRewriter<C>
{
    private final PlanNodeRewriter<C> nodeRewriter;
    private final PlanVisitor<Context<C>, PlanNode> visitor;

    public static <C, T extends PlanNode> T rewriteWith(PlanNodeRewriter<C> rewriter, T node)
    {
        return rewriteWith(rewriter, node, null);
    }

    public static <C, T extends PlanNode> T rewriteWith(PlanNodeRewriter<C> rewriter, T node, C context)
    {
        return new PlanRewriter<>(rewriter).rewrite(node, context);
    }

    public PlanRewriter(PlanNodeRewriter<C> nodeRewriter)
    {
        this.nodeRewriter = nodeRewriter;
        this.visitor = new RewritingVisitor();
    }

    public <T extends PlanNode> T rewrite(T node, C context)
    {
        return (T) node.accept(visitor, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the node rewriter for the provided node.
     */
    public <T extends PlanNode> T defaultRewrite(T node, C context)
    {
        return (T) node.accept(visitor, new Context<>(context, true));
    }

    public static <C, T extends PlanNode> Function<PlanNode, T> rewriteFunction(final PlanNodeRewriter<C> rewriter)
    {
        return new Function<PlanNode, T>()
        {
            @Override
            public T apply(PlanNode node)
            {
                return (T) rewriteWith(rewriter, node);
            }
        };
    }

    private class RewritingVisitor
            extends PlanVisitor<PlanRewriter.Context<C>, PlanNode>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteNode(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            throw new UnsupportedOperationException("not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteExchange(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteAggregation(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new AggregationNode(node.getId(), source, node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), node.getStep(), node.getSampleWeight(), node.getConfidence());
            }

            return node;
        }

        @Override
        public PlanNode visitMaterializeSample(MaterializeSampleNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteMaterializeSample(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new MaterializeSampleNode(node.getId(), source, node.getSampleWeightSymbol());
            }

            return node;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteMarkDistinct(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new MarkDistinctNode(node.getId(), source, node.getMarkerSymbol(), node.getDistinctSymbols(), node.getSampleWeightSymbol());
            }

            return node;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteWindow(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new WindowNode(node.getId(), source, node.getPartitionBy(), node.getOrderBy(), node.getOrderings(), node.getWindowFunctions(), node.getSignatures());
            }

            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteFilter(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new FilterNode(node.getId(), source, node.getPredicate());
            }

            return node;
        }

        @Override
        public PlanNode visitSample(SampleNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteSample(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new SampleNode(node.getId(), source, node.getSampleRatio(), node.getSampleType(), node.isRescaled(), node.getSampleWeightSymbol());
            }

            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteProject(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new ProjectNode(node.getId(), source, node.getOutputMap());
            }

            return node;
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteTopN(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new TopNNode(node.getId(), source, node.getCount(), node.getOrderBy(), node.getOrderings(), node.isPartial(), node.getSampleWeight());
            }

            return node;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteOutput(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputSymbols());
            }

            return node;
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteLimit(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new LimitNode(node.getId(), source, node.getCount(), node.getSampleWeight());
            }

            return node;
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteDistinctLimit(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new DistinctLimitNode(node.getId(), source, node.getLimit());
            }

            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteTableScan(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public PlanNode visitValues(ValuesNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteValues(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteTableWriter(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new TableWriterNode(node.getId(), source, node.getTarget(), node.getColumns(), node.getColumnNames(), node.getOutputSymbols(), node.getSampleWeightSymbol(), node.getCatalog(), node.getTableMetadata(), node.isSampleWeightSupported());
            }

            return node;
        }

        @Override
        public PlanNode visitTableCommit(TableCommitNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteTableCommit(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new TableCommitNode(node.getId(), source, node.getTarget(), node.getOutputSymbols());
            }

            return node;
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteIndexSource(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteJoin(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode left = rewrite(node.getLeft(), context.get());
            PlanNode right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new JoinNode(node.getId(), node.getType(), left, right, node.getCriteria());
            }

            return node;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteSemiJoin(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());
            PlanNode filteringSource = rewrite(node.getFilteringSource(), context.get());

            if (source != node.getSource() || filteringSource != node.getFilteringSource()) {
                return new SemiJoinNode(node.getId(), source, filteringSource, node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
            }

            return node;
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteIndexJoin(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode probeSource = rewrite(node.getProbeSource(), context.get());
            PlanNode indexSource = rewrite(node.getIndexSource(), context.get());

            if (probeSource != node.getProbeSource() || indexSource != node.getIndexSource()) {
                return new IndexJoinNode(node.getId(), node.getType(), probeSource, indexSource, node.getCriteria());
            }

            return node;
        }

        @Override
        public PlanNode visitSort(SortNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteSort(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            PlanNode source = rewrite(node.getSource(), context.get());

            if (source != node.getSource()) {
                return new SortNode(node.getId(), source, node.getOrderBy(), node.getOrderings());
            }

            return node;
        }

        @Override
        public PlanNode visitUnion(UnionNode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                PlanNode result = nodeRewriter.rewriteUnion(node, context.get(), PlanRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (PlanNode subPlan : node.getSources()) {
                PlanNode rewriteSubPlan = rewrite(subPlan, context.get());
                if (rewriteSubPlan != subPlan) {
                    modified = true;
                }
                builder.add(rewriteSubPlan);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping());
            }

            return node;
        }
    }

    public static class Context<C>
    {
        private boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }
}
