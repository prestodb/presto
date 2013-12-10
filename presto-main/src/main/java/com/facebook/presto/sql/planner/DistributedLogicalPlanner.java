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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class DistributedLogicalPlanner
{
    private final Metadata metadata;
    private final PlanNodeIdAllocator idAllocator;

    public DistributedLogicalPlanner(Metadata metadata, PlanNodeIdAllocator idAllocator)
    {
        this.metadata = metadata;
        this.idAllocator = idAllocator;
    }

    public SubPlan createSubPlans(Plan plan, boolean createSingleNodePlan)
    {
        Visitor visitor = new Visitor(plan.getSymbolAllocator(), createSingleNodePlan);
        SubPlanBuilder builder = plan.getRoot().accept(visitor, null);

        SubPlan subplan = builder.build();
        subplan.sanityCheck();

        return subplan;
    }

    private class Visitor
            extends PlanVisitor<Void, SubPlanBuilder>
    {
        private int nextFragmentId = 0;

        private final SymbolAllocator allocator;
        private final boolean createSingleNodePlan;

        public Visitor(SymbolAllocator allocator, boolean createSingleNodePlan)
        {
            this.allocator = allocator;
            this.createSingleNodePlan = createSingleNodePlan;
        }

        @Override
        public SubPlanBuilder visitAggregation(AggregationNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (!current.isDistributed()) {
                // add the aggregation node as the root of the current fragment
                current.setRoot(new AggregationNode(node.getId(), current.getRoot(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), SINGLE, node.getSampleWeight(), node.getConfidence()));
                return current;
            }

            Map<Symbol, FunctionCall> aggregations = node.getAggregations();
            Map<Symbol, Signature> functions = node.getFunctions();
            Map<Symbol, Symbol> masks = node.getMasks();
            List<Symbol> groupBy = node.getGroupBy();

            boolean decomposable = true;
            for (Signature function : functions.values()) {
                if (!metadata.getExactFunction(function).getAggregationFunction().isDecomposable()) {
                    decomposable = false;
                    break;
                }
            }

            // else, we need to "close" the current fragment and create an unpartitioned fragment for the final aggregation
            if (decomposable) {
                return addDistributedAggregation(current, aggregations, functions, masks, groupBy, node.getSampleWeight(), node.getConfidence());
            }
            return addSingleNodeAggregation(current, aggregations, functions, masks, groupBy, node.getSampleWeight(), node.getConfidence());
        }

        @Override
        public SubPlanBuilder visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            // Check if the subplan is already partitioned the way we want it
            boolean alreadyPartitioned = false;
            if (current.getDistribution() == PlanDistribution.FIXED) {
                for (SubPlan child : current.getChildren()) {
                    if (child.getFragment().getOutputPartitioning() == OutputPartitioning.HASH &&
                            ImmutableSet.copyOf(child.getFragment().getPartitionBy()).equals(ImmutableSet.copyOf(node.getDistinctSymbols()))) {
                        alreadyPartitioned = true;
                        break;
                    }
                }
            }
            if (createSingleNodePlan || alreadyPartitioned || !current.isDistributed()) {
                MarkDistinctNode markNode = new MarkDistinctNode(idAllocator.getNextId(), current.getRoot(), node.getMarkerSymbol(), node.getDistinctSymbols(), node.getSampleWeightSymbol());
                current.setRoot(markNode);
                return current;
            }
            else {
                PlanNode sink = new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols());
                current.setRoot(sink)
                        .setHashOutputPartitioning(node.getDistinctSymbols());

                PlanNode exchange = new ExchangeNode(idAllocator.getNextId(), current.getId(), sink.getOutputSymbols());
                MarkDistinctNode markNode = new MarkDistinctNode(idAllocator.getNextId(), exchange, node.getMarkerSymbol(), node.getDistinctSymbols(), node.getSampleWeightSymbol());
                return createFixedDistributionPlan(markNode)
                        .addChild(current.build());
            }
        }

        private SubPlanBuilder addSingleNodeAggregation(SubPlanBuilder plan, Map<Symbol, FunctionCall> aggregations, Map<Symbol, Signature> functions, Map<Symbol, Symbol> masks, List<Symbol> groupBy, Optional<Symbol> sampleWeight, double confidence)
        {
            plan.setRoot(new SinkNode(idAllocator.getNextId(), plan.getRoot(), plan.getRoot().getOutputSymbols()));

            // create aggregation plan
            ExchangeNode source = new ExchangeNode(idAllocator.getNextId(), plan.getId(), plan.getRoot().getOutputSymbols());
            AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(), source, groupBy, aggregations, functions, masks, SINGLE, sampleWeight, confidence);
            plan = createSingleNodePlan(aggregation).addChild(plan.build());

            return plan;
        }

        private SubPlanBuilder addDistributedAggregation(SubPlanBuilder plan, Map<Symbol, FunctionCall> aggregations, Map<Symbol, Signature> functions, Map<Symbol, Symbol> masks, List<Symbol> groupBy, Optional<Symbol> sampleWeight, double confidence)
        {
            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : aggregations.entrySet()) {
                Signature signature = functions.get(entry.getKey());
                FunctionInfo function = metadata.getExactFunction(signature);

                Symbol intermediateSymbol = allocator.newSymbol(function.getName().getSuffix(), function.getIntermediateType());
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(function.getName(), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            // create partial aggregation plan
            AggregationNode partialAggregation = new AggregationNode(idAllocator.getNextId(), plan.getRoot(), groupBy, intermediateCalls, intermediateFunctions, intermediateMask, PARTIAL, sampleWeight, confidence);
            plan.setRoot(new SinkNode(idAllocator.getNextId(), partialAggregation, partialAggregation.getOutputSymbols()));

            // create final aggregation plan
            ExchangeNode source = new ExchangeNode(idAllocator.getNextId(), plan.getId(), plan.getRoot().getOutputSymbols());
            AggregationNode finalAggregation = new AggregationNode(idAllocator.getNextId(), source, groupBy, finalCalls, functions, ImmutableMap.<Symbol, Symbol>of(), FINAL, Optional.<Symbol>absent(), confidence);

            if (groupBy.isEmpty()) {
                plan = createSingleNodePlan(finalAggregation)
                        .addChild(plan.build());
            }
            else {
                plan.setHashOutputPartitioning(groupBy);
                plan = createFixedDistributionPlan(finalAggregation)
                        .addChild(plan.build());
            }
            return plan;
        }

        @Override
        public SubPlanBuilder visitWindow(WindowNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isDistributed()) {
                List<Symbol> partitionedBy = node.getPartitionBy();
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                ExchangeNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                if (partitionedBy.isEmpty()) {
                    // create a new non-partitioned fragment
                    current = createSingleNodePlan(source)
                            .addChild(current.build());
                }
                else {
                    current.setHashOutputPartitioning(partitionedBy);
                    current = createFixedDistributionPlan(source)
                            .addChild(current.build());
                }
            }

            current.setRoot(new WindowNode(node.getId(), current.getRoot(), node.getPartitionBy(), node.getOrderBy(), node.getOrderings(), node.getWindowFunctions(), node.getSignatures()));

            return current;
        }

        @Override
        public SubPlanBuilder visitFilter(FilterNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new FilterNode(node.getId(), current.getRoot(), node.getPredicate()));
            return current;
        }

        @Override
        public SubPlanBuilder visitSample(SampleNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new SampleNode(node.getId(), current.getRoot(), node.getSampleRatio(), node.getSampleType(), node.isRescaled(), node.getSampleWeightSymbol()));
            return current;
        }

        @Override
        public SubPlanBuilder visitMaterializeSample(MaterializeSampleNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new MaterializeSampleNode(node.getId(), current.getRoot(), node.getSampleWeightSymbol()));
            return current;
        }

        @Override
        public SubPlanBuilder visitProject(ProjectNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new ProjectNode(node.getId(), current.getRoot(), node.getOutputMap()));
            return current;
        }

        @Override
        public SubPlanBuilder visitTopN(TopNNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            current.setRoot(new TopNNode(node.getId(), current.getRoot(), node.getCount(), node.getOrderBy(), node.getOrderings(), false, node.getSampleWeight()));

            if (current.isDistributed()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                // create merge plan fragment
                PlanNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                TopNNode merge = new TopNNode(idAllocator.getNextId(), source, node.getCount(), node.getOrderBy(), node.getOrderings(), true, node.getSampleWeight());
                current = createSingleNodePlan(merge)
                        .addChild(current.build());
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitSort(SortNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isDistributed()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                // create a new non-partitioned fragment
                current = createSingleNodePlan(new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols()))
                        .addChild(current.build());
            }

            current.setRoot(new SortNode(node.getId(), current.getRoot(), node.getOrderBy(), node.getOrderings()));

            return current;
        }

        @Override
        public SubPlanBuilder visitOutput(OutputNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isDistributed()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                // create a new non-partitioned fragment
                current = createSingleNodePlan(new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols()))
                        .addChild(current.build());
            }

            current.setRoot(new OutputNode(node.getId(), current.getRoot(), node.getColumnNames(), node.getOutputSymbols()));

            return current;
        }

        @Override
        public SubPlanBuilder visitLimit(LimitNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            current.setRoot(new LimitNode(node.getId(), current.getRoot(), node.getCount(), node.getSampleWeight()));

            if (current.isDistributed()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                // create merge plan fragment
                PlanNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                LimitNode merge = new LimitNode(idAllocator.getNextId(), source, node.getCount(), node.getSampleWeight());
                current = createSingleNodePlan(merge)
                        .addChild(current.build());
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);

            current.setRoot(new DistinctLimitNode(node.getId(), current.getRoot(), node.getLimit()));

            if (current.isDistributed()) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                PlanNode source = new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols());
                DistinctLimitNode merge = new DistinctLimitNode(idAllocator.getNextId(), source, node.getLimit());
                current = createSingleNodePlan(merge).addChild(current.build());
            }
            return current;
        }

        @Override
        public SubPlanBuilder visitTableScan(TableScanNode node, Void context)
        {
            return createSourceDistributionPlan(node, node.getId());
        }

        @Override
        public SubPlanBuilder visitValues(ValuesNode node, Void context)
        {
            return createSingleNodePlan(node);
        }

        @Override
        public SubPlanBuilder visitTableWriter(TableWriterNode node, Void context)
        {
            // TODO: create table in pre-execution step, not here
            // Part of the plan should be an Optional<StateChangeListener<QueryState>> and this
            // callback can create the table and abort the table creation if the query fails.
            OutputTableHandle target = metadata.beginCreateTable(node.getCatalog(), node.getTableMetadata());

            SubPlanBuilder current = node.getSource().accept(this, context);
            current.setRoot(new TableWriterNode(node.getId(), current.getRoot(), target, node.getColumns(), node.getColumnNames(), node.getOutputSymbols(), node.getSampleWeightSymbol()));
            return current;
        }

        @Override
        public SubPlanBuilder visitTableCommit(TableCommitNode node, Void context)
        {
            SubPlanBuilder current = node.getSource().accept(this, context);
            checkState(current.getRoot() instanceof TableWriterNode, "table commit node must be preceeded by table writer node");
            OutputTableHandle target = ((TableWriterNode) current.getRoot()).getTarget();
            checkNotNull(target, "target table handle should have been constructed when we visited the table writer node");

            if (current.getDistribution() != PlanDistribution.COORDINATOR_ONLY && !createSingleNodePlan) {
                current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), current.getRoot().getOutputSymbols()));

                // create a new non-partitioned fragment to run on the coordinator
                current = createCoordinatorOnlyPlan(new ExchangeNode(idAllocator.getNextId(), current.getId(), current.getRoot().getOutputSymbols()))
                        .addChild(current.build());
            }

            current.setRoot(new TableCommitNode(node.getId(), current.getRoot(), target, node.getOutputSymbols()));

            return current;
        }

        @Override
        public SubPlanBuilder visitJoin(JoinNode node, Void context)
        {
            SubPlanBuilder left = node.getLeft().accept(this, context);
            SubPlanBuilder right = node.getRight().accept(this, context);

            if (left.isDistributed() || right.isDistributed()) {
                switch (node.getType()) {
                    case INNER:
                    case LEFT:
                        right.setRoot(new SinkNode(idAllocator.getNextId(), right.getRoot(), right.getRoot().getOutputSymbols()));
                        left.setRoot(new JoinNode(node.getId(),
                                node.getType(),
                                left.getRoot(),
                                new ExchangeNode(idAllocator.getNextId(), right.getId(), right.getRoot().getOutputSymbols()),
                                node.getCriteria()));
                        left.addChild(right.build());

                        return left;
                    case RIGHT:
                        left.setRoot(new SinkNode(idAllocator.getNextId(), left.getRoot(), left.getRoot().getOutputSymbols()));
                        right.setRoot(new JoinNode(node.getId(),
                                node.getType(),
                                new ExchangeNode(idAllocator.getNextId(), left.getId(), left.getRoot().getOutputSymbols()),
                                right.getRoot(),
                                node.getCriteria()));
                        right.addChild(left.build());

                        return right;
                    default:
                        throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
                }
            }
            else {
                JoinNode join = new JoinNode(node.getId(), node.getType(), left.getRoot(), right.getRoot(), node.getCriteria());
                return createSingleNodePlan(join)
                        .setChildren(Iterables.concat(left.getChildren(), right.getChildren()));
            }
        }

        @Override
        public SubPlanBuilder visitSemiJoin(SemiJoinNode node, Void context)
        {
            SubPlanBuilder source = node.getSource().accept(this, context);
            SubPlanBuilder filteringSource = node.getFilteringSource().accept(this, context);

            if (source.isDistributed() || filteringSource.isDistributed()) {
                filteringSource.setRoot(new SinkNode(idAllocator.getNextId(), filteringSource.getRoot(), filteringSource.getRoot().getOutputSymbols()));
                source.setRoot(new SemiJoinNode(node.getId(),
                        source.getRoot(),
                        new ExchangeNode(idAllocator.getNextId(), filteringSource.getId(), filteringSource.getRoot().getOutputSymbols()),
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput()));
                source.addChild(filteringSource.build());

                return source;
            }
            else {
                SemiJoinNode semiJoinNode = new SemiJoinNode(node.getId(), source.getRoot(), filteringSource.getRoot(), node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
                return createSingleNodePlan(semiJoinNode)
                        .setChildren(Iterables.concat(source.getChildren(), filteringSource.getChildren()));
            }
        }

        @Override
        public SubPlanBuilder visitIndexJoin(IndexJoinNode node, Void context)
        {
            SubPlanBuilder current = node.getProbeSource().accept(this, context);
            current.setRoot(new IndexJoinNode(node.getId(), node.getType(), current.getRoot(), node.getIndexSource(), node.getCriteria()));
            return current;
        }

        @Override
        public SubPlanBuilder visitUnion(UnionNode node, Void context)
        {
            if (createSingleNodePlan) {
                ImmutableList.Builder<PlanNode> sourceBuilder = ImmutableList.builder();
                for (PlanNode source : node.getSources()) {
                    sourceBuilder.add(source.accept(this, context).getRoot());
                }
                UnionNode unionNode = new UnionNode(node.getId(), sourceBuilder.build(), node.getSymbolMapping());
                return createSingleNodePlan(unionNode);
            }
            else {
                ImmutableList.Builder<SubPlan> sourceBuilder = ImmutableList.builder();
                ImmutableList.Builder<PlanFragmentId> fragmentIdBuilder = ImmutableList.builder();
                for (int i = 0; i < node.getSources().size(); i++) {
                    PlanNode subPlan = node.getSources().get(i);
                    SubPlanBuilder current = subPlan.accept(this, context);
                    current.setRoot(new SinkNode(idAllocator.getNextId(), current.getRoot(), node.sourceOutputLayout(i)));
                    fragmentIdBuilder.add(current.getId());
                    sourceBuilder.add(current.build());
                }
                ExchangeNode exchangeNode = new ExchangeNode(idAllocator.getNextId(), fragmentIdBuilder.build(), node.getOutputSymbols());
                return createSingleNodePlan(exchangeNode)
                        .setChildren(sourceBuilder.build());
            }
        }

        @Override
        protected SubPlanBuilder visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        public SubPlanBuilder createSingleNodePlan(PlanNode root)
        {
            return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, PlanDistribution.NONE, root, null);
        }

        public SubPlanBuilder createFixedDistributionPlan(PlanNode root)
        {
            return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, PlanDistribution.FIXED, root, null);
        }

        public SubPlanBuilder createSourceDistributionPlan(PlanNode root, PlanNodeId partitionedSourceId)
        {
            if (createSingleNodePlan) {
                // when creating a single node plan, we tell the planner that the table is not partitioned,
                // but we still need to set the source id for the execution engine
                return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, PlanDistribution.NONE, root, partitionedSourceId);
            }
            else {
                return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, PlanDistribution.SOURCE, root, partitionedSourceId);
            }
        }

        public SubPlanBuilder createCoordinatorOnlyPlan(PlanNode root)
        {
            return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, PlanDistribution.COORDINATOR_ONLY, root, null);
        }

        private SubPlanBuilder createSubPlan(PlanNode root, PlanDistribution distribution, PlanNodeId partitionedSourceId)
        {
            return new SubPlanBuilder(new PlanFragmentId(nextSubPlanId()), allocator, distribution, root, partitionedSourceId);
        }

        private String nextSubPlanId()
        {
            return String.valueOf(nextFragmentId++);
        }
    }
}
