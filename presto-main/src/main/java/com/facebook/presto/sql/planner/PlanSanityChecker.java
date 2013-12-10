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

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer.IndexKeyTracer;

/**
 * Ensures that all dependencies (i.e., symbols in expressions) for a plan node are provided by its source nodes
 */
public final class PlanSanityChecker
{
    private PlanSanityChecker() {}

    public static void validate(PlanNode plan)
    {
        plan.accept(new Visitor(), null);
    }

    private static class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Map<PlanNodeId, PlanNode> nodesById = new HashMap<>();

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getGroupBy()), "Invalid node. Group by symbols (%s) not in source plan output (%s)", node.getGroupBy(), node.getSource().getOutputSymbols());

            if (node.getSampleWeight().isPresent()) {
                Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeight().get()), "Invalid node. Sample weight symbol (%s) is not in source plan output (%s)", node.getSampleWeight().get(), node.getSource().getOutputSymbols());
            }

            for (FunctionCall call : node.getAggregations().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(call);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Aggregation dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getDistinctSymbols()), "Invalid node. Mark distinct symbols (%s) not in source plan output (%s)", node.getDistinctSymbols(), source.getOutputSymbols());

            if (node.getSampleWeightSymbol().isPresent()) {
                Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeightSymbol().get()), "Invalid node. Sample weight symbol (%s) is not in source plan output (%s)", node.getSampleWeightSymbol().get(), node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getPartitionBy()), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()), "Invalid node. Order by symbols (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            for (FunctionCall call : node.getWindowFunctions().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(call);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Window function dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());

            Set<Symbol> dependencies = DependencyExtractor.extractUnique(node.getPredicate());

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Predicate dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            for (Expression expression : node.getExpressions()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(expression);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderBy(),
                    node.getSource().getOutputSymbols());

            if (node.getSampleWeight().isPresent()) {
                Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeight().get()), "Invalid node. Sample weight symbol (%s) is not in source plan output (%s)", node.getSampleWeight().get(), node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()), "Invalid node. Order by dependencies (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitMaterializeSample(MaterializeSampleNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context);
            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeightSymbol()), "Invalid node. Sample weight symbol (%s) not in source plan output (%s)", node.getSampleWeightSymbol(), source.getOutputSymbols());
            return null;
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            if (node.getSampleWeight().isPresent()) {
                Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeight().get()), "Invalid node. Sample weight symbol (%s) is not in source plan output (%s)", node.getSampleWeight().get(), node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            verifyUniqueId(node);

            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                Preconditions.checkArgument(node.getLeft().getOutputSymbols().contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputSymbols());
                Preconditions.checkArgument(node.getRight().getOutputSymbols().contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            verifyUniqueId(node);

            Preconditions.checkArgument(node.getSource().getOutputSymbols().contains(node.getSourceJoinSymbol()), "Symbol from semi join clause (%s) not in source (%s)", node.getSourceJoinSymbol(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(node.getFilteringSource().getOutputSymbols().contains(node.getFilteringSourceJoinSymbol()), "Symbol from semi join clause (%s) not in filtering source (%s)", node.getSourceJoinSymbol(), node.getFilteringSource().getOutputSymbols());
            Preconditions.checkArgument(node.getOutputSymbols().containsAll(node.getSource().getOutputSymbols()), "Semi join output symbols (%s) must contain all of the source symbols (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(node.getOutputSymbols().contains(node.getSemiJoinOutput()),
                    "Semi join output symbols (%s) must contain join result (%s)",
                    node.getOutputSymbols(),
                    node.getSemiJoinOutput());

            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            verifyUniqueId(node);

            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                Preconditions.checkArgument(node.getProbeSource().getOutputSymbols().contains(clause.getProbe()), "Probe symbol from index join clause (%s) not in probe source (%s)", clause.getProbe(), node.getProbeSource().getOutputSymbols());
                Preconditions.checkArgument(node.getIndexSource().getOutputSymbols().contains(clause.getIndex()), "Index symbol from index join clause (%s) not in index source (%s)", clause.getIndex(), node.getIndexSource().getOutputSymbols());
            }

            Set<Symbol> lookupSymbols = FluentIterable.from(node.getCriteria())
                    .transform(IndexJoinNode.EquiJoinClause.indexGetter())
                    .toSet();
            Preconditions.checkArgument(IndexKeyTracer.trace(node.getIndexSource(), lookupSymbols).keySet().containsAll(lookupSymbols),
                    "Index lookup symbols are not traceable to index source: %s",
                    lookupSymbols);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            verifyUniqueId(node);

            Preconditions.checkArgument(node.getOutputSymbols().containsAll(node.getLookupSymbols()), "Lookup symbols must be part of output symbols");
            Preconditions.checkArgument(node.getAssignments().keySet().containsAll(node.getOutputSymbols()), "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            verifyUniqueId(node);

            Preconditions.checkArgument(node.getAssignments().keySet().containsAll(node.getOutputSymbols()), "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            verifyUniqueId(node);

            Preconditions.checkArgument(node.getOutputSymbols().containsAll(node.getOutputSymbols()), "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitSink(SinkNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            if (node.getSampleWeightSymbol().isPresent()) {
                Preconditions.checkArgument(source.getOutputSymbols().contains(node.getSampleWeightSymbol().get()), "Invalid node. Sample weight symbol (%s) is not in source plan output (%s)", node.getSampleWeightSymbol().get(), node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitTableCommit(TableCommitNode node, Void context)
        {
            PlanNode source = node.getSource();
            Preconditions.checkArgument(source instanceof TableWriterNode, "Invalid node. TableCommit source must be a TableWriter not %s", source.getClass().getSimpleName());
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                Preconditions.checkArgument(subplan.getOutputSymbols().containsAll(node.sourceOutputLayout(i)), "UNION subplan must provide all of the necessary symbols");
                subplan.accept(this, context); // visit child
            }

            verifyUniqueId(node);

            return null;
        }

        private void verifyUniqueId(PlanNode node)
        {
            PlanNodeId id = node.getId();
            Preconditions.checkArgument(!nodesById.containsKey(id), "Duplicate node id found %s between %s and %s", node.getId(), node, nodesById.get(id));

            nodesById.put(id, node);
        }
    }
}
