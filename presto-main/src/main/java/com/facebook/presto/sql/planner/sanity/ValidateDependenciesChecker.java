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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer.IndexKeyTracer;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Ensures that all dependencies (i.e., symbols in expressions) for a plan node are provided by its source nodes
 */
public final class ValidateDependenciesChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, Map<Symbol, Type> types)
    {
        validate(plan);
    }

    public static void validate(PlanNode plan)
    {
        plan.accept(new Visitor(), ImmutableSet.of());
    }

    private static class Visitor
            extends PlanVisitor<Set<Symbol>, Void>
    {
        private final Map<PlanNodeId, PlanNode> nodesById = new HashMap<>();

        @Override
        protected Void visitPlan(PlanNode node, Set<Symbol> boundSymbols)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            checkDependencies(inputs, node.getGroupingKeys(), "Invalid node. Grouping key symbols (%s) not in source plan output (%s)", node.getGroupingKeys(), node.getSource().getOutputSymbols());

            for (FunctionCall call : node.getAggregations().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(call);
                checkDependencies(inputs, dependencies, "Invalid node. Aggregation dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            checkDependencies(source.getOutputSymbols(), node.getInputSymbols(), "Invalid node. Grouping symbols (%s) not in source plan output (%s)", node.getInputSymbols(), source.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            checkDependencies(source.getOutputSymbols(), node.getDistinctSymbols(), "Invalid node. Mark distinct symbols (%s) not in source plan output (%s)", node.getDistinctSymbols(), source.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);

            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputSymbols());
            checkDependencies(inputs, node.getOrderBy(), "Invalid node. Order by symbols (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            ImmutableList.Builder<Symbol> bounds = ImmutableList.builder();
            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getStartValue().isPresent()) {
                    bounds.add(frame.getStartValue().get());
                }
                if (frame.getEndValue().isPresent()) {
                    bounds.add(frame.getEndValue().get());
                }
            }
            checkDependencies(inputs, bounds.build(), "Invalid node. Frame bounds (%s) not in source plan output (%s)", bounds.build(), node.getSource().getOutputSymbols());

            for (WindowNode.Function function : node.getWindowFunctions().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(function.getFunctionCall());
                checkDependencies(inputs, dependencies, "Invalid node. Window function dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputSymbols());
            checkDependencies(inputs, node.getOrderBy(), "Invalid node. Order by symbols (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            checkDependencies(source.getOutputSymbols(), node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            checkDependencies(inputs, node.getOutputSymbols(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());

            Set<Symbol> dependencies = DependencyExtractor.extractUnique(node.getPredicate());
            checkDependencies(inputs, dependencies, "Invalid node. Predicate dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitSample(SampleNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            for (Expression expression : node.getAssignments().getExpressions()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(expression);
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            checkDependencies(inputs, node.getOutputSymbols(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            checkDependencies(inputs, node.getOrderBy(),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderBy(),
                    node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitSort(SortNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            Set<Symbol> inputs = createInputs(source, boundSymbols);
            checkDependencies(inputs, node.getOutputSymbols(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            checkDependencies(inputs, node.getOrderBy(), "Invalid node. Order by dependencies (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitOutput(OutputNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            checkDependencies(source.getOutputSymbols(), node.getOutputSymbols(), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputSymbols(), source.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Set<Symbol> boundSymbols)
        {
            node.getLeft().accept(this, boundSymbols);
            node.getRight().accept(this, boundSymbols);

            verifyUniqueId(node);

            Set<Symbol> leftInputs = createInputs(node.getLeft(), boundSymbols);
            Set<Symbol> rightInputs = createInputs(node.getRight(), boundSymbols);
            Set<Symbol> allInputs = ImmutableSet.<Symbol>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(leftInputs.contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputSymbols());
                checkArgument(rightInputs.contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputSymbols());
            }

            node.getFilter().ifPresent(predicate -> {
                Set<Symbol> predicateSymbols = DependencyExtractor.extractUnique(predicate);
                checkArgument(
                        allInputs.containsAll(predicateSymbols),
                        "Symbol from filter (%s) not in sources (%s)",
                        predicateSymbols,
                        allInputs);
            });

            checkDependencies(allInputs, node.getOutputSymbols(), "Symbol from join output (%s) not in sources (%s)", node.getOutputSymbols(), allInputs);

            List<Integer> rightSymbolIndexes = node.getRight().getOutputSymbols().stream()
                    .filter(symbol -> node.getOutputSymbols().contains(symbol))
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol))
                    .collect(toImmutableList());

            boolean allLeftBeforeRight = node.getLeft().getOutputSymbols().stream()
                    .filter(symbol -> node.getOutputSymbols().contains(symbol))
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol))
                    .allMatch(leftIndex -> rightSymbolIndexes.stream()
                            .allMatch(rightIndex -> rightIndex > leftIndex));
            checkState(allLeftBeforeRight, "Not all left output symbols are before right output symbols");

            if (node.isCrossJoin()) {
                List<Symbol> allInputsOrdered = ImmutableList.<Symbol>builder()
                        .addAll(node.getLeft().getOutputSymbols())
                        .addAll(node.getRight().getOutputSymbols())
                        .build();
                checkState(allInputsOrdered.equals(node.getOutputSymbols()), "Outputs symbols (%s) for cross join do not match ordered input symbols (%s)", node.getOutputSymbols(), allInputsOrdered);
            }

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Set<Symbol> boundSymbols)
        {
            node.getSource().accept(this, boundSymbols);
            node.getFilteringSource().accept(this, boundSymbols);

            verifyUniqueId(node);

            checkArgument(node.getSource().getOutputSymbols().contains(node.getSourceJoinSymbol()), "Symbol from semi join clause (%s) not in source (%s)", node.getSourceJoinSymbol(), node.getSource().getOutputSymbols());
            checkArgument(node.getFilteringSource().getOutputSymbols().contains(node.getFilteringSourceJoinSymbol()), "Symbol from semi join clause (%s) not in filtering source (%s)", node.getSourceJoinSymbol(), node.getFilteringSource().getOutputSymbols());

            Set<Symbol> outputs = createInputs(node, boundSymbols);
            checkArgument(outputs.containsAll(node.getSource().getOutputSymbols()), "Semi join output symbols (%s) must contain all of the source symbols (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            checkArgument(outputs.contains(node.getSemiJoinOutput()),
                    "Semi join output symbols (%s) must contain join result (%s)",
                    node.getOutputSymbols(),
                    node.getSemiJoinOutput());

            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Set<Symbol> boundSymbols)
        {
            node.getProbeSource().accept(this, boundSymbols);
            node.getIndexSource().accept(this, boundSymbols);

            verifyUniqueId(node);

            Set<Symbol> probeInputs = createInputs(node.getProbeSource(), boundSymbols);
            Set<Symbol> indexSourceInputs = createInputs(node.getIndexSource(), boundSymbols);
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(probeInputs.contains(clause.getProbe()), "Probe symbol from index join clause (%s) not in probe source (%s)", clause.getProbe(), node.getProbeSource().getOutputSymbols());
                checkArgument(indexSourceInputs.contains(clause.getIndex()), "Index symbol from index join clause (%s) not in index source (%s)", clause.getIndex(), node.getIndexSource().getOutputSymbols());
            }

            Set<Symbol> lookupSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());
            Map<Symbol, Symbol> trace = IndexKeyTracer.trace(node.getIndexSource(), lookupSymbols);
            checkArgument(!trace.isEmpty() && lookupSymbols.containsAll(trace.keySet()),
                    "Index lookup symbols are not traceable to index source: %s",
                    lookupSymbols);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Set<Symbol> boundSymbols)
        {
            verifyUniqueId(node);

            checkDependencies(node.getOutputSymbols(), node.getLookupSymbols(), "Lookup symbols must be part of output symbols");
            checkDependencies(node.getAssignments().keySet(), node.getOutputSymbols(), "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Set<Symbol> boundSymbols)
        {
            verifyUniqueId(node);

            checkArgument(node.getAssignments().keySet().containsAll(node.getOutputSymbols()), "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Set<Symbol> boundSymbols)
        {
            verifyUniqueId(node);
            return null;
        }

        @Override
        public Void visitUnnest(UnnestNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols);

            verifyUniqueId(node);

            Set<Symbol> required = ImmutableSet.<Symbol>builder()
                    .addAll(node.getReplicateSymbols())
                    .addAll(node.getUnnestSymbols().keySet())
                    .build();

            checkDependencies(source.getOutputSymbols(), required, "Invalid node. Dependencies (%s) not in source plan output (%s)", required, source.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Set<Symbol> boundSymbols)
        {
            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Set<Symbol> boundSymbols)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(subplan.getOutputSymbols(), node.getInputs().get(i), "EXCHANGE subplan must provide all of the necessary symbols");
                subplan.accept(this, boundSymbols); // visit child
            }

            checkDependencies(node.getOutputSymbols(), node.getPartitioningScheme().getOutputLayout(), "EXCHANGE must provide all of the necessary symbols for partition function");

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitDelete(DeleteNode node, Set<Symbol> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            checkArgument(source.getOutputSymbols().contains(node.getRowId()), "Invalid node. Row ID symbol (%s) is not in source plan output (%s)", node.getRowId(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Set<Symbol> boundSymbols)
        {
            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Set<Symbol> boundSymbols)
        {
            node.getSource().accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Set<Symbol> boundSymbols)
        {
            return visitSetOperation(node, boundSymbols);
        }

        private Void visitSetOperation(SetOperationNode node, Set<Symbol> boundSymbols)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(subplan.getOutputSymbols(), node.sourceOutputLayout(i), "%s subplan must provide all of the necessary symbols", node.getClass().getSimpleName());
                subplan.accept(this, boundSymbols); // visit child
            }

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitIntersect(IntersectNode node, Set<Symbol> boundSymbols)
        {
            return visitSetOperation(node, boundSymbols);
        }

        @Override
        public Void visitExcept(ExceptNode node, Set<Symbol> boundSymbols)
        {
            return visitSetOperation(node, boundSymbols);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Set<Symbol> boundSymbols)
        {
            node.getSource().accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Set<Symbol> boundSymbols)
        {
            node.getSource().accept(this, boundSymbols); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Set<Symbol> boundSymbols)
        {
            Set<Symbol> subqueryCorrelation = ImmutableSet.<Symbol>builder()
                    .addAll(boundSymbols)
                    .addAll(node.getCorrelation())
                    .build();

            node.getInput().accept(this, boundSymbols); // visit child
            node.getSubquery().accept(this, subqueryCorrelation); // visit child

            checkDependencies(node.getInput().getOutputSymbols(), node.getCorrelation(), "APPLY input must provide all the necessary correlation symbols for subquery");
            checkDependencies(DependencyExtractor.extractUnique(node.getSubquery()), node.getCorrelation(), "not all APPLY correlation symbols are used in subquery");

            ImmutableSet<Symbol> inputs = ImmutableSet.<Symbol>builder()
                    .addAll(createInputs(node.getSubquery(), boundSymbols))
                    .addAll(createInputs(node.getInput(), boundSymbols))
                    .build();

            for (Expression expression : node.getSubqueryAssignments().getExpressions()) {
                Set<Symbol> dependencies = DependencyExtractor.extractUnique(expression);
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            verifyUniqueId(node);

            return null;
        }

        private void verifyUniqueId(PlanNode node)
        {
            PlanNodeId id = node.getId();
            checkArgument(!nodesById.containsKey(id), "Duplicate node id found %s between %s and %s", node.getId(), node, nodesById.get(id));

            nodesById.put(id, node);
        }

        private static ImmutableSet<Symbol> createInputs(PlanNode source, Set<Symbol> boundSymbols)
        {
            return ImmutableSet.<Symbol>builder()
                    .addAll(source.getOutputSymbols())
                    .addAll(boundSymbols)
                    .build();
        }
    }

    private static void checkDependencies(Collection<Symbol> inputs, Collection<Symbol> required, String message, Object... parameters)
    {
        checkArgument(ImmutableSet.copyOf(inputs).containsAll(required), message, parameters);
    }
}
