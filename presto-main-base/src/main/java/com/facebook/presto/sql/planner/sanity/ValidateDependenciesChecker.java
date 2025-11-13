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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.spi.plan.StatisticAggregationsDescriptor.ColumnStatisticsDescriptor;
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
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.optimizations.WindowNodeUtil;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.plan.JoinNode.checkLeftOutputVariablesBeforeRight;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer.IndexKeyTracer;
import static com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Ensures that all dependencies (i.e., symbols in expressions) for a plan node are provided by its source nodes
 */
public final class ValidateDependenciesChecker
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        validate(plan);
    }

    public static void validate(PlanNode plan)
    {
        plan.accept(new Visitor(), ImmutableSet.of());
    }

    private static class Visitor
            extends InternalPlanVisitor<Void, Set<VariableReferenceExpression>>
    {
        public Visitor()
        {
        }

        @Override
        public Void visitPlan(PlanNode node, Set<VariableReferenceExpression> boundVariables)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Void visitTableFunction(TableFunctionNode node, Set<VariableReferenceExpression> boundSymbols)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode source = node.getSources().get(i);
                source.accept(this, boundSymbols);
                Set<VariableReferenceExpression> inputs = createInputs(source, boundSymbols);
                TableFunctionNode.TableArgumentProperties argumentProperties = node.getTableArgumentProperties().get(i);

                checkDependencies(
                        inputs,
                        argumentProperties.getRequiredColumns(),
                        "Invalid node. Required input symbols from source %s (%s) not in source plan output (%s)",
                        argumentProperties.getArgumentName(),
                        argumentProperties.getRequiredColumns(),
                        source.getOutputVariables());
                argumentProperties.getSpecification().ifPresent(specification -> {
                    checkDependencies(
                            inputs,
                            specification.getPartitionBy(),
                            "Invalid node. Partition by symbols for source %s (%s) not in source plan output (%s)",
                            argumentProperties.getArgumentName(),
                            specification.getPartitionBy(),
                            source.getOutputVariables());
                    specification.getOrderingScheme().ifPresent(orderingScheme -> {
                        checkDependencies(
                                inputs,
                                orderingScheme.getOrderByVariables(),
                                "Invalid node. Order by symbols for source %s (%s) not in source plan output (%s)",
                                argumentProperties.getArgumentName(),
                                orderingScheme.getOrderBy(),
                                source.getOutputVariables());
                    });
                });
                Set<VariableReferenceExpression> passThroughVariable = argumentProperties.getPassThroughSpecification().getColumns().stream()
                        .map(PassThroughColumn::getOutputVariables)
                        .collect(toImmutableSet());
                checkDependencies(
                        inputs,
                        passThroughVariable,
                        "Invalid node. Pass-through symbols for source %s (%s) not in source plan output (%s)",
                        argumentProperties.getArgumentName(),
                        passThroughVariable,
                        source.getOutputVariables());
            }
            return null;
        }

        @Override
        public Void visitTableFunctionProcessor(TableFunctionProcessorNode node, Set<VariableReferenceExpression> boundVariables)
        {
            if (!node.getSource().isPresent()) {
                return null;
            }

            PlanNode source = node.getSource().get();
            source.accept(this, boundVariables);

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);

            Set<VariableReferenceExpression> passThroughSymbols = node.getPassThroughSpecifications().stream()
                    .map(PassThroughSpecification::getColumns)
                    .flatMap(Collection::stream)
                    .map(PassThroughColumn::getOutputVariables)
                    .collect(toImmutableSet());
            checkDependencies(
                    inputs,
                    passThroughSymbols,
                    "Invalid node. Pass-through symbols (%s) not in source plan output (%s)",
                    passThroughSymbols,
                    source.getOutputVariables());

            Set<VariableReferenceExpression> requiredSymbols = node.getRequiredVariables().stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());
            checkDependencies(
                    inputs,
                    requiredSymbols,
                    "Invalid node. Required symbols (%s) not in source plan output (%s)",
                    requiredSymbols,
                    source.getOutputVariables());

            node.getMarkerVariables().ifPresent(mapping -> {
                checkDependencies(
                        inputs,
                        mapping.keySet(),
                        "Invalid node. Source symbols (%s) not in source plan output (%s)",
                        mapping.keySet(),
                        source.getOutputVariables());
                checkDependencies(
                        inputs,
                        mapping.values(),
                        "Invalid node. Source marker symbols (%s) not in source plan output (%s)",
                        mapping.values(),
                        source.getOutputVariables());
            });

            node.getSpecification().ifPresent(specification -> {
                checkDependencies(
                        inputs,
                        specification.getPartitionBy(),
                        "Invalid node. Partition by symbols (%s) not in source plan output (%s)",
                        specification.getPartitionBy(),
                        source.getOutputVariables());
                specification.getOrderingScheme().ifPresent(orderingScheme -> {
                    checkDependencies(
                            inputs,
                            orderingScheme.getOrderByVariables(),
                            "Invalid node. Order by symbols (%s) not in source plan output (%s)",
                            orderingScheme.getOrderBy(),
                            source.getOutputVariables());
                });
            });

            return null;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getGroupingKeys(), "Invalid node. Grouping key variables (%s) not in source plan output (%s)", node.getGroupingKeys(), node.getSource().getOutputVariables());

            for (Aggregation aggregation : node.getAggregations().values()) {
                Set<VariableReferenceExpression> dependencies = extractAggregationUniqueVariables(aggregation);
                checkDependencies(inputs, dependencies, "Invalid node. Aggregation dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputVariables());
                aggregation.getMask().ifPresent(mask -> {
                    checkDependencies(inputs, ImmutableSet.of(mask), "Invalid node. Aggregation mask symbol (%s) not in source plan output (%s)", mask, node.getSource().getOutputVariables());
                });
            }

            return null;
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getInputVariables(), "Invalid node. Grouping symbols (%s) not in source plan output (%s)", node.getInputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getDistinctVariables(), "Invalid node. Mark distinct symbols (%s) not in source plan output (%s)", node.getDistinctVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);

            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputVariables());
            if (node.getOrderingScheme().isPresent()) {
                checkDependencies(
                        inputs,
                        node.getOrderingScheme().get().getOrderByVariables(),
                        "Invalid node. Order by symbols (%s) not in source plan output (%s)",
                        node.getOrderingScheme().get().getOrderByVariables(), node.getSource().getOutputVariables());
            }

            ImmutableList.Builder<VariableReferenceExpression> bounds = ImmutableList.builder();
            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getStartValue().isPresent()) {
                    bounds.add(frame.getStartValue().get());
                }
                if (frame.getEndValue().isPresent()) {
                    bounds.add(frame.getEndValue().get());
                }
            }
            checkDependencies(inputs, bounds.build(), "Invalid node. Frame bounds (%s) not in source plan output (%s)", bounds.build(), node.getSource().getOutputVariables());

            ImmutableList.Builder<VariableReferenceExpression> symbolsForFrameBoundsComparison = ImmutableList.builder();
            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getSortKeyCoercedForFrameStartComparison().isPresent()) {
                    symbolsForFrameBoundsComparison.add(frame.getSortKeyCoercedForFrameStartComparison().get());
                }
                if (frame.getSortKeyCoercedForFrameEndComparison().isPresent()) {
                    symbolsForFrameBoundsComparison.add(frame.getSortKeyCoercedForFrameEndComparison().get());
                }
            }
            checkDependencies(inputs, symbolsForFrameBoundsComparison.build(), "Invalid node. Symbols for frame bound comparison (%s) not in source plan output (%s)", symbolsForFrameBoundsComparison.build(), node.getSource().getOutputVariables());

            for (WindowNode.Function function : node.getWindowFunctions().values()) {
                Set<VariableReferenceExpression> dependencies = WindowNodeUtil.extractWindowFunctionUniqueVariables(function);
                checkDependencies(inputs, dependencies, "Invalid node. Window function dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputVariables());
            }

            return null;
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by symbols (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());

            Set<String> dependencies = VariablesExtractor.extractUnique(node.getPredicate()).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
            checkArgument(
                    inputs.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()).containsAll(dependencies),
                    "Symbol from filter (%s) not in sources (%s)",
                    dependencies,
                    inputs);

            return null;
        }

        @Override
        public Void visitSample(SampleNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            for (RowExpression expression : node.getAssignments().getExpressions()) {
                Set<VariableReferenceExpression> dependencies = VariablesExtractor.extractUnique(expression);
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(),
                    node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitSort(SortNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitOutput(OutputNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getOutputVariables(), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitOffset(OffsetNode node, Set<VariableReferenceExpression> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getOutputVariables(), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitMaterializedViewScan(MaterializedViewScanNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode dataTablePlan = node.getSources().get(0);
            PlanNode viewQueryPlan = node.getSources().get(1);

            dataTablePlan.accept(this, boundVariables);
            viewQueryPlan.accept(this, boundVariables);

            Set<VariableReferenceExpression> dataTableOutputs = ImmutableSet.copyOf(dataTablePlan.getOutputVariables());
            Set<VariableReferenceExpression> viewQueryOutputs = ImmutableSet.copyOf(viewQueryPlan.getOutputVariables());

            for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
                VariableReferenceExpression dataTableVariable = node.getDataTableMappings().get(outputVariable);
                checkArgument(
                        dataTableVariable != null,
                        "Output variable %s has no mapping in dataTableMappings",
                        outputVariable);
                checkArgument(
                        dataTableOutputs.contains(dataTableVariable),
                        "Data table mapping variable %s for output %s not in data table plan output (%s)",
                        dataTableVariable,
                        outputVariable,
                        dataTableOutputs);

                VariableReferenceExpression viewQueryVariable = node.getViewQueryMappings().get(outputVariable);
                checkArgument(
                        viewQueryVariable != null,
                        "Output variable %s has no mapping in viewQueryMappings",
                        outputVariable);
                checkArgument(
                        viewQueryOutputs.contains(viewQueryVariable),
                        "View query mapping variable %s for output %s not in view query plan output (%s)",
                        viewQueryVariable,
                        outputVariable,
                        viewQueryOutputs);
            }

            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            for (EquiJoinClause clause : node.getCriteria()) {
                checkArgument(leftInputs.contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputVariables());
                checkArgument(rightInputs.contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputVariables());
            }

            node.getFilter().ifPresent(predicate -> {
                // Only verify names here as filter expression would contain type cast, which will be translated to an non-existent variable in
                // SqlToRowExpressionTranslator
                // TODO https://github.com/prestodb/presto/issues/12892
                Set<String> predicateVariables = VariablesExtractor.extractUnique(predicate).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
                checkArgument(
                        allInputs.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()).containsAll(predicateVariables),
                        "Symbol from filter (%s) not in sources (%s)",
                        predicateVariables,
                        allInputs);
            });

            checkLeftOutputVariablesBeforeRight(node.getLeft().getOutputVariables(), node.getOutputVariables());
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables);
            node.getFilteringSource().accept(this, boundVariables);

            checkArgument(node.getSource().getOutputVariables().contains(node.getSourceJoinVariable()), "Symbol from semi join clause (%s) not in source (%s)", node.getSourceJoinVariable(), node.getSource().getOutputVariables());
            checkArgument(node.getFilteringSource().getOutputVariables().contains(node.getFilteringSourceJoinVariable()), "Symbol from semi join clause (%s) not in filtering source (%s)", node.getSourceJoinVariable(), node.getFilteringSource().getOutputVariables());

            Set<VariableReferenceExpression> outputs = createInputs(node, boundVariables);
            checkArgument(outputs.containsAll(node.getSource().getOutputVariables()), "Semi join output symbols (%s) must contain all of the source symbols (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkArgument(outputs.contains(node.getSemiJoinOutput()),
                    "Semi join output symbols (%s) must contain join result (%s)",
                    node.getOutputVariables(),
                    node.getSemiJoinOutput());

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            Set<VariableReferenceExpression> predicateVariables = VariablesExtractor.extractUnique(node.getFilter());

            checkArgument(
                    allInputs.containsAll(predicateVariables),
                    "Symbol from filter (%s) not in sources (%s)",
                    predicateVariables,
                    allInputs);

            checkLeftOutputVariablesBeforeRight(node.getLeft().getOutputVariables(), node.getOutputVariables());
            return null;
        }

        @Override
        public Void visitMergeJoin(MergeJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            for (EquiJoinClause clause : node.getCriteria()) {
                checkArgument(leftInputs.contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputVariables());
                checkArgument(rightInputs.contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputVariables());
            }

            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getProbeSource().accept(this, boundVariables);
            node.getIndexSource().accept(this, boundVariables);

            Set<VariableReferenceExpression> probeInputs = createInputs(node.getProbeSource(), boundVariables);
            Set<VariableReferenceExpression> indexSourceInputs = createInputs(node.getIndexSource(), boundVariables);
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(probeInputs.contains(clause.getProbe()), "Probe variable from index join clause (%s) not in probe source (%s)", clause.getProbe(), node.getProbeSource().getOutputVariables());
                checkArgument(indexSourceInputs.contains(clause.getIndex()), "Index variable from index join clause (%s) not in index source (%s)", clause.getIndex(), node.getIndexSource().getOutputVariables());
            }

            Set<VariableReferenceExpression> lookupVariables = ImmutableSet.copyOf(node.getLookupVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(node.getIndexSource(), lookupVariables);
            checkArgument(!trace.isEmpty() && lookupVariables.containsAll(trace.keySet()),
                    "Index lookup symbols are not traceable to index source: %s",
                    lookupVariables);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            checkDependencies(
                    node.getOutputVariables(),
                    node.getLookupVariables(),
                    "Lookup variables must be part of output symbols");
            checkDependencies(
                    node.getAssignments().keySet(),
                    node.getOutputVariables(),
                    "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Set<VariableReferenceExpression> boundVariables)
        {
            //We don't have to do a check here as TableScanNode has no dependencies.
            return null;
        }

        @Override
        public Void visitCteReference(CteReferenceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables);
            return null;
        }

        public Void visitCteProducer(CteProducerNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables);
            checkDependencies(source.getOutputVariables(), node.getOutputVariables(),
                    "Invalid node. Output column dependencies (%s) not in source plan output (%s)",
                    node.getOutputVariables(), source.getOutputVariables());

            return null;
        }

        public Void visitCteConsumer(CteConsumerNode node, Set<VariableReferenceExpression> boundVariables)
        {
            //We don't have to do a check here as CteConsumerNode has no dependencies.
            return null;
        }

        public Void visitSequence(SequenceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSources().forEach(plan -> plan.accept(this, boundVariables));
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> correlatedDependencies = VariablesExtractor.extractUnique(node);
            checkDependencies(
                    boundVariables,
                    correlatedDependencies,
                    "Invalid node. Expression correlated dependencies (%s) not satisfied by (%s)",
                    correlatedDependencies,
                    boundVariables);
            return null;
        }

        @Override
        public Void visitUnnest(UnnestNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables);

            Set<VariableReferenceExpression> required = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getReplicateVariables())
                    .addAll(node.getUnnestVariables().keySet())
                    .build();

            checkDependencies(source.getOutputVariables(), required, "Invalid node. Dependencies (%s) not in source plan output (%s)", required, source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(
                        subplan.getOutputVariables(),
                        node.getInputs().get(i),
                        "EXCHANGE subplan must provide all of the necessary symbols");
                subplan.accept(this, boundVariables); // visit child
            }

            checkDependencies(
                    node.getOutputVariables(),
                    node.getPartitioningScheme().getOutputLayout(),
                    "EXCHANGE must provide all of the necessary symbols for partition function");

            return null;
        }

        @Override
        public Void visitCallDistributedProcedure(CallDistributedProcedureNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitTableWriteMerge(TableWriterMergeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitMergeWriter(MergeWriterNode node, Set<VariableReferenceExpression> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child
            return null;
        }

        @Override
        public Void visitMergeProcessor(MergeProcessorNode node, Set<VariableReferenceExpression> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            checkArgument(source.getOutputVariables().contains(node.getTargetTableRowIdColumnVariable()),
                    "Invalid node. rowId symbol (%s) is not in source plan output (%s)",
                    node.getTargetTableRowIdColumnVariable(), node.getSource().getOutputVariables());
            checkArgument(source.getOutputVariables().contains(node.getMergeRowVariable()),
                    "Invalid node. Merge row symbol (%s) is not in source plan output (%s)",
                    node.getMergeRowVariable(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitDelete(DeleteNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            node.getRowId().ifPresent(rowid ->
                    checkArgument(source.getOutputVariables().contains(rowid),
                            "Invalid node. Row ID symbol (%s) is not in source plan output (%s)", rowid, node.getSource().getOutputVariables()));

            return null;
        }

        @Override
        public Void visitUpdate(UpdateNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child
            node.getRowId().ifPresent(r ->
                    checkArgument(source.getOutputVariables().contains(r), "Invalid node. Row ID symbol (%s) is not in source plan output (%s)", node.getRowId(), node.getSource().getOutputVariables()));
            checkArgument(source.getOutputVariables().containsAll(node.getColumnValueAndRowIdSymbols()), "Invalid node. Some UPDATE SET expression symbols (%s) are not contained in the outputSymbols (%s)", node.getColumnValueAndRowIdSymbols(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return null;
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor = node.getDescriptor();
            Set<VariableReferenceExpression> dependencies = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(descriptor.getGrouping().values())
                    .addAll(descriptor.getColumnStatistics().stream().map(ColumnStatisticsDescriptor::getItem).iterator())
                    .addAll(descriptor.getTableStatistics().values())
                    .build();
            List<VariableReferenceExpression> outputVariables = node.getSource().getOutputVariables();
            checkDependencies(dependencies, dependencies, "Invalid node. Dependencies (%s) not in source plan output (%s)", dependencies, outputVariables);
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        private Void visitSetOperation(SetOperationNode node, Set<VariableReferenceExpression> boundVariables)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(subplan.getOutputVariables(), node.sourceOutputLayout(i), "%s subplan must provide all of the necessary symbols", node.getClass().getSimpleName());
                subplan.accept(this, boundVariables); // visit child
            }

            return null;
        }

        @Override
        public Void visitIntersect(IntersectNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        @Override
        public Void visitExcept(ExceptNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> subqueryCorrelation = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(boundVariables)
                    .addAll(node.getCorrelation())
                    .build();

            node.getInput().accept(this, boundVariables); // visit child
            node.getSubquery().accept(this, subqueryCorrelation); // visit child

            checkDependencies(node.getInput().getOutputVariables(), node.getCorrelation(), "APPLY input must provide all the necessary correlation variables for subquery");
            checkDependencies(VariablesExtractor.extractUnique(node.getSubquery()), node.getCorrelation(), "not all APPLY correlation symbols are used in subquery");

            ImmutableSet<VariableReferenceExpression> inputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(createInputs(node.getSubquery(), boundVariables))
                    .addAll(createInputs(node.getInput(), boundVariables))
                    .build();

            for (RowExpression expression : node.getSubqueryAssignments().getExpressions()) {
                Set<VariableReferenceExpression> dependencies = VariablesExtractor.extractUnique(expression);
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            return null;
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> subqueryCorrelation = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(boundVariables)
                    .addAll(node.getCorrelation())
                    .build();

            node.getInput().accept(this, boundVariables); // visit child
            node.getSubquery().accept(this, subqueryCorrelation); // visit child

            checkDependencies(
                    node.getInput().getOutputVariables(),
                    node.getCorrelation(),
                    "LATERAL input must provide all the necessary correlation symbols for subquery");
            checkDependencies(
                    VariablesExtractor.extractUnique(node.getSubquery()),
                    node.getCorrelation(),
                    "not all LATERAL correlation symbols are used in subquery");

            return null;
        }

        private static ImmutableSet<VariableReferenceExpression> createInputs(PlanNode source, Set<VariableReferenceExpression> boundVariables)
        {
            return ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(source.getOutputVariables())
                    .addAll(boundVariables)
                    .build();
        }

        private void checkDependencies(Collection<VariableReferenceExpression> inputs, Collection<VariableReferenceExpression> required, String message, Object... parameters)
        {
            checkArgument(ImmutableSet.copyOf(inputs).containsAll(required), message, parameters);
        }
    }
}
