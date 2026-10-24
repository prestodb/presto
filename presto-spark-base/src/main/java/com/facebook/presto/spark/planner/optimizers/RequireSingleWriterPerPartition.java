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
package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.TopNRowNumberNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkTableWriterPartitionCount;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * When spark_table_writer_partition_count is set, requires a single writer per partition for table writes
 * whose layout allows multiple writers per partition, for example a partitioned but not bucketed Hive table.
 * AddExchanges then shuffles the writer input on the table partitioning, as it does for bucketed tables.
 * Must run after PushTableWriteThroughUnion, which only pushes a writer into the branches of a union while
 * multiple writers per partition are allowed, and before AddExchanges.
 * <p>
 * A map task that is recomputed after some writers have read its output must send the same rows, with the same
 * values, to the same writers as its first attempt. So a write keeps its layout when the values of a partition column
 * are not known to be deterministic, when its input depends on the order of rows, as LIMIT, row numbers, window
 * functions and table functions do, or when it copies the value of an aggregate that is not known to be deterministic
 * and independent of the order of rows to several rows, or matches rows by it, through a join, a semi join, an unnest
 * or grouping sets. PrestoSparkQueryPlanner does not run this optimizer for a query that calls non-deterministic
 * functions or uses TABLESAMPLE, whose random choices can hide in table scans and join filters.
 * A write also keeps its layout when a partition column has a type that the Hive bucket function cannot hash,
 * because the shuffle would fail.
 */
public class RequireSingleWriterPerPartition
        implements PlanOptimizer
{
    // Built-in aggregates whose result is deterministic and does not depend on the order of the input rows
    private static final Set<String> ORDER_INDEPENDENT_AGGREGATES = ImmutableSet.of(
            "count",
            "count_if",
            "min",
            "max",
            "sum",
            "avg",
            "bool_and",
            "bool_or",
            "every",
            "checksum",
            "approx_distinct",
            "bitwise_and_agg",
            "bitwise_or_agg",
            "bitwise_xor_agg");
    // Aggregates whose floating point results can differ in the last bits between attempts, which add the values in another order
    private static final Set<String> FLOATING_POINT_SENSITIVE_AGGREGATES = ImmutableSet.of("sum", "avg");

    private final FunctionAndTypeManager functionAndTypeManager;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public RequireSingleWriterPerPartition(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, boolean forceEnableOptimizer)
    {
        if (!getSparkTableWriterPartitionCount(session).isPresent()) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        Rewriter rewriter = new Rewriter();
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    // The types that HiveBucketing hashes
    private static boolean isHashable(Type type)
    {
        return type.equals(BOOLEAN)
                || type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type instanceof VarcharType
                || type.equals(DATE)
                || type.equals(TIMESTAMP);
    }

    // Nodes whose output rows or values can depend on the order in which their input rows arrive
    private static boolean dependsOnRowOrder(PlanNode node)
    {
        return node instanceof LimitNode
                || node instanceof TopNNode
                || node instanceof DistinctLimitNode
                || node instanceof TopNRowNumberNode
                || node instanceof RowNumberNode
                || node instanceof WindowNode
                || node instanceof TableFunctionProcessorNode;
    }

    // Nodes that copy an input row to several output rows, or select rows by the values of other rows
    private static boolean copiesOrMatchesRows(PlanNode node)
    {
        return node instanceof JoinNode
                || node instanceof SemiJoinNode
                || node instanceof SpatialJoinNode
                || node instanceof IndexJoinNode
                || node instanceof UnnestNode
                || node instanceof GroupIdNode;
    }

    /**
     * Returns whether an aggregation below a node that copies or matches rows computes a value that is not known to be
     * the same in every attempt of a task, because it can depend on the order of the input rows or on random choices.
     * Rows on several writers could then carry the values of different attempts. Without such a node above it, an
     * aggregation yields one row per group, which one writer writes.
     */
    private boolean copiesOrderDependentAggregate(PlanNode node, boolean belowCopy)
    {
        if (belowCopy && node instanceof AggregationNode && !((AggregationNode) node).getAggregations().values().stream().allMatch(this::isOrderIndependent)) {
            return true;
        }
        boolean sourcesBelowCopy = belowCopy || copiesOrMatchesRows(node);
        return node.getSources().stream().anyMatch(source -> copiesOrderDependentAggregate(source, sourcesBelowCopy));
    }

    private boolean isOrderIndependent(AggregationNode.Aggregation aggregation)
    {
        FunctionMetadata function = functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle());
        CatalogSchemaName namespace = function.getName().getCatalogSchemaName();
        String name = function.getName().getObjectName();
        return (namespace.equals(JAVA_BUILTIN_NAMESPACE) || namespace.equals(functionAndTypeManager.getDefaultNamespace()))
                && ORDER_INDEPENDENT_AGGREGATES.contains(name)
                && !(FLOATING_POINT_SENSITIVE_AGGREGATES.contains(name)
                        && Stream.concat(Stream.of(function.getReturnType()), function.getArgumentTypes().stream()).anyMatch(RequireSingleWriterPerPartition::isFloatingPoint));
    }

    private static boolean isFloatingPoint(TypeSignature type)
    {
        return type.getBase().equals(StandardTypes.DOUBLE) || type.getBase().equals(StandardTypes.REAL);
    }

    /**
     * Returns whether the given output variables of the node are known to be deterministic: table columns, and values
     * that deterministic projections compute from them, passed through other nodes. A variable that another node
     * creates, for example an aggregate or window function result or a VALUES column, is not known to be deterministic.
     */
    private boolean isDeterministic(PlanNode node, Set<VariableReferenceExpression> variables)
    {
        if (variables.isEmpty() || node instanceof TableScanNode) {
            return true;
        }
        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            ImmutableSet.Builder<VariableReferenceExpression> sourceVariables = ImmutableSet.builder();
            for (VariableReferenceExpression variable : variables) {
                RowExpression expression = project.getAssignments().get(variable);
                if (!determinismEvaluator.isDeterministic(expression)) {
                    return false;
                }
                sourceVariables.addAll(extractUnique(expression));
            }
            return isDeterministic(project.getSource(), sourceVariables.build());
        }
        if (node instanceof UnionNode) {
            UnionNode union = (UnionNode) node;
            for (int source = 0; source < union.getSources().size(); source++) {
                int sourceIndex = source;
                Set<VariableReferenceExpression> sourceVariables = variables.stream()
                        .map(variable -> union.getVariableMapping().get(variable).get(sourceIndex))
                        .collect(toImmutableSet());
                if (!isDeterministic(union.getSources().get(source), sourceVariables)) {
                    return false;
                }
            }
            return true;
        }
        if (node instanceof UnnestNode) {
            UnnestNode unnest = (UnnestNode) node;
            ImmutableSet.Builder<VariableReferenceExpression> sourceVariables = ImmutableSet.builder();
            for (VariableReferenceExpression variable : variables) {
                if (unnest.getOrdinalityVariable().map(variable::equals).orElse(false)) {
                    sourceVariables.addAll(unnest.getUnnestVariables().keySet());
                    continue;
                }
                sourceVariables.add(unnest.getUnnestVariables().entrySet().stream()
                        .filter(entry -> entry.getValue().contains(variable))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElse(variable));
            }
            return isDeterministic(unnest.getSource(), sourceVariables.build());
        }
        if (node instanceof GroupIdNode) {
            GroupIdNode groupId = (GroupIdNode) node;
            // the group id is a constant for each grouping set
            Set<VariableReferenceExpression> sourceVariables = variables.stream()
                    .filter(variable -> !variable.equals(groupId.getGroupIdVariable()))
                    .map(variable -> groupId.getGroupingColumns().getOrDefault(variable, variable))
                    .collect(toImmutableSet());
            return isDeterministic(groupId.getSource(), sourceVariables);
        }
        List<PlanNode> sources = node.getSources();
        if (!variables.stream().allMatch(variable -> sources.stream().anyMatch(source -> source.getOutputVariables().contains(variable)))) {
            return false;
        }
        return sources.stream()
                .allMatch(source -> isDeterministic(
                        source,
                        variables.stream()
                                .filter(source.getOutputVariables()::contains)
                                .collect(toImmutableSet())));
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private boolean planChanged;

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            TableWriterNode rewrittenNode = (TableWriterNode) context.defaultRewrite(node);
            Optional<PartitioningScheme> partitioningScheme = rewrittenNode.getTablePartitioningScheme();
            if (!partitioningScheme.isPresent() || !partitioningScheme.get().isScaleWriters()) {
                return rewrittenNode;
            }
            Partitioning partitioning = partitioningScheme.get().getPartitioning();
            if (!partitioning.getArguments().stream().map(RowExpression::getType).allMatch(RequireSingleWriterPerPartition::isHashable)
                    || !isDeterministic(rewrittenNode.getSource(), partitioning.getVariableReferences())
                    || searchFrom(rewrittenNode.getSource()).where(RequireSingleWriterPerPartition::dependsOnRowOrder).matches()
                    || copiesOrderDependentAggregate(rewrittenNode.getSource(), false)) {
                return rewrittenNode;
            }
            planChanged = true;
            return new TableWriterNode(
                    rewrittenNode.getSourceLocation(),
                    rewrittenNode.getId(),
                    rewrittenNode.getStatsEquivalentPlanNode(),
                    rewrittenNode.getSource(),
                    rewrittenNode.getTarget(),
                    rewrittenNode.getRowCountVariable(),
                    rewrittenNode.getFragmentVariable(),
                    rewrittenNode.getTableCommitContextVariable(),
                    rewrittenNode.getColumns(),
                    rewrittenNode.getColumnNames(),
                    rewrittenNode.getNotNullColumnVariables(),
                    Optional.of(new PartitioningScheme(
                            partitioning,
                            partitioningScheme.get().getOutputLayout(),
                            partitioningScheme.get().getHashColumn(),
                            partitioningScheme.get().isReplicateNullsAndAny(),
                            false,
                            partitioningScheme.get().getEncoding(),
                            partitioningScheme.get().getBucketToPartition())),
                    rewrittenNode.getStatisticsAggregation(),
                    rewrittenNode.getTaskCountIfScaledWriter(),
                    rewrittenNode.getIsTemporaryTableWriter());
        }
    }
}
