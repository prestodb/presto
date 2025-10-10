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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TableWriterNode.MergeParadigmAndTypes;
import com.facebook.presto.spi.plan.TableWriterNode.MergeTarget;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TestingConnectorIndexHandle;
import com.facebook.presto.sql.planner.TestingConnectorTransactionHandle;
import com.facebook.presto.sql.planner.TestingWriterTarget;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.plan.ExchangeEncoding.COLUMNAR;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.MoreLists.nElements;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;

public class PlanBuilder
{
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Map<String, Type> variables = new HashMap<>();

    public PlanBuilder(Session session, PlanNodeIdAllocator idAllocator, Metadata metadata)
    {
        this.session = session;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    public static Assignments assignment(VariableReferenceExpression variable, RowExpression expression)
    {
        return Assignments.builder().put(variable, expression).build();
    }

    public static Assignments assignment(VariableReferenceExpression variable1, RowExpression expression1, VariableReferenceExpression variable2, RowExpression expression2)
    {
        return Assignments.builder().put(variable1, expression1).put(variable2, expression2).build();
    }

    public OutputNode output(List<String> columnNames, List<VariableReferenceExpression> variables, PlanNode source)
    {
        return new OutputNode(
                source.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                columnNames,
                variables);
    }

    public OutputNode output(Consumer<OutputBuilder> outputBuilderConsumer)
    {
        OutputBuilder outputBuilder = new OutputBuilder();
        outputBuilderConsumer.accept(outputBuilder);
        return outputBuilder.build();
    }

    public class OutputBuilder
    {
        private PlanNode source;
        private List<String> columnNames = new ArrayList<>();
        private List<VariableReferenceExpression> outputVariables = new ArrayList<>();

        public OutputBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public OutputBuilder column(VariableReferenceExpression variable, String columnName)
        {
            outputVariables.add(variable);
            columnNames.add(columnName);
            return this;
        }

        protected OutputNode build()
        {
            return new OutputNode(source.getSourceLocation(), idAllocator.getNextId(), source, columnNames, outputVariables);
        }
    }

    public ValuesNode values()
    {
        return values(idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of());
    }

    public ValuesNode values(VariableReferenceExpression... columns)
    {
        return values(idAllocator.getNextId(), 0, columns);
    }

    public ValuesNode values(PlanNodeId id, VariableReferenceExpression... columns)
    {
        return values(id, 0, columns);
    }

    public ValuesNode values(int rows, VariableReferenceExpression... columns)
    {
        return values(idAllocator.getNextId(), rows, columns);
    }

    public ValuesNode values(PlanNodeId id, int rows, VariableReferenceExpression... columns)
    {
        List<VariableReferenceExpression> variables = ImmutableList.copyOf(columns);
        return values(
                id,
                variables,
                nElements(rows, row -> nElements(columns.length, cell -> constantNull(variables.get(cell).getSourceLocation(), UNKNOWN))));
    }

    public ValuesNode values(List<VariableReferenceExpression> variables, List<List<RowExpression>> rows)
    {
        return values(idAllocator.getNextId(), variables, rows);
    }

    public ValuesNode values(PlanNodeId id, List<VariableReferenceExpression> variables, List<List<RowExpression>> rows)
    {
        return new ValuesNode(Optional.empty(), id, variables, rows, Optional.empty());
    }

    public EnforceSingleRowNode enforceSingleRow(PlanNode source)
    {
        return new EnforceSingleRowNode(source.getSourceLocation(), idAllocator.getNextId(), source);
    }

    public SortNode sort(List<VariableReferenceExpression> orderBy, PlanNode source)
    {
        return new SortNode(
                orderBy.get(0).getSourceLocation(),
                idAllocator.getNextId(),
                source,
                new OrderingScheme(orderBy.stream().map(variable -> new Ordering(variable, SortOrder.ASC_NULLS_FIRST)).collect(toImmutableList())),
                false,
                ImmutableList.of());
    }

    public OffsetNode offset(long rowCount, PlanNode source)
    {
        return new OffsetNode(source.getSourceLocation(), idAllocator.getNextId(), source, rowCount);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(source.getSourceLocation(), idAllocator.getNextId(), source, limit, FINAL);
    }

    public TopNNode topN(long count, List<VariableReferenceExpression> orderBy, PlanNode source)
    {
        return new TopNNode(
                orderBy.get(0).getSourceLocation(),
                idAllocator.getNextId(),
                source,
                count,
                new OrderingScheme(orderBy.stream().map(variable -> new Ordering(variable, ASC_NULLS_FIRST)).collect(toImmutableList())),
                TopNNode.Step.SINGLE);
    }

    public DistinctLimitNode distinctLimit(long count, List<VariableReferenceExpression> distinctSymbols, PlanNode source)
    {
        return new DistinctLimitNode(
                source.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                count,
                false,
                distinctSymbols,
                Optional.empty(),
                0);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(source.getSourceLocation(), idAllocator.getNextId(), source, sampleRatio, type);
    }

    public ProjectNode project(PlanNode source, Assignments assignments)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public MarkDistinctNode markDistinct(VariableReferenceExpression markerVariable, List<VariableReferenceExpression> distinctVariables, PlanNode source)
    {
        return new MarkDistinctNode(source.getSourceLocation(), idAllocator.getNextId(), source, markerVariable, distinctVariables, Optional.empty());
    }

    public MarkDistinctNode markDistinct(VariableReferenceExpression markerVariable, List<VariableReferenceExpression> distinctVariables, VariableReferenceExpression hashVariable, PlanNode source)
    {
        return new MarkDistinctNode(source.getSourceLocation(), idAllocator.getNextId(), source, markerVariable, distinctVariables, Optional.of(hashVariable));
    }

    public FilterNode filter(RowExpression predicate, PlanNode source)
    {
        return filter(idAllocator.getNextId(), predicate, source);
    }

    public FilterNode filter(PlanNodeId planNodeId, RowExpression predicate, PlanNode source)
    {
        return new FilterNode(source.getSourceLocation(), planNodeId, source, predicate);
    }

    public AggregationNode aggregation(Consumer<AggregationBuilder> aggregationBuilderConsumer)
    {
        AggregationBuilder aggregationBuilder = new AggregationBuilder(getTypes());
        aggregationBuilderConsumer.accept(aggregationBuilder);
        return aggregationBuilder.build();
    }

    public RemoteSourceNode remoteSource(List<PlanFragmentId> sourceFragmentIds)
    {
        return remoteSource(idAllocator.getNextId(), sourceFragmentIds, ImmutableList.of());
    }

    public RemoteSourceNode remoteSource(PlanNodeId planNodeId, List<PlanFragmentId> sourceFragmentIds, List<VariableReferenceExpression> outputVariables)
    {
        return new RemoteSourceNode(Optional.empty(), planNodeId, sourceFragmentIds, outputVariables, false, Optional.empty(), REPARTITION, COLUMNAR);
    }

    public RemoteSourceNode remoteSource(List<PlanFragmentId> sourceFragmentIds, PlanNode statsEquivalentPlanNode)
    {
        return new RemoteSourceNode(
                Optional.empty(),
                idAllocator.getNextId(),
                Optional.of(statsEquivalentPlanNode),
                sourceFragmentIds, ImmutableList.of(),
                false,
                Optional.empty(),
                REPARTITION,
                COLUMNAR);
    }

    public CallExpression binaryOperation(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()).arithmeticFunction(operatorType, left.getType(), right.getType());
        return call(operatorType.getOperator(), functionHandle, left.getType(), left, right);
    }

    public CallExpression comparison(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()).comparisonFunction(operatorType, left.getType(), right.getType());
        return call(operatorType.getOperator(), functionHandle, left.getType(), left, right);
    }

    public class AggregationBuilder
    {
        private final TypeProvider types;
        private PlanNode source;
        private PlanNodeId planNodeId;
        // Preserve order when creating assignments, so it's consistent when printed/iterated. Some
        // optimizations create variable names by iterating over it, and this will make plan more consistent
        // in future runs.
        private Map<VariableReferenceExpression, Aggregation> assignments = new LinkedHashMap<>();
        private AggregationNode.GroupingSetDescriptor groupingSets;
        private List<VariableReferenceExpression> preGroupedVariables = new ArrayList<>();
        private Step step = Step.SINGLE;
        private Optional<VariableReferenceExpression> hashVariable = Optional.empty();
        private Optional<VariableReferenceExpression> groupIdVariable = Optional.empty();
        private Session session = testSessionBuilder().build();

        public AggregationBuilder(TypeProvider types)
        {
            this.types = types;
        }

        public AggregationBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public AggregationBuilder setPlanNodeId(PlanNodeId planNodeId)
        {
            this.planNodeId = planNodeId;
            return this;
        }

        public AggregationBuilder addAggregation(VariableReferenceExpression output, RowExpression expression)
        {
            return addAggregation(output, expression, false);
        }

        public AggregationBuilder addAggregation(VariableReferenceExpression output, RowExpression expression, boolean isDistinct)
        {
            return addAggregation(output, expression, Optional.empty(), Optional.empty(), isDistinct, Optional.empty());
        }

        public AggregationBuilder addAggregation(
                VariableReferenceExpression output,
                RowExpression expression,
                Optional<RowExpression> filter,
                Optional<OrderingScheme> orderingScheme,
                boolean isDistinct,
                Optional<VariableReferenceExpression> mask)
        {
            checkArgument(expression instanceof CallExpression);
            CallExpression call = (CallExpression) expression;
            return addAggregation(output, new Aggregation(
                    call,
                    filter,
                    orderingScheme,
                    isDistinct,
                    mask));
        }

        public AggregationBuilder addAggregation(VariableReferenceExpression output, Aggregation aggregation)
        {
            assignments.put(output, aggregation);
            return this;
        }

        public AggregationBuilder globalGrouping()
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.of()));
            return this;
        }

        public AggregationBuilder singleGroupingSet(VariableReferenceExpression... variables)
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.copyOf(variables)));
            return this;
        }

        public AggregationBuilder groupingSets(AggregationNode.GroupingSetDescriptor groupingSets)
        {
            checkState(this.groupingSets == null, "groupingSets already defined");
            this.groupingSets = groupingSets;
            return this;
        }

        public AggregationBuilder preGroupedVariables(VariableReferenceExpression... variables)
        {
            checkState(this.preGroupedVariables.isEmpty(), "preGroupedVariables already defined");
            this.preGroupedVariables = ImmutableList.copyOf(variables);
            return this;
        }

        public AggregationBuilder step(Step step)
        {
            this.step = step;
            return this;
        }

        public AggregationBuilder hashVariable(VariableReferenceExpression hashVariable)
        {
            this.hashVariable = Optional.of(hashVariable);
            return this;
        }

        public AggregationBuilder groupIdVariable(VariableReferenceExpression groupIdVariable)
        {
            this.groupIdVariable = Optional.of(groupIdVariable);
            return this;
        }

        protected AggregationNode build()
        {
            checkState(groupingSets != null, "No grouping sets defined; use globalGrouping/groupingKeys method");
            return new AggregationNode(
                    source.getSourceLocation(),
                    planNodeId == null ? idAllocator.getNextId() : planNodeId,
                    source,
                    assignments,
                    groupingSets,
                    preGroupedVariables,
                    step,
                    hashVariable,
                    groupIdVariable,
                    Optional.empty());
        }
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<VariableReferenceExpression> correlation, PlanNode input, PlanNode subquery)
    {
        return apply(subqueryAssignments, correlation, input, subquery, false);
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<VariableReferenceExpression> correlation, PlanNode input, PlanNode subquery, boolean mayParticipateInAntiJoin)
    {
        verifySubquerySupported(subqueryAssignments);
        return new ApplyNode(subquery.getSourceLocation(), idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation, "", mayParticipateInAntiJoin);
    }

    public AssignUniqueId assignUniqueId(VariableReferenceExpression variable, PlanNode source)
    {
        return new AssignUniqueId(source.getSourceLocation(), idAllocator.getNextId(), source, variable);
    }

    public LateralJoinNode lateral(List<VariableReferenceExpression> correlation, PlanNode input, PlanNode subquery)
    {
        return new LateralJoinNode(subquery.getSourceLocation(), idAllocator.getNextId(), input, subquery, correlation, LateralJoinNode.Type.INNER, "");
    }

    public TableScanNode tableScan(String catalogName, List<VariableReferenceExpression> variables, Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(catalogName),
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        return tableScan(tableHandle, variables, assignments, TupleDomain.all(), TupleDomain.all());
    }

    public TableScanNode tableScan(List<VariableReferenceExpression> variables, Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        return tableScan("testConnector", variables, assignments);
    }

    public TableScanNode tableScan(TableHandle tableHandle, List<VariableReferenceExpression> variables, Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        return tableScan(tableHandle, variables, assignments, TupleDomain.all(), TupleDomain.all());
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<VariableReferenceExpression> variables,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                variables,
                assignments,
                ImmutableList.of(),
                currentConstraint,
                enforcedConstraint,
                Optional.empty());
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<VariableReferenceExpression> variables,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint,
            List<TableConstraint<ColumnHandle>> tableConstraints)
    {
        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                variables,
                assignments,
                tableConstraints,
                currentConstraint,
                enforcedConstraint, Optional.empty());
    }

    public TableFinishNode tableDelete(SchemaTableName schemaTableName, PlanNode deleteSource, VariableReferenceExpression deleteRowId)
    {
        TableWriterNode.DeleteHandle deleteHandle = new TableWriterNode.DeleteHandle(
                new TableHandle(
                        new ConnectorId("testConnector"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.empty()),
                schemaTableName);
        return new TableFinishNode(
                deleteSource.getSourceLocation(),
                idAllocator.getNextId(),
                exchange(e -> e
                        .addSource(new DeleteNode(
                                deleteSource.getSourceLocation(),
                                idAllocator.getNextId(),
                                deleteSource,
                                Optional.of(deleteRowId),
                                ImmutableList.of(deleteRowId),
                                Optional.empty()))
                        .addInputsSet(deleteRowId)
                        .singleDistributionPartitioningScheme(deleteRowId)),
                Optional.of(deleteHandle),
                deleteRowId,
                Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public MergeWriterNode merge(
            SchemaTableName schemaTableName,
            PlanNode mergeSource,
            List<VariableReferenceExpression> inputSymbols,
            List<VariableReferenceExpression> outputSymbols)
    {
        return new MergeWriterNode(
                mergeSource.getSourceLocation(),
                idAllocator.getNextId(),
                mergeSource,
                mergeTarget(schemaTableName),
                inputSymbols,
                Optional.empty(),
                outputSymbols);
    }

    private MergeTarget mergeTarget(SchemaTableName schemaTableName)
    {
        return new MergeTarget(
                new TableHandle(
                        new ConnectorId("testConnector"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.empty()),
                Optional.empty(),
                schemaTableName,
                new MergeParadigmAndTypes(RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW, ImmutableList.of(), INTEGER));
    }

    public ExchangeNode gatheringExchange(ExchangeNode.Scope scope, PlanNode child)
    {
        return exchange(builder -> builder.type(ExchangeNode.Type.GATHER)
                .scope(scope)
                .singleDistributionPartitioningScheme(child.getOutputVariables())
                .addSource(child)
                .addInputsSet(child.getOutputVariables()));
    }

    public SemiJoinNode semiJoin(
            VariableReferenceExpression sourceJoinVariable,
            VariableReferenceExpression filteringSourceJoinVariable,
            VariableReferenceExpression semiJoinOutput,
            Optional<VariableReferenceExpression> sourceHashVariable,
            Optional<VariableReferenceExpression> filteringSourceHashVariable,
            PlanNode source,
            PlanNode filteringSource)
    {
        return semiJoin(
                source,
                filteringSource,
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinOutput,
                sourceHashVariable,
                filteringSourceHashVariable,
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            VariableReferenceExpression sourceJoinVariable,
            VariableReferenceExpression filteringSourceJoinVariable,
            VariableReferenceExpression semiJoinOutput,
            Optional<VariableReferenceExpression> sourceHashVariable,
            Optional<VariableReferenceExpression> filteringSourceHashVariable,
            Optional<SemiJoinNode.DistributionType> distributionType)
    {
        return new SemiJoinNode(
                filteringSource.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                filteringSource,
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinOutput,
                sourceHashVariable,
                filteringSourceHashVariable,
                distributionType,
                ImmutableMap.of());
    }

    public IndexSourceNode indexSource(
            TableHandle tableHandle,
            Set<VariableReferenceExpression> lookupVariables,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> effectiveTupleDomain)
    {
        return new IndexSourceNode(
                Optional.empty(),
                idAllocator.getNextId(),
                new IndexHandle(
                        tableHandle.getConnectorId(),
                        TestingConnectorTransactionHandle.INSTANCE,
                        TestingConnectorIndexHandle.INSTANCE),
                tableHandle,
                lookupVariables,
                outputVariables,
                assignments,
                effectiveTupleDomain);
    }

    public ExchangeNode exchange(Consumer<ExchangeBuilder> exchangeBuilderConsumer)
    {
        ExchangeBuilder exchangeBuilder = new ExchangeBuilder();
        exchangeBuilderConsumer.accept(exchangeBuilder);
        return exchangeBuilder.build();
    }

    public class ExchangeBuilder
    {
        private ExchangeNode.Type type = ExchangeNode.Type.GATHER;
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE_STREAMING;
        private PartitioningScheme partitioningScheme;
        private boolean ensureSourceOrdering;
        private OrderingScheme orderingScheme;
        private List<PlanNode> sources = new ArrayList<>();
        private List<List<VariableReferenceExpression>> inputs = new ArrayList<>();

        public ExchangeBuilder type(ExchangeNode.Type type)
        {
            this.type = type;
            return this;
        }

        public ExchangeBuilder scope(ExchangeNode.Scope scope)
        {
            this.scope = scope;
            return this;
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(VariableReferenceExpression... outputVariables)
        {
            return singleDistributionPartitioningScheme(Arrays.asList(outputVariables));
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(List<VariableReferenceExpression> outputVariables)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), outputVariables));
        }

        public ExchangeBuilder fixedHashDistributionPartitioningScheme(List<VariableReferenceExpression> outputVariables, List<VariableReferenceExpression> partitioningVariables)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningVariables)),
                    ImmutableList.copyOf(outputVariables)));
        }

        public ExchangeBuilder fixedHashDistributionPartitioningScheme(List<VariableReferenceExpression> outputVariables, List<VariableReferenceExpression> partitioningVariables, VariableReferenceExpression hashVariable)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningVariables)),
                    ImmutableList.copyOf(outputVariables),
                    Optional.of(hashVariable)));
        }

        public ExchangeBuilder partitioningScheme(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
            return this;
        }

        public ExchangeBuilder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public ExchangeBuilder addInputsSet(VariableReferenceExpression... inputs)
        {
            return addInputsSet(Arrays.asList(inputs));
        }

        public ExchangeBuilder addInputsSet(List<VariableReferenceExpression> inputs)
        {
            this.inputs.add(inputs);
            return this;
        }

        public ExchangeBuilder setEnsureSourceOrdering(boolean ensureSourceOrdering)
        {
            this.ensureSourceOrdering = ensureSourceOrdering;
            return this;
        }

        public ExchangeBuilder orderingScheme(OrderingScheme orderingScheme)
        {
            this.orderingScheme = orderingScheme;
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(Optional.empty(), idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs, ensureSourceOrdering, Optional.ofNullable(orderingScheme));
        }
    }

    public JoinNode join(JoinType joinType, PlanNode left, PlanNode right, EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.empty(), criteria);
    }

    public JoinNode join(JoinType joinType, PlanNode left, PlanNode right, RowExpression filter, EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.of(filter), criteria);
    }

    private JoinNode join(JoinType joinType, PlanNode left, PlanNode right, Optional<RowExpression> filter, EquiJoinClause... criteria)
    {
        return join(
                joinType,
                left,
                right,
                ImmutableList.copyOf(criteria),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                filter,
                Optional.empty(),
                Optional.empty());
    }

    public JoinNode join(JoinType type, PlanNode left, PlanNode right, List<EquiJoinClause> criteria, List<VariableReferenceExpression> outputVariables, Optional<RowExpression> filter)
    {
        return join(type, left, right, criteria, outputVariables, filter, Optional.empty(), Optional.empty());
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable)
    {
        return join(type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.empty(), ImmutableMap.of());
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        return join(type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.empty(), dynamicFilters);
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable,
            Optional<JoinDistributionType> distributionType,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        return new JoinNode(Optional.empty(), idAllocator.getNextId(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType, dynamicFilters);
    }

    public PlanNode indexJoin(JoinType type, PlanNode probe, PlanNode index)
    {
        return indexJoin(type, probe, index, emptyList(), Optional.empty());
    }

    public PlanNode indexJoin(JoinType type,
            PlanNode probe,
            PlanNode index,
            List<IndexJoinNode.EquiJoinClause> criteria,
            Optional<RowExpression> filter)
    {
        return new IndexJoinNode(
                Optional.empty(),
                idAllocator.getNextId(),
                type,
                probe,
                index,
                criteria,
                filter,
                Optional.empty(),
                Optional.empty(),
                index.getOutputVariables());
    }

    public CteProducerNode cteProducerNode(String ctename,
            VariableReferenceExpression rowCountVar, List<VariableReferenceExpression> outputVars, PlanNode source)
    {
        return new CteProducerNode(Optional.empty(),
                idAllocator.getNextId(),
                source,
                ctename,
                rowCountVar, outputVars);
    }

    public UnionNode union(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs, List<PlanNode> sources)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = fromListMultimap(outputsToInputs);
        return new UnionNode(Optional.empty(), idAllocator.getNextId(), sources, ImmutableList.copyOf(mapping.keySet()), mapping);
    }

    public IntersectNode intersect(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs, List<PlanNode> sources)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = fromListMultimap(outputsToInputs);
        return new IntersectNode(Optional.empty(), idAllocator.getNextId(), sources, ImmutableList.copyOf(mapping.keySet()), mapping);
    }

    public ExceptNode except(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs, List<PlanNode> sources)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = fromListMultimap(outputsToInputs);
        return new ExceptNode(Optional.empty(), idAllocator.getNextId(), sources, ImmutableList.copyOf(mapping.keySet()), mapping);
    }

    public TableWriterNode tableWriter(List<VariableReferenceExpression> columns, List<String> columnNames, PlanNode source)
    {
        return new TableWriterNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                Optional.of(new TestingWriterTarget()),
                variable("partialrows", BIGINT),
                variable("fragment", VARBINARY),
                variable("tablecommitcontext", VARBINARY),
                columns,
                columnNames,
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public VariableReferenceExpression variable(String name)
    {
        return variable(name, BIGINT);
    }

    public VariableReferenceExpression variable(VariableReferenceExpression variable)
    {
        return variable(variable.getName(), variable.getType());
    }

    public VariableReferenceExpression variable(String name, Type type)
    {
        Type old = variables.put(name, type);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Variable '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            variables.put(name, type);
        }
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    public PlanBuilder registerVariable(VariableReferenceExpression expression)
    {
        variable(expression);
        return this;
    }

    public WindowNode window(DataOrganizationSpecification specification, Map<VariableReferenceExpression, WindowNode.Function> functions, PlanNode source)
    {
        return new WindowNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public WindowNode window(DataOrganizationSpecification specification, Map<VariableReferenceExpression, WindowNode.Function> functions, VariableReferenceExpression hashVariable, PlanNode source)
    {
        return new WindowNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.of(hashVariable),
                ImmutableSet.of(),
                0);
    }

    public RowNumberNode rowNumber(List<VariableReferenceExpression> partitionBy, Optional<Integer> maxRowCountPerPartition, VariableReferenceExpression rowNumberVariable, PlanNode source)
    {
        return new RowNumberNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                partitionBy,
                rowNumberVariable,
                maxRowCountPerPartition,
                false,
                Optional.empty());
    }

    public UnnestNode unnest(PlanNode source, List<VariableReferenceExpression> replicateVariables, Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables, Optional<VariableReferenceExpression> ordinalityVariable)
    {
        return new UnnestNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                replicateVariables,
                unnestVariables,
                ordinalityVariable);
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static Expression expression(String sql, ParsingOptions.DecimalLiteralTreatment decimalLiteralTreatment)
    {
        ParsingOptions.Builder builder = ParsingOptions.builder();
        builder.setDecimalLiteralTreatment(decimalLiteralTreatment);
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, builder.build()));
    }

    public RowExpression rowExpression(String sql)
    {
        return rowExpression(expression(sql));
    }

    public RowExpression rowExpression(String sql, ParsingOptions.DecimalLiteralTreatment decimalLiteralTreatment)
    {
        return rowExpression(expression(sql, decimalLiteralTreatment));
    }

    private RowExpression rowExpression(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                getTypes(),
                expression,
                ImmutableMap.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionTypes,
                ImmutableMap.of(),
                metadata.getFunctionAndTypeManager(),
                session);
    }

    public static Expression expression(String sql, ParsingOptions options)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, options));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(toImmutableList());
    }

    public static List<RowExpression> constantExpressions(Type type, Object... values)
    {
        return Stream.of(values)
                .map(value -> constant(value, type))
                .collect(toImmutableList());
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(variables);
    }

    public PlanNodeIdAllocator getIdAllocator()
    {
        return idAllocator;
    }

    public GroupIdNode groupId(List<List<VariableReferenceExpression>> groupingSets, List<VariableReferenceExpression> aggregationArguments, VariableReferenceExpression groupIdSymbol, PlanNode source)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(toImmutableMap(identity(), identity()));
        return groupId(groupingSets, groupingColumns, aggregationArguments, groupIdSymbol, source);
    }

    public GroupIdNode groupId(List<List<VariableReferenceExpression>> groupingSets,
            Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns,
            List<VariableReferenceExpression> aggregationArguments,
            VariableReferenceExpression groupIdSymbol,
            PlanNode source)
    {
        return new GroupIdNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                groupingSets,
                groupingColumns,
                aggregationArguments,
                groupIdSymbol);
    }
}
