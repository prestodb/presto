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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.TestingConnectorIndexHandle;
import com.facebook.presto.sql.planner.TestingConnectorTransactionHandle;
import com.facebook.presto.sql.planner.TestingWriterTarget;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.PlannerUtils.toOrderingScheme;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.MoreLists.nElements;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

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

    public static Assignments assignment(VariableReferenceExpression variable, Expression expression)
    {
        return Assignments.builder().put(variable, OriginalExpressionUtils.castToRowExpression(expression)).build();
    }

    public static Assignments assignment(VariableReferenceExpression variable, RowExpression expression)
    {
        return Assignments.builder().put(variable, expression).build();
    }

    public static Assignments assignment(VariableReferenceExpression variable1, Expression expression1, VariableReferenceExpression variable2, Expression expression2)
    {
        return Assignments.builder().put(variable1, OriginalExpressionUtils.castToRowExpression(expression1)).put(variable2, OriginalExpressionUtils.castToRowExpression(expression2)).build();
    }

    public static Assignments assignment(VariableReferenceExpression variable1, RowExpression expression1, VariableReferenceExpression variable2, RowExpression expression2)
    {
        return Assignments.builder().put(variable1, expression1).put(variable2, expression2).build();
    }

    public OutputNode output(List<String> columnNames, List<VariableReferenceExpression> variables, PlanNode source)
    {
        return new OutputNode(
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
            return new OutputNode(idAllocator.getNextId(), source, columnNames, outputVariables);
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
                nElements(rows, row -> nElements(columns.length, cell -> constantNull(UNKNOWN))));
    }

    public ValuesNode values(List<VariableReferenceExpression> variables, List<List<RowExpression>> rows)
    {
        return values(idAllocator.getNextId(), variables, rows);
    }

    public ValuesNode values(PlanNodeId id, List<VariableReferenceExpression> variables, List<List<RowExpression>> rows)
    {
        return new ValuesNode(id, variables, rows);
    }

    public EnforceSingleRowNode enforceSingleRow(PlanNode source)
    {
        return new EnforceSingleRowNode(idAllocator.getNextId(), source);
    }

    public SortNode sort(List<VariableReferenceExpression> orderBy, PlanNode source)
    {
        ImmutableList<Ordering> ordering = orderBy.stream().map(variable -> new Ordering(variable, ASC_NULLS_FIRST)).collect(toImmutableList());
        return new SortNode(
                idAllocator.getNextId(),
                source,
                new OrderingScheme(ordering),
                false);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit, FINAL);
    }

    public TopNNode topN(long count, List<VariableReferenceExpression> orderBy, PlanNode source)
    {
        return new TopNNode(
                idAllocator.getNextId(),
                source,
                count,
                new OrderingScheme(orderBy.stream().map(variable -> new Ordering(variable, ASC_NULLS_FIRST)).collect(toImmutableList())),
                TopNNode.Step.SINGLE);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public MarkDistinctNode markDistinct(VariableReferenceExpression markerVariable, List<VariableReferenceExpression> distinctVariables, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerVariable, distinctVariables, Optional.empty());
    }

    public MarkDistinctNode markDistinct(VariableReferenceExpression markerVariable, List<VariableReferenceExpression> distinctVariables, VariableReferenceExpression hashVariable, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerVariable, distinctVariables, Optional.of(hashVariable));
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, OriginalExpressionUtils.castToRowExpression(predicate));
    }

    public FilterNode filter(RowExpression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public AggregationNode aggregation(Consumer<AggregationBuilder> aggregationBuilderConsumer)
    {
        AggregationBuilder aggregationBuilder = new AggregationBuilder(getTypes());
        aggregationBuilderConsumer.accept(aggregationBuilder);
        return aggregationBuilder.build();
    }

    public CallExpression binaryOperation(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = new FunctionResolution(metadata.getFunctionAndTypeManager()).arithmeticFunction(operatorType, left.getType(), right.getType());
        return call(operatorType.getOperator(), functionHandle, left.getType(), left, right);
    }

    public CallExpression comparison(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = new FunctionResolution(metadata.getFunctionAndTypeManager()).comparisonFunction(operatorType, left.getType(), right.getType());
        return call(operatorType.getOperator(), functionHandle, left.getType(), left, right);
    }

    public DistinctLimitNode distinctLimit(long count, List<VariableReferenceExpression> distinctSymbols, PlanNode source)
    {
        return new DistinctLimitNode(
                idAllocator.getNextId(),
                source,
                count,
                false,
                distinctSymbols,
                Optional.empty());
    }

    public class AggregationBuilder
    {
        private final TypeProvider types;
        private PlanNode source;
        private Map<VariableReferenceExpression, Aggregation> assignments = new HashMap<>();
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

        public AggregationBuilder addAggregation(VariableReferenceExpression output, Expression expression, List<Type> inputTypes)
        {
            return addAggregation(output, expression, inputTypes, Optional.empty());
        }

        public AggregationBuilder addAggregation(VariableReferenceExpression output, Expression expression, List<Type> inputTypes, VariableReferenceExpression mask)
        {
            return addAggregation(output, expression, inputTypes, Optional.of(mask));
        }

        private AggregationBuilder addAggregation(VariableReferenceExpression output, Expression expression, List<Type> inputTypes, Optional<VariableReferenceExpression> mask)
        {
            checkArgument(expression instanceof FunctionCall);
            FunctionCall call = (FunctionCall) expression;
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveFunction(
                    Optional.of(session.getSessionFunctions()),
                    session.getTransactionId(),
                    qualifyObjectName(call.getName()),
                    TypeSignatureProvider.fromTypes(inputTypes));
            return addAggregation(output, new Aggregation(
                    new CallExpression(
                            call.getName().getSuffix(),
                            functionHandle,
                            metadata.getType(metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle).getReturnType()),
                            call.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                    call.getFilter().map(OriginalExpressionUtils::castToRowExpression),
                    call.getOrderBy().map(orderBy -> toOrderingScheme(orderBy, types)),
                    call.isDistinct(),
                    mask));
        }

        public AggregationBuilder addAggregation(VariableReferenceExpression output, RowExpression expression)
        {
            return addAggregation(output, expression, Optional.empty(), Optional.empty(), false, Optional.empty());
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
                    idAllocator.getNextId(),
                    source,
                    assignments,
                    groupingSets,
                    preGroupedVariables,
                    step,
                    hashVariable,
                    groupIdVariable);
        }
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<VariableReferenceExpression> correlation, PlanNode input, PlanNode subquery)
    {
        verifySubquerySupported(subqueryAssignments);
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation, "");
    }

    public AssignUniqueId assignUniqueId(VariableReferenceExpression variable, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, variable);
    }

    public LateralJoinNode lateral(List<VariableReferenceExpression> correlation, PlanNode input, PlanNode subquery)
    {
        return new LateralJoinNode(idAllocator.getNextId(), input, subquery, correlation, LateralJoinNode.Type.INNER, "");
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
                idAllocator.getNextId(),
                tableHandle,
                variables,
                assignments,
                currentConstraint,
                enforcedConstraint);
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
                idAllocator.getNextId(),
                exchange(e -> e
                        .addSource(new DeleteNode(
                                idAllocator.getNextId(),
                                deleteSource,
                                deleteRowId,
                                ImmutableList.of(deleteRowId)))
                        .addInputsSet(deleteRowId)
                        .singleDistributionPartitioningScheme(deleteRowId)),
                Optional.of(deleteHandle),
                deleteRowId,
                Optional.empty(),
                Optional.empty());
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

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<VariableReferenceExpression> outputVariables, List<VariableReferenceExpression> partitioningVariables)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningVariables)),
                    ImmutableList.copyOf(outputVariables)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<VariableReferenceExpression> outputVariables, List<VariableReferenceExpression> partitioningVariables, VariableReferenceExpression hashVariable)
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
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs, ensureSourceOrdering, Optional.ofNullable(orderingScheme));
        }
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.empty(), criteria);
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, RowExpression filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.of(filter), criteria);
    }

    private JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, Optional<RowExpression> filter, JoinNode.EquiJoinClause... criteria)
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

    public JoinNode join(JoinNode.Type type, PlanNode left, PlanNode right, List<JoinNode.EquiJoinClause> criteria, List<VariableReferenceExpression> outputVariables, Optional<RowExpression> filter)
    {
        return join(type, left, right, criteria, outputVariables, filter, Optional.empty(), Optional.empty());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable)
    {
        return join(type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.empty(), ImmutableMap.of());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        return join(type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.empty(), dynamicFilters);
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            Optional<VariableReferenceExpression> rightHashVariable,
            Optional<JoinNode.DistributionType> distributionType,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        return new JoinNode(idAllocator.getNextId(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType, dynamicFilters);
    }

    public PlanNode indexJoin(IndexJoinNode.Type type, TableScanNode probe, TableScanNode index)
    {
        return new IndexJoinNode(
                idAllocator.getNextId(),
                type,
                probe,
                index,
                emptyList(),
                Optional.empty(),
                Optional.empty());
    }

    public UnionNode union(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs, List<PlanNode> sources)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = fromListMultimap(outputsToInputs);
        return new UnionNode(idAllocator.getNextId(), sources, ImmutableList.copyOf(mapping.keySet()), mapping);
    }

    public IntersectNode intersect(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs, List<PlanNode> sources)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = fromListMultimap(outputsToInputs);
        return new IntersectNode(idAllocator.getNextId(), sources, ImmutableList.copyOf(mapping.keySet()), mapping);
    }

    public TableWriterNode tableWriter(List<VariableReferenceExpression> columns, List<String> columnNames, PlanNode source)
    {
        return new TableWriterNode(
                idAllocator.getNextId(),
                source,
                Optional.of(new TestingWriterTarget()),
                variable("partialrows", BIGINT),
                variable("fragment", VARBINARY),
                variable("tablecommitcontext", VARBINARY),
                columns,
                columnNames,
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
        return new VariableReferenceExpression(name, type);
    }

    public WindowNode window(WindowNode.Specification specification, Map<VariableReferenceExpression, WindowNode.Function> functions, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public WindowNode window(WindowNode.Specification specification, Map<VariableReferenceExpression, WindowNode.Function> functions, VariableReferenceExpression hashVariable, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.of(hashVariable),
                ImmutableSet.of(),
                0);
    }

    public RowNumberNode rowNumber(List<VariableReferenceExpression> partitionBy, Optional<Integer> maxRowCountPerPartition, VariableReferenceExpression rownNumberVariable, PlanNode source)
    {
        return new RowNumberNode(
                idAllocator.getNextId(),
                source,
                partitionBy,
                rownNumberVariable,
                maxRowCountPerPartition,
                Optional.empty());
    }

    public UnnestNode unnest(PlanNode source, List<VariableReferenceExpression> replicateVariables, Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables, Optional<VariableReferenceExpression> ordinalityVariable)
    {
        return new UnnestNode(
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

    public RowExpression rowExpression(String sql)
    {
        Expression expression = expression(sql);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                getTypes(),
                expression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionTypes,
                ImmutableMap.of(),
                metadata.getFunctionAndTypeManager(),
                session);
    }

    public static RowExpression castToRowExpression(String sql)
    {
        return OriginalExpressionUtils.castToRowExpression(ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql)));
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
}
