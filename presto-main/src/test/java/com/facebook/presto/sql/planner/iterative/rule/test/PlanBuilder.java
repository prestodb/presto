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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingConnectorIndexHandle;
import com.facebook.presto.sql.planner.TestingConnectorTransactionHandle;
import com.facebook.presto.sql.planner.TestingWriterTarget;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public PlanBuilder(PlanNodeIdAllocator idAllocator, Metadata metadata)
    {
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    public OutputNode output(List<String> columnNames, List<Symbol> outputs, PlanNode source)
    {
        return new OutputNode(
                idAllocator.getNextId(),
                source,
                columnNames,
                outputs);
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
        private List<Symbol> outputs = new ArrayList<>();

        public OutputBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public OutputBuilder column(Symbol symbol)
        {
            return column(symbol, symbol.getName());
        }

        public OutputBuilder column(Symbol symbol, String columnName)
        {
            outputs.add(symbol);
            columnNames.add(columnName);
            return this;
        }

        protected OutputNode build()
        {
            return new OutputNode(idAllocator.getNextId(), source, columnNames, outputs);
        }
    }

    public ValuesNode values(Symbol... columns)
    {
        return values(idAllocator.getNextId(), columns);
    }

    public ValuesNode values(PlanNodeId id, Symbol... columns)
    {
        return new ValuesNode(
                id,
                ImmutableList.copyOf(columns),
                ImmutableList.of());
    }

    public ValuesNode values(List<Symbol> columns, List<List<Expression>> rows)
    {
        return new ValuesNode(idAllocator.getNextId(), columns, rows);
    }

    public EnforceSingleRowNode enforceSingleRow(PlanNode source)
    {
        return new EnforceSingleRowNode(idAllocator.getNextId(), source);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit, false);
    }

    public TopNNode topN(long count, List<Symbol> orderBy, PlanNode source)
    {
        return new TopNNode(
                idAllocator.getNextId(),
                source,
                count,
                new OrderingScheme(
                        orderBy,
                        Maps.toMap(orderBy, Functions.constant(SortOrder.ASC_NULLS_FIRST))),
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

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.empty());
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, Symbol hashSymbol, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.of(hashSymbol));
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public AggregationNode aggregation(Consumer<AggregationBuilder> aggregationBuilderConsumer)
    {
        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        aggregationBuilderConsumer.accept(aggregationBuilder);
        return aggregationBuilder.build();
    }

    public class AggregationBuilder
    {
        private PlanNode source;
        private Map<Symbol, Aggregation> assignments = new HashMap<>();
        private List<List<Symbol>> groupingSets = new ArrayList<>();
        private List<Symbol> preGroupedSymbols = new ArrayList<>();
        private Step step = Step.SINGLE;
        private Optional<Symbol> hashSymbol = Optional.empty();
        private Optional<Symbol> groupIdSymbol = Optional.empty();

        public AggregationBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes)
        {
            return addAggregation(output, expression, inputTypes, Optional.empty());
        }

        public AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes, Symbol mask)
        {
            return addAggregation(output, expression, inputTypes, Optional.of(mask));
        }

        private AggregationBuilder addAggregation(Symbol output, Expression expression, List<Type> inputTypes, Optional<Symbol> mask)
        {
            checkArgument(expression instanceof FunctionCall);
            FunctionCall aggregation = (FunctionCall) expression;
            Signature signature = metadata.getFunctionRegistry().resolveFunction(aggregation.getName(), TypeSignatureProvider.fromTypes(inputTypes));
            return addAggregation(output, new Aggregation(aggregation, signature, mask));
        }

        public AggregationBuilder addAggregation(Symbol output, Aggregation aggregation)
        {
            assignments.put(output, aggregation);
            return this;
        }

        public AggregationBuilder globalGrouping()
        {
            return groupingSets(ImmutableList.of(ImmutableList.of()));
        }

        public AggregationBuilder groupingSets(List<List<Symbol>> groupingSets)
        {
            checkState(this.groupingSets.isEmpty(), "groupingSets already defined");
            this.groupingSets.addAll(groupingSets);
            return this;
        }

        public AggregationBuilder addGroupingSet(Symbol... symbols)
        {
            return addGroupingSet(ImmutableList.copyOf(symbols));
        }

        public AggregationBuilder addGroupingSet(List<Symbol> symbols)
        {
            groupingSets.add(ImmutableList.copyOf(symbols));
            return this;
        }

        public AggregationBuilder preGroupedSymbols(Symbol... symbols)
        {
            checkState(this.preGroupedSymbols.isEmpty(), "preGroupedSymbols already defined");
            this.preGroupedSymbols = ImmutableList.copyOf(symbols);
            return this;
        }

        public AggregationBuilder step(Step step)
        {
            this.step = step;
            return this;
        }

        public AggregationBuilder hashSymbol(Symbol hashSymbol)
        {
            this.hashSymbol = Optional.of(hashSymbol);
            return this;
        }

        public AggregationBuilder groupIdSymbol(Symbol groupIdSymbol)
        {
            this.groupIdSymbol = Optional.of(groupIdSymbol);
            return this;
        }

        protected AggregationNode build()
        {
            checkState(!groupingSets.isEmpty(), "No grouping sets defined; use globalGrouping/addGroupingSet/addEmptyGroupingSet method");
            return new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    assignments,
                    groupingSets,
                    preGroupedSymbols,
                    step,
                    hashSymbol,
                    groupIdSymbol);
        }
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        NullLiteral originSubquery = new NullLiteral(); // does not matter for tests
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation, originSubquery);
    }

    public AssignUniqueId assignUniqueId(Symbol unique, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, unique);
    }

    public LateralJoinNode lateral(List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        NullLiteral originSubquery = new NullLiteral(); // does not matter for tests
        return new LateralJoinNode(idAllocator.getNextId(), input, subquery, correlation, LateralJoinNode.Type.INNER, originSubquery);
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(symbols, assignments, null);
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments, Expression originalConstraint)
    {
        TableHandle tableHandle = new TableHandle(new ConnectorId("testConnector"), new TestingTableHandle());
        return tableScan(tableHandle, symbols, assignments, Optional.empty(), TupleDomain.all(), originalConstraint);
    }

    public TableScanNode tableScan(TableHandle tableHandle, List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(tableHandle, symbols, assignments, Optional.empty());
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            Optional<TableLayoutHandle> tableLayout)
    {
        return tableScan(tableHandle, symbols, assignments, tableLayout, TupleDomain.all());
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            Optional<TableLayoutHandle> tableLayout,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableScan(tableHandle, symbols, assignments, tableLayout, tupleDomain, null);
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            Optional<TableLayoutHandle> tableLayout,
            TupleDomain<ColumnHandle> tupleDomain,
            Expression originalConstraint)
    {
        return new TableScanNode(
                idAllocator.getNextId(),
                tableHandle,
                symbols,
                assignments,
                tableLayout,
                tupleDomain,
                originalConstraint);
    }

    public TableFinishNode tableDelete(SchemaTableName schemaTableName, PlanNode deleteSource, Symbol deleteRowId)
    {
        TableWriterNode.DeleteHandle deleteHandle = new TableWriterNode.DeleteHandle(
                new TableHandle(
                        new ConnectorId("testConnector"),
                        new TestingTableHandle()),
                schemaTableName);
        return new TableFinishNode(
                idAllocator.getNextId(),
                exchange(e -> e
                        .addSource(new DeleteNode(
                                idAllocator.getNextId(),
                                deleteSource,
                                deleteHandle,
                                deleteRowId,
                                ImmutableList.of(deleteRowId)))
                        .addInputsSet(deleteRowId)
                        .singleDistributionPartitioningScheme(deleteRowId)),
                deleteHandle,
                ImmutableList.of(deleteRowId));
    }

    public ExchangeNode gatheringExchange(ExchangeNode.Scope scope, PlanNode child)
    {
        return exchange(builder -> builder.type(ExchangeNode.Type.GATHER)
                .scope(scope)
                .singleDistributionPartitioningScheme(child.getOutputSymbols())
                .addSource(child)
                .addInputsSet(child.getOutputSymbols()));
    }

    public SemiJoinNode semiJoin(
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            PlanNode source,
            PlanNode filteringSource)
    {
        return semiJoin(
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            Optional<SemiJoinNode.DistributionType> distributionType)
    {
        return new SemiJoinNode(
                idAllocator.getNextId(),
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType);
    }

    public IndexSourceNode indexSource(
            TableHandle tableHandle,
            Set<Symbol> lookupSymbols,
            List<Symbol> outputSymbols,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> effectiveTupleDomain)
    {
        return new IndexSourceNode(
                idAllocator.getNextId(),
                new IndexHandle(
                        tableHandle.getConnectorId(),
                        TestingConnectorTransactionHandle.INSTANCE,
                        TestingConnectorIndexHandle.INSTANCE),
                tableHandle,
                Optional.empty(),
                lookupSymbols,
                outputSymbols,
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
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE;
        private PartitioningScheme partitioningScheme;
        private OrderingScheme orderingScheme;
        private List<PlanNode> sources = new ArrayList<>();
        private List<List<Symbol>> inputs = new ArrayList<>();

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

        public ExchangeBuilder singleDistributionPartitioningScheme(Symbol... outputSymbols)
        {
            return singleDistributionPartitioningScheme(Arrays.asList(outputSymbols));
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(List<Symbol> outputSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), outputSymbols));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols),
                    Optional.of(hashSymbol)));
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

        public ExchangeBuilder addInputsSet(Symbol... inputs)
        {
            return addInputsSet(Arrays.asList(inputs));
        }

        public ExchangeBuilder addInputsSet(List<Symbol> inputs)
        {
            this.inputs.add(inputs);
            return this;
        }

        public ExchangeBuilder orderingScheme(OrderingScheme orderingScheme)
        {
            this.orderingScheme = orderingScheme;
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs, Optional.ofNullable(orderingScheme));
        }
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.empty(), criteria);
    }

    public JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, Expression filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.of(filter), criteria);
    }

    private JoinNode join(JoinNode.Type joinType, PlanNode left, PlanNode right, Optional<Expression> filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(
                joinType,
                left,
                right,
                ImmutableList.copyOf(criteria),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                filter,
                Optional.empty(),
                Optional.empty());
    }

    public JoinNode join(JoinNode.Type type, PlanNode left, PlanNode right, List<JoinNode.EquiJoinClause> criteria, List<Symbol> outputSymbols, Optional<Expression> filter)
    {
        return join(type, left, right, criteria, outputSymbols, filter, Optional.empty(), Optional.empty());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> outputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol)
    {
        return join(type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.empty());
    }

    public JoinNode join(
            JoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> outputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Optional<JoinNode.DistributionType> distributionType)
    {
        return new JoinNode(idAllocator.getNextId(), type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, distributionType);
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

    public UnionNode union(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        ImmutableList<Symbol> outputs = outputsToInputs.keySet().stream().collect(toImmutableList());
        return new UnionNode(idAllocator.getNextId(), sources, outputsToInputs, outputs);
    }

    public TableWriterNode tableWriter(List<Symbol> columns, List<String> columnNames, PlanNode source)
    {
        return new TableWriterNode(
                idAllocator.getNextId(),
                source,
                new TestingWriterTarget(),
                columns,
                columnNames,
                ImmutableList.of(symbol("partialrows", BIGINT), symbol("fragment", VARBINARY)),
                Optional.empty());
    }

    public Symbol symbol(String name)
    {
        return symbol(name, BIGINT);
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(name);

        Type old = symbols.put(symbol, type);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            symbols.put(symbol, type);
        }

        return symbol;
    }

    public WindowNode window(WindowNode.Specification specification, Map<Symbol, WindowNode.Function> functions, PlanNode source)
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

    public WindowNode window(WindowNode.Specification specification, Map<Symbol, WindowNode.Function> functions, Symbol hashSymbol, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.of(hashSymbol),
                ImmutableSet.of(),
                0);
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(toImmutableList());
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.copyOf(symbols);
    }
}
