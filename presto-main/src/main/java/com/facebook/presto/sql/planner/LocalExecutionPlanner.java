package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.SourceHashProvider;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.operator.Input;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkNotNull;

public class LocalExecutionPlanner
{
    private final Session session;
    private final Metadata metadata;
    private final PlanFragmentSourceProvider sourceProvider;
    private final Map<Symbol, Type> types;

    private final PlanFragmentSource split;
    private final Map<TableHandle, TableScanPlanFragmentSource> tableScans;
    private final OperatorStats operatorStats;

    private final Map<String, ExchangePlanFragmentSource> exchangeSources;

    private final SourceHashProviderFactory joinHashFactory;
    private final DataSize maxOperatorMemoryUsage;

    public LocalExecutionPlanner(Session session, Metadata metadata,
            PlanFragmentSourceProvider sourceProvider,
            Map<Symbol, Type> types,
            PlanFragmentSource split,
            Map<TableHandle, TableScanPlanFragmentSource> tableScans,
            Map<String, ExchangePlanFragmentSource> exchangeSources,
            OperatorStats operatorStats,
            SourceHashProviderFactory joinHashFactory,
            DataSize maxOperatorMemoryUsage)
    {
        this.session = checkNotNull(session, "session is null");
        this.tableScans = tableScans;
        this.operatorStats = Preconditions.checkNotNull(operatorStats, "operatorStats is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sourceProvider = checkNotNull(sourceProvider, "sourceProvider is null");
        this.types = checkNotNull(types, "types is null");
        this.split = split;
        this.exchangeSources = ImmutableMap.copyOf(checkNotNull(exchangeSources, "exchangeSources is null"));
        this.joinHashFactory = checkNotNull(joinHashFactory, "joinHashFactory is null");
        this.maxOperatorMemoryUsage = Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");
    }

    public Operator plan(PlanNode plan)
    {
        return plan.accept(new Visitor(), null);
    }

    private class Visitor
            extends PlanVisitor<Void, Operator>
    {
        @Override
        public Operator visitExchange(ExchangeNode node, Void context)
        {
            int sourceFragmentId = node.getSourceFragmentId();
            ExchangePlanFragmentSource source = exchangeSources.get(String.valueOf(sourceFragmentId));
            Preconditions.checkState(source != null, "Exchange source for fragment %s was not found: available sources %s", sourceFragmentId, exchangeSources.keySet());
            return sourceProvider.createDataStream(source, ImmutableList.<ColumnHandle>of());
        }

        @Override
        public Operator visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource();
            Operator sourceOperator = plan(node.getSource());

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());

            List<Symbol> resultSymbols = Lists.transform(node.getColumnNames(), forMap(node.getAssignments()));
            if (resultSymbols.equals(source.getOutputSymbols())) {
                // no need for a projection -- the output matches the result of the underlying operator
                return sourceOperator;
            }

            List<ProjectionFunction> projections = new ArrayList<>();
            for (Symbol symbol : resultSymbols) {
                ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), symbolToChannelMappings.get(symbol), 0);
                projections.add(function);
            }

            return new FilterAndProjectOperator(sourceOperator, FilterFunctions.TRUE_FUNCTION, projections);
        }

        @Override
        public Operator visitTopN(TopNNode node, Void context)
        {
            Preconditions.checkArgument(node.getOrderBy().size() == 1, "Order by multiple fields not yet supported");
            Symbol orderBySymbol = Iterables.getOnlyElement(node.getOrderBy());

            PlanNode source = node.getSource();

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());

            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), symbolToChannelMappings.get(symbol), 0);
                projections.add(function);
            }

            Ordering<TupleReadable> ordering = Ordering.from(FieldOrderedTupleComparator.INSTANCE);
            if (node.getOrderings().get(orderBySymbol) == SortItem.Ordering.ASCENDING) {
                ordering = ordering.reverse();
            }

            return new TopNOperator(plan(source), (int) node.getCount(), symbolToChannelMappings.get(orderBySymbol), projections, ordering);
        }

        @Override
        public Operator visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource();

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());

            int[] outputChannels = new int[node.getOutputSymbols().size()];
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                outputChannels[i] = symbolToChannelMappings.get(symbol);
            }

            Preconditions.checkArgument(node.getOrderBy().size() == 1, "Order by multiple fields not yet supported");

            Symbol orderBySymbol = Iterables.getOnlyElement(node.getOrderBy());
            int[] sortFields = new int[] { 0 };
            boolean[] sortOrder = new boolean[] { node.getOrderings().get(orderBySymbol) == SortItem.Ordering.ASCENDING};

            return new InMemoryOrderByOperator(plan(source), symbolToChannelMappings.get(orderBySymbol), outputChannels, 1_000_000, sortFields, sortOrder, maxOperatorMemoryUsage);
        }

        @Override
        public Operator visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            return new LimitOperator(plan(source), node.getCount());
        }

        @Override
        public Operator visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            Operator sourceOperator = plan(source);

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());

            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                List<Input> arguments = new ArrayList<>();
                for (Expression argument : entry.getValue().getArguments()) {
                    int channel = symbolToChannelMappings.get(Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName()));
                    int field = 0; // TODO: support composite channels

                    arguments.add(new Input(channel, field));
                }

                FunctionHandle functionHandle = node.getFunctions().get(entry.getKey());
                AggregationFunctionDefinition functionDefinition = metadata.getFunction(functionHandle).bind(arguments);

                functionDefinitions.add(functionDefinition);
            }

            Operator aggregationOperator;
            if (node.getGroupBy().isEmpty()) {
                aggregationOperator = new AggregationOperator(sourceOperator, node.getStep(), functionDefinitions);
            } else {
                Preconditions.checkArgument(node.getGroupBy().size() <= 1, "Only single GROUP BY key supported at this time");
                Symbol groupBySymbol = Iterables.getOnlyElement(node.getGroupBy());
                aggregationOperator = new HashAggregationOperator(sourceOperator,
                        symbolToChannelMappings.get(groupBySymbol),
                        node.getStep(),
                        functionDefinitions,
                        100_000,
                        maxOperatorMemoryUsage);
            }

            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getOutputSymbols().size(); ++i) {
                Symbol symbol = node.getOutputSymbols().get(i);
                projections.add(ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), i, 0));
            }

            return new FilterAndProjectOperator(aggregationOperator, FilterFunctions.TRUE_FUNCTION, projections);
        }

        @Override
        public Operator visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            Operator sourceOperator = plan(source);

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());

            FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), symbolToChannelMappings, metadata, session);

            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), symbolToChannelMappings.get(symbol), 0);
                projections.add(function);
            }

            return new FilterAndProjectOperator(sourceOperator, filter, projections);
        }

        @Override
        public Operator visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            Operator sourceOperator = plan(node.getSource());

            Map<Symbol, Integer> symbolToChannelMappings = mapSymbolsToChannels(source.getOutputSymbols());


            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getExpressions().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(types.get(reference).getRawType(), symbolToChannelMappings.get(symbol), 0);
                }
                else {
                    function = new InterpretedProjectionFunction(types.get(symbol), expression, symbolToChannelMappings, metadata, session);
                }

                projections.add(function);
            }

            return new FilterAndProjectOperator(sourceOperator, FilterFunctions.TRUE_FUNCTION, projections);
        }

        @Override
        public Operator visitTableScan(TableScanNode node, Void context)
        {
            List<ColumnHandle> columns = IterableTransformer.on(node.getAssignments().entrySet())
                    .orderBy(Ordering.explicit(node.getOutputSymbols()).onResultOf(MoreFunctions.<Symbol, ColumnHandle>keyGetter()))
                    .transform(MoreFunctions.<Symbol, ColumnHandle>valueGetter())
                    .list();

            // table scan only works with a split
            Preconditions.checkState(split != null || tableScans != null, "This fragment does not have a split");

            PlanFragmentSource tableSplit = split;
            if (tableSplit == null) {
                tableSplit = tableScans.get(node.getTable());
            }

            Operator operator = sourceProvider.createDataStream(tableSplit, columns);
            return operator;
        }

        @Override
        public Operator visitJoin(JoinNode node, Void context)
        {
            ComparisonExpression comparison = (ComparisonExpression) node.getCriteria();
            Symbol first = Symbol.fromQualifiedName(((QualifiedNameReference) comparison.getLeft()).getName());
            Symbol second = Symbol.fromQualifiedName(((QualifiedNameReference) comparison.getRight()).getName());

            Map<Symbol, Integer> probeMappings = mapSymbolsToChannels(node.getLeft().getOutputSymbols());
            Map<Symbol, Integer> buildMappings = mapSymbolsToChannels(node.getRight().getOutputSymbols());

            int probeChannel;
            int buildChannel;
            if (node.getLeft().getOutputSymbols().contains(first)) {
                probeChannel = probeMappings.get(first);
                buildChannel = buildMappings.get(second);
            }
            else {
                probeChannel = probeMappings.get(second);
                buildChannel = buildMappings.get(first);
            }

            SourceHashProvider hashProvider = joinHashFactory.getSourceHashProvider(node, LocalExecutionPlanner.this, buildChannel, operatorStats);
            Operator leftOperator = plan(node.getLeft());
            HashJoinOperator operator = new HashJoinOperator(hashProvider, leftOperator, probeChannel);
            return operator;
        }

        @Override
        protected Operator visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    private Map<Symbol, Integer> mapSymbolsToChannels(List<Symbol> outputs)
    {
        Map<Symbol, Integer> symbolToChannelMappings = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            symbolToChannelMappings.put(outputs.get(i), i);
        }
        return symbolToChannelMappings;
    }
}
