package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.Input;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.SourceHashProvider;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.TopNOperator;
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
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.operator.Input.channelGetter;
import static com.facebook.presto.operator.Input.fieldGetter;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

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
        return plan.accept(new Visitor(), null).getOperator();
    }

    private class Visitor
            extends PlanVisitor<Void, PhysicalOperation>
    {
        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, Void context)
        {
            int sourceFragmentId = node.getSourceFragmentId();
            ExchangePlanFragmentSource source = exchangeSources.get(String.valueOf(sourceFragmentId));
            Preconditions.checkState(source != null, "Exchange source for fragment %s was not found: available sources %s", sourceFragmentId, exchangeSources.keySet());

            Operator operator = sourceProvider.createDataStream(source, ImmutableList.<ColumnHandle>of());

            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            Map<Symbol, Input> outputMappings = new HashMap<>();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(operator, outputMappings);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> resultSymbols = Lists.transform(node.getColumnNames(), forMap(node.getAssignments()));


            // see if we need to introduce a projection
            //   1. verify that there's one symbol per channel
            //   2. verify that symbols from "source" match the expected order of columns according to OutputNode
            Ordering<Input> comparator = Ordering.from(new Comparator<Input>()
            {
                @Override
                public int compare(Input o1, Input o2)
                {
                    return ComparisonChain.start()
                            .compare(o1.getChannel(), o2.getChannel())
                            .compare(o1.getField(), o2.getField())
                            .result();
                }
            });

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list();

            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getOperator().getChannelCount()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), types);

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            Preconditions.checkArgument(orderBySymbols.size() == 1, "ORDER BY multiple fields + LIMIT not yet supported"); // TODO

            Symbol orderBySymbol = Iterables.getOnlyElement(orderBySymbols);
            int keyChannel = source.getLayout().get(orderBySymbol).getChannel();

            Ordering<TupleReadable> ordering = Ordering.from(FieldOrderedTupleComparator.INSTANCE);
            if (node.getOrderings().get(orderBySymbol) == SortItem.Ordering.ASCENDING) {
                ordering = ordering.reverse();
            }

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), types);

            TopNOperator operator = new TopNOperator(source.getOperator(), (int) node.getCount(), keyChannel, mappings.getProjections(), ordering);
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // see if the order-by fields are in the same channel
            Map<Symbol, Input> layout = source.getLayout();
            Set<Integer> channels = getChannelsForSymbols(orderBySymbols, layout);

            if (channels.size() > 1) {
                // insert a projection to pack the order-by fields into the same channel
                source = pack(source, orderBySymbols, types);

                // get the channels where the orderBySymbols were placed
                channels = IterableTransformer.on(orderBySymbols)
                        .transform(forMap(source.getLayout()))
                        .transform(channelGetter())
                        .set();
            }

            int orderByChannel = Iterables.getOnlyElement(channels);

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);

                sortFields[i] = source.getLayout().get(symbol).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }


            int[] outputChannels = new int[source.getOperator().getChannelCount()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            Operator operator = new InMemoryOrderByOperator(source.getOperator(), orderByChannel, outputChannels, 1_000_000, sortFields, sortOrder, maxOperatorMemoryUsage);
            return new PhysicalOperation(operator, source.getLayout());
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            return new PhysicalOperation(new LimitOperator(source.getOperator(), node.getCount()), source.getLayout());
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupBy().isEmpty()) {
                return planGlobalAggregation(node, source);
            }

            return planGroupByAggregation(node, source);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), source.getLayout(), metadata, session);

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), types);

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), filter, mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<Symbol, Input> outputMappings = new HashMap<>();
            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getExpressions().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(types.get(reference).getRawType(), source.getLayout().get(symbol));
                }
                else {
                    function = new InterpretedProjectionFunction(types.get(symbol), expression, source.getLayout(), metadata, session);
                }
                projections.add(function);

                outputMappings.put(symbol, new Input(i, 0)); // one field per channel
            }

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, projections);
            return new PhysicalOperation(operator, outputMappings);
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, Void context)
        {
            // table scan only works with a split
            Preconditions.checkState(split != null || tableScans != null, "This fragment does not have a split");

            PlanFragmentSource tableSplit = split;
            if (tableSplit == null) {
                tableSplit = tableScans.get(node.getTable());
            }

            Map<Symbol, Input> mappings = new HashMap<>();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                mappings.put(symbol, new Input(channel, 0)); // one column per channel
                channel++;
            }

            Operator operator = sourceProvider.createDataStream(tableSplit, columns);
            return new PhysicalOperation(operator, mappings);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, Void context)
        {
            Preconditions.checkArgument(node.getCriteria().size() == 1, "Joining by multiple conditions not yet supported");

            JoinNode.EquiJoinClause clause = Iterables.getOnlyElement(node.getCriteria());

            PhysicalOperation leftSource = node.getLeft().accept(this, context);
            int probeChannel = leftSource.getLayout().get(clause.getLeft()).getChannel();

            PhysicalOperation rightSource = node.getRight().accept(this, context);
            int buildChannel = rightSource.getLayout().get(clause.getRight()).getChannel();

            Preconditions.checkState(leftSource.getOperator().getTupleInfos().get(probeChannel).getFieldCount() == 1 &&
                    rightSource.getOperator().getTupleInfos().get(buildChannel).getFieldCount() == 1,
                    "JOIN with the results of a multi-field GROUP BY, DISTINCT or ORDER BY not yet supported");

            SourceHashProvider hashProvider = joinHashFactory.getSourceHashProvider(node, rightSource.getOperator(), buildChannel, operatorStats);

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(leftSource.getLayout());

            // inputs from right side of the join are laid out following the input from the left side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = leftSource.getOperator().getChannelCount();
            for (Map.Entry<Symbol, Input> entry : rightSource.getLayout().entrySet()) {
                Input input = entry.getValue();
                outputMappings.put(entry.getKey(), new Input(offset + input.getChannel(), input.getField()));
            }

            HashJoinOperator operator = new HashJoinOperator(hashProvider, leftSource.getOperator(), probeChannel);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, Void context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // if any symbols are mapped to a non-zero field, re-map to one field per channel
            // TODO: this is currently what the exchange operator expects -- figure out how to remove this assumption
            // to avoid unnecessary projections
            boolean needsProjection = IterableTransformer.on(source.getLayout().values())
                    .transform(fieldGetter())
                    .any(not(equalTo(0)));

            if (needsProjection) {
                return projectToOneFieldPerChannel(source, types);
            }

            return source;
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private AggregationFunctionDefinition buildFunctionDefinition(PhysicalOperation source, FunctionHandle function, FunctionCall call)
        {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(source.getLayout().get(argumentSymbol));
            }

            return metadata.getFunction(function).bind(arguments);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                outputMappings.put(symbol, new Input(outputChannel, 0)); // one aggregation per channel
                outputChannel++;
            }

            Operator operator = new AggregationOperator(source.getOperator(), node.getStep(), functionDefinitions);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            // see if the group-by fields are in the same channel and are the only fields in that channel
            // first, compute the unique set of channels
            Set<Integer> channels = getChannelsForSymbols(groupBySymbols, source.getLayout());

            boolean needsProjection = true;

            // if there's more than one channel, we need to pack the group-by fields into the same channel
            if (channels.size() == 1) {
                int channel = Iterables.getOnlyElement(channels);

                // otherwise, verify that the only the group by symbols are in the group-by channel
                TupleInfo channelTupleInfo = source.getOperator().getTupleInfos().get(channel);
                if (channelTupleInfo.getFieldCount() == groupBySymbols.size()) {
                    needsProjection = false;
                }
            }

            // insert a projection that packs all the group-by fields into the same channel if necessary
            if (needsProjection) {
                source = pack(source, groupBySymbols, types);

                // get the channels where the groupBySymbols were placed
                channels = IterableTransformer.on(groupBySymbols)
                        .transform(forMap(source.getLayout()))
                        .transform(channelGetter())
                        .set();
            }

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            // add group-by key fields. They all go in channel 0 in the same order produced by the source operator
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, new Input(0, source.getLayout().get(symbol).getField()));
            }

            // aggregations go in remaining channels starting at 1, one per channel
            int channel = 1;
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Operator aggregationOperator = new HashAggregationOperator(source.getOperator(),
                    Iterables.getOnlyElement(channels),
                    node.getStep(),
                    functionDefinitions,
                    100_000,
                    maxOperatorMemoryUsage);

            return new PhysicalOperation(aggregationOperator, outputMappings.build());
        }
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Map<Symbol, Input> inputLayout, Map<Symbol, Type> types)
    {
        Map<Symbol, Input> outputLayout = new HashMap<>();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), inputLayout.get(symbol));
            projections.add(function);
            outputLayout.put(symbol, new Input(channel, 0)); // one field per channel
            channel++;
        }

        return new IdentityProjectionInfo(outputLayout, projections);
    }

    /**
     * Inserts a Projection that places the requested symbols into the same channel in the order specified
     */
    private static PhysicalOperation pack(PhysicalOperation source, List<Symbol> symbols, Map<Symbol, Type> types)
    {
        Preconditions.checkArgument(!symbols.isEmpty(), "symbols is empty");

        List<Symbol> otherSymbols = ImmutableList.copyOf(Sets.difference(source.getLayout().keySet(), ImmutableSet.copyOf(symbols)));

        // split composite channels into one field per channel. TODO: Fix it so that it preserves the layout of channels for "otherSymbols"
        IdentityProjectionInfo mappings = computeIdentityMapping(otherSymbols, source.getLayout(), types);

        ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
        ImmutableList.Builder<ProjectionFunction> projections = ImmutableList.builder();

        outputMappings.putAll(mappings.getOutputLayout());
        projections.addAll(mappings.getProjections());

        // append a projection that packs all the input symbols into a single channel (it goes in the last channel)
        List<ProjectionFunction> packedProjections = new ArrayList<>();
        int channel = mappings.getProjections().size();
        int field = 0;
        for (Symbol symbol : symbols) {
            packedProjections.add(ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), source.getLayout().get(symbol)));
            outputMappings.put(symbol, new Input(channel, field));
            field++;
        }
        projections.add(ProjectionFunctions.concat(packedProjections));

        Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, projections.build());
        return new PhysicalOperation(operator, outputMappings.build());
    }

    private static PhysicalOperation projectToOneFieldPerChannel(PhysicalOperation source, Map<Symbol, Type> types)
    {
        List<Symbol> symbols = ImmutableList.copyOf(source.getLayout().keySet());
        IdentityProjectionInfo mappings = computeIdentityMapping(symbols, source.getLayout(), types);

        Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
        return new PhysicalOperation(operator, mappings.getOutputLayout());
    }

    private static Set<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Input> layout)
    {
        return IterableTransformer.on(symbols)
                .transform(Functions.forMap(layout))
                .transform(Input.channelGetter())
                .set();
    }

    private static class IdentityProjectionInfo
    {
        private Map<Symbol, Input> layout = new HashMap<>();
        private List<ProjectionFunction> projections = new ArrayList<>();

        public IdentityProjectionInfo(Map<Symbol, Input> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = outputLayout;
            this.projections = projections;
        }

        public Map<Symbol, Input> getOutputLayout()
        {
            return layout;
        }

        public List<ProjectionFunction> getProjections()
        {
            return projections;
        }
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private Operator operator;
        private Map<Symbol, Input> layout;

        public PhysicalOperation(Operator operator, Map<Symbol, Input> layout)
        {
            checkNotNull(operator, "operator is null");
            Preconditions.checkNotNull(layout, "layout is null");

            this.operator = operator;
            this.layout = layout;
        }

        public Operator getOperator()
        {
            return operator;
        }

        public Map<Symbol, Input> getLayout()
        {
            return layout;
        }
    }
}
