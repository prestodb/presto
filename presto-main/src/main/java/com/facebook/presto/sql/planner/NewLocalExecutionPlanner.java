package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.InMemoryExchange;
import com.facebook.presto.noperator.InMemoryExchangeSourceOperator.InMemoryExchangeSourceOperatorFactory;
import com.facebook.presto.noperator.NewAggregationOperator.NewAggregationOperatorFactory;
import com.facebook.presto.noperator.NewExchangeOperator.NewExchangeOperatorFactory;
import com.facebook.presto.noperator.NewFilterAndProjectOperator.NewFilterAndProjectOperatorFactory;
import com.facebook.presto.noperator.NewHashAggregationOperator.NewHashAggregationOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashProvider;
import com.facebook.presto.noperator.NewHashJoinOperator;
import com.facebook.presto.noperator.NewHashJoinOperator.NewHashJoinOperatorFactory;
import com.facebook.presto.noperator.NewHashSemiJoinOperator.NewHashSemiJoinOperatorFactory;
import com.facebook.presto.noperator.NewInMemoryOrderByOperator.NewInMemoryOrderByOperatorFactory;
import com.facebook.presto.noperator.NewInMemoryWindowOperator.NewInMemoryWindowOperatorFactory;
import com.facebook.presto.noperator.NewLimitOperator.NewLimitOperatorFactory;
import com.facebook.presto.noperator.NewOperatorFactory;
import com.facebook.presto.noperator.NewSetBuilderOperator.NewSetBuilderOperatorFactory;
import com.facebook.presto.noperator.NewSetBuilderOperator.NewSetSupplier;
import com.facebook.presto.noperator.NewTableScanOperator.NewTableScanOperatorFactory;
import com.facebook.presto.noperator.NewTableWriterOperator.NewTableWriterOperatorFactory;
import com.facebook.presto.noperator.NewTopNOperator.NewTopNOperatorFactory;
import com.facebook.presto.noperator.OutputFactory;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.tree.Input.fieldGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

public class NewLocalExecutionPlanner
{
    private static final Logger log = Logger.get(NewLocalExecutionPlanner.class);

    private final NodeInfo nodeInfo;
    private final Metadata metadata;

    private final DataStreamProvider dataStreamProvider;
    private final LocalStorageManager storageManager;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final ExpressionCompiler compiler;

    @Inject
    public NewLocalExecutionPlanner(NodeInfo nodeInfo,
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            LocalStorageManager storageManager,
            Supplier<ExchangeClient> exchangeClientSupplier,
            ExpressionCompiler compiler)
    {
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.compiler = checkNotNull(compiler, "compiler is null");
    }

    public NewLocalExecutionPlan plan(Session session,
            PlanNode plan,
            Map<Symbol, Type> types,
            OutputFactory outputOperatorFactory)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(session, types);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(), context);
        DriverFactory driverFactory = new DriverFactory(ImmutableList.<NewOperatorFactory>builder()
                .addAll(physicalOperation.getOperatorFactories())
                .add(outputOperatorFactory.createOutputOperator(physicalOperation.getTupleInfos()))
                .build());
        context.addDriverFactory(driverFactory);

        return new NewLocalExecutionPlan(context.getDriverFactories());
    }

    private static class LocalExecutionPlanContext
    {
        private final Session session;
        private final Map<Symbol, Type> types;

        private final List<DriverFactory> driverFactories = new ArrayList<>();

        public LocalExecutionPlanContext(Session session, Map<Symbol, Type> types)
        {
            this.session = session;
            this.types = types;
        }

        public void addDriverFactory(DriverFactory driverFactory)
        {
            driverFactories.add(checkNotNull(driverFactory, "driverFactory is null"));
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public Session getSession()
        {
            return session;
        }

        public Map<Symbol, Type> getTypes()
        {
            return types;
        }
    }

    public static class NewLocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;

        public NewLocalExecutionPlan(List<DriverFactory> driverFactories)
        {
            this.driverFactories = ImmutableList.copyOf(checkNotNull(driverFactories, "driverFactories is null"));
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }
    }

    private class Visitor
            extends PlanVisitor<LocalExecutionPlanContext, PhysicalOperation>
    {
        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());

            NewOperatorFactory operatorFactory = new NewExchangeOperatorFactory(node.getId(), exchangeClientSupplier, tupleInfos);

            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // see if we need to introduce a projection
            //   1. verify that there's one symbol per channel
            //   2. verify that symbols from "source" match the expected order of columns according to OutputNode
            Ordering<Input> comparator = inputOrdering();

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entries())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list();

            List<Symbol> resultSymbols = node.getOutputSymbols();
            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getTupleInfos().size()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), context.getTypes());

            NewOperatorFactory operatorFactory = new NewFilterAndProjectOperatorFactory(FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();

            // sort by PARTITION BY, then by ORDER BY
            List<Symbol> orderingSymbols = ImmutableList.copyOf(Iterables.concat(partitionBySymbols, orderBySymbols));

            // insert a projection to put all the sort fields in a single channel if necessary
            if (!orderingSymbols.isEmpty()) {
                source = packIfNecessary(orderingSymbols, source, context.getTypes());
            }

            // find channel that fields were packed into if there is an ordering
            int orderByChannel = 0;
            if (!orderingSymbols.isEmpty()) {
                orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderingSymbols, source.getLayout()));
            }

            int[] partitionFields = new int[partitionBySymbols.size()];
            for (int i = 0; i < partitionFields.length; i++) {
                Symbol symbol = partitionBySymbols.get(i);
                partitionFields[i] = getFirst(source.getLayout().get(symbol)).getField();
            }

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortFields[i] = getFirst(source.getLayout().get(symbol)).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getTupleInfos().size()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            ImmutableList.Builder<WindowFunction> windowFunctions = ImmutableList.builder();
            List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                FunctionHandle handle = node.getFunctionHandles().get(symbol);
                windowFunctions.add(metadata.getFunction(handle).getWindowFunction().get());
                windowFunctionOutputSymbols.add(symbol);
            }

            // compute the layout of the output from the window operator
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.putAll(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTupleInfos().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            NewOperatorFactory operatorFactory = new NewInMemoryWindowOperatorFactory(
                    source.getTupleInfos(),
                    orderByChannel,
                    outputChannels,
                    windowFunctions.build(),
                    partitionFields,
                    sortFields,
                    sortOrder,
                    1_000_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // insert a projection to put all the sort fields in a single channel if necessary
            source = packIfNecessary(orderBySymbols, source, context.getTypes());

            int orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderBySymbols, source.getLayout()));

            List<Integer> sortFields = new ArrayList<>();
            List<SortItem.Ordering> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortFields.add(getFirst(source.getLayout().get(symbol)).getField());
                sortOrders.add(node.getOrderings().get(symbol));
            }

            Ordering<TupleReadable> ordering = Ordering.from(new FieldOrderedTupleComparator(sortFields, sortOrders));

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());

            NewOperatorFactory operator = new NewTopNOperatorFactory(
                    (int) node.getCount(),
                    orderByChannel,
                    mappings.getProjections(),
                    ordering,
                    node.isPartial());

            return new PhysicalOperation(operator, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // insert a projection to put all the sort fields in a single channel if necessary
            source = packIfNecessary(orderBySymbols, source, context.getTypes());

            int orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderBySymbols, source.getLayout()));

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);

                sortFields[i] = getFirst(source.getLayout().get(symbol)).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getTupleInfos().size()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            NewOperatorFactory operator = new NewInMemoryOrderByOperatorFactory(
                    source.getTupleInfos(),
                    orderByChannel,
                    outputChannels,
                    10_000,
                    sortFields,
                    sortOrder);

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            NewOperatorFactory operatorFactory = new NewLimitOperatorFactory(source.getTupleInfos(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupBy().isEmpty()) {
                return planGlobalAggregation(node, source);
            }

            return planGroupByAggregation(node, source, context);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Input, Type> inputTypes = getInputTypes(source.getLayout(), source.getTupleInfos());

            FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
            NewOperatorFactory operatorFactory = new NewFilterAndProjectOperatorFactory(filter, mappings.getProjections());
            return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            // collapse project on filter to a single operator
            PhysicalOperation source;
            Expression filterExpression;
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                source = filterNode.getSource().accept(this, context);
                filterExpression = filterNode.getPredicate();
            }
            else {
                source = node.getSource().accept(this, context);
                filterExpression = BooleanLiteral.TRUE_LITERAL;
            }

            List<Expression> expressions = node.getExpressions();
            Map<Input, Type> inputTypes = null;
            FilterFunction filter;
            if (filterExpression != BooleanLiteral.TRUE_LITERAL) {
                filter = new InterpretedFilterFunction(filterExpression, convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
            }
            else {
                filter = FilterFunctions.TRUE_FUNCTION;
            }

            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < expressions.size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                Expression expression = expressions.get(i);

                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(context.getTypes().get(reference).getRawType(), getFirst(source.getLayout().get(symbol)));
                }
                else {
                    function = new InterpretedProjectionFunction(context.getTypes().get(symbol), expression, convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
                }
                projections.add(function);

                outputMappings.put(symbol, new Input(i, 0)); // one field per channel
            }

            NewOperatorFactory operatorFactory = new NewFilterAndProjectOperatorFactory(filter, projections);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private Map<Input, Type> getInputTypes(Multimap<Symbol, Input> layout, List<TupleInfo> tupleInfos)
        {
            Builder<Input, Type> inputTypes = ImmutableMap.builder();
            for (Input input : ImmutableSet.copyOf(layout.values())) {
                TupleInfo.Type type = tupleInfos.get(input.getChannel()).getTypes().get(input.getField());
                switch (type) {
                    case BOOLEAN:
                        inputTypes.put(input, Type.BOOLEAN);
                        break;
                    case FIXED_INT_64:
                        inputTypes.put(input, Type.LONG);
                        break;
                    case VARIABLE_BINARY:
                        inputTypes.put(input, Type.STRING);
                        break;
                    case DOUBLE:
                        inputTypes.put(input, Type.DOUBLE);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
            return inputTypes.build();
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                outputMappings.put(symbol, new Input(channel, 0)); // one column per channel
                channel++;
            }

            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());
            NewOperatorFactory operatorFactory = new NewTableScanOperatorFactory(node.getId(), dataStreamProvider, tupleInfos, columns);
            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // introduce a projection to put all fields from the left side into a single channel if necessary
            PhysicalOperation leftSource = node.getLeft().accept(this, context);
            List<Symbol> leftSymbols = Lists.transform(clauses, leftGetter());

            // do the same on the right side
            PhysicalOperation rightSource = node.getRight().accept(this, context);
            List<Symbol> rightSymbols = Lists.transform(clauses, rightGetter());

            switch (node.getType())
            {
                case INNER:
                case LEFT:
                    return createJoinOperator(node, leftSource, leftSymbols, rightSource, rightSymbols, context);
                case RIGHT:
                    return createJoinOperator(node, rightSource, rightSymbols, leftSource, leftSymbols, context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private PhysicalOperation createJoinOperator(JoinNode node, PhysicalOperation probeSource, List<Symbol> probeSymbols, PhysicalOperation buildSource, List<Symbol> buildSymbols, LocalExecutionPlanContext context)
        {
            // introduce a projection to put all fields from the probe side into a single channel if necessary
            probeSource = packIfNecessary(probeSymbols, probeSource, context.getTypes());

            // do the same on the build side
            buildSource = packIfNecessary(buildSymbols, buildSource, context.getTypes());

            int probeChannel = Iterables.getOnlyElement(getChannelSetForSymbols(probeSymbols, probeSource.getLayout()));
            int buildChannel = Iterables.getOnlyElement(getChannelSetForSymbols(buildSymbols, buildSource.getLayout()));

            NewHashBuilderOperatorFactory hashBuilderOperatorFactory = new NewHashBuilderOperatorFactory(buildSource.getTupleInfos(), buildChannel, 100_000);
            NewHashProvider hashProvider = hashBuilderOperatorFactory.getHashProvider();
            DriverFactory buildDriverFactory = new DriverFactory(ImmutableList.<NewOperatorFactory>builder()
                    .addAll(buildSource.getOperatorFactories())
                    .add(hashBuilderOperatorFactory)
                    .build());
            context.addDriverFactory(buildDriverFactory);

            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTupleInfos().size();
            for (Map.Entry<Symbol, Input> entry : buildSource.getLayout().entries()) {
                Input input = entry.getValue();
                outputMappings.put(entry.getKey(), new Input(offset + input.getChannel(), input.getField()));
            }

            NewOperatorFactory operator = createJoinOperator(node.getType(), hashProvider, probeSource.getTupleInfos(), probeChannel);
            return new PhysicalOperation(operator, outputMappings.build(), probeSource);
        }

        private NewHashJoinOperatorFactory createJoinOperator(JoinNode.Type type, NewHashProvider hashProvider, List<TupleInfo> probeTupleInfos, int probeJoinChannel)
        {
            switch (type) {
                case INNER:
                    return NewHashJoinOperator.innerJoin(hashProvider, probeTupleInfos, probeJoinChannel);
                case LEFT:
                case RIGHT:
                    return NewHashJoinOperator.outerJoin(hashProvider, probeTupleInfos, probeJoinChannel);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + type);
            }
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getSource().accept(this, context);
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, context);

            // introduce a projection to put all fields from the probe side into a single channel if necessary
            probeSource = packIfNecessary(ImmutableList.of(node.getSourceJoinSymbol()), probeSource, context.getTypes());

            // do the same on the build side
            buildSource = packIfNecessary(ImmutableList.of(node.getFilteringSourceJoinSymbol()), buildSource, context.getTypes());

            int probeChannel = Iterables.getOnlyElement(getChannelSetForSymbols(ImmutableList.of(node.getSourceJoinSymbol()), probeSource.getLayout()));
            int buildChannel = Iterables.getOnlyElement(getChannelSetForSymbols(ImmutableList.of(node.getFilteringSourceJoinSymbol()), buildSource.getLayout()));

            NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(buildSource.getTupleInfos(), buildChannel, 100_000);
            NewSetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            DriverFactory buildDriverFactory = new DriverFactory(ImmutableList.<NewOperatorFactory>builder()
                    .addAll(buildSource.getOperatorFactories())
                    .add(setBuilderOperatorFactory)
                    .build());
            context.addDriverFactory(buildDriverFactory);

            // Source channels are always laid out first, followed by the boolean output symbol
            ImmutableMultimap<Symbol, Input> outputMappings = ImmutableMultimap.<Symbol, Input>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), new Input(probeSource.getLayout().size(), 0))
                    .build();

            NewHashSemiJoinOperatorFactory operator = new NewHashSemiJoinOperatorFactory(setProvider, probeSource.getTupleInfos(), probeChannel);
            return new PhysicalOperation(operator, outputMappings, probeSource);
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entries())
                    .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list()
                    .equals(node.getOutputSymbols());

            // if any symbols are mapped to a non-zero field, re-map to one field per channel
            // TODO: this is currently what the exchange operator expects -- figure out how to remove this assumption
            // to avoid unnecessary projections
            boolean hasMultiFieldChannels = IterableTransformer.on(source.getLayout().values())
                    .transform(fieldGetter())
                    .any(not(equalTo(0)));

            if (hasMultiFieldChannels || !projectionMatchesOutput) {
                IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
                NewOperatorFactory operatorFactory = new NewFilterAndProjectOperatorFactory(FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
                // NOTE: the generated output layout may not be completely accurate if the same field was projected as multiple inputs.
                // However, this should not affect the operation of the sink.
                return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
            }

            return source;
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation query = node.getSource().accept(this, context);

            ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getColumns().entrySet()) {
                symbols.add(entry.getKey());
                columns.add(entry.getValue());
            }

            // introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(symbols.build(), query.getLayout(), context.getTypes());
            NewOperatorFactory sourceOperator = new NewFilterAndProjectOperatorFactory(FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            PhysicalOperation source = new PhysicalOperation(sourceOperator, mappings.getOutputLayout(), query);

            Symbol outputSymbol = Iterables.getOnlyElement(node.getOutputSymbols());
            NewTableWriterOperatorFactory operator = new NewTableWriterOperatorFactory(
                    node.getId(),
                    storageManager,
                    nodeInfo.getNodeId(),
                    columns.build());

            return new PhysicalOperation(operator, ImmutableMultimap.of(outputSymbol, new Input(0, 0)), source);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());
            InMemoryExchange inMemoryExchange = new InMemoryExchange(tupleInfos);

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                List<Symbol> expectedLayout = node.sourceOutputLayout(i);

                PhysicalOperation source = subplan.accept(this, context);
                List<NewOperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entries())
                        .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                        .transform(MoreFunctions.<Symbol, Input>keyGetter())
                        .list()
                        .equals(expectedLayout);

                if (!projectionMatchesOutput) {
                    IdentityProjectionInfo mappings = computeIdentityMapping(expectedLayout, source.getLayout(), context.getTypes());
                    operatorFactories.add(new NewFilterAndProjectOperatorFactory(FilterFunctions.TRUE_FUNCTION, mappings.getProjections()));
                }

                operatorFactories.add(inMemoryExchange.createSinkFactory());

                DriverFactory driverFactory = new DriverFactory(operatorFactories);
                context.addDriverFactory(driverFactory);
            }
            inMemoryExchange.noMoreSinkFactories();

            // Fow now, we assume that subplans always produce one symbol per channel. TODO: remove this assumption
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(new InMemoryExchangeSourceOperatorFactory(inMemoryExchange), outputMappings.build());
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<TupleInfo> getSourceOperatorTupleInfos(PlanNode node, Map<Symbol, Type> types)
        {
            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            return ImmutableList.copyOf(IterableTransformer.on(node.getOutputSymbols())
                    .transform(Functions.forMap(types))
                    .transform(Type.toRaw())
                    .transform(new Function<TupleInfo.Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(TupleInfo.Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());
        }

        private AggregationFunctionDefinition buildFunctionDefinition(PhysicalOperation source, FunctionHandle function, FunctionCall call)
        {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(getFirst(source.getLayout().get(argumentSymbol)));
            }

            return metadata.getFunction(function).bind(arguments);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                outputMappings.put(symbol, new Input(outputChannel, 0)); // one aggregation per channel
                outputChannel++;
            }

            NewOperatorFactory operatorFactory = new NewAggregationOperatorFactory(node.getStep(), functionDefinitions);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            // introduce a projection to put all group by fields from the source into a single channel if necessary
            source = packIfNecessary(groupBySymbols, source, context.getTypes());

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            // add group-by key fields. They all go in channel 0 in the same order produced by the source operator
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, new Input(0, getFirst(source.getLayout().get(symbol)).getField()));
            }

            // aggregations go in remaining channels starting at 1, one per channel
            int channel = 1;
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Integer groupByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(groupBySymbols, source.getLayout()));
            NewOperatorFactory operatorFactory = new NewHashAggregationOperatorFactory(
                    source.getTupleInfos().get(groupByChannel),
                    groupByChannel,
                    node.getStep(),
                    functionDefinitions,
                    10_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Multimap<Symbol, Input> inputLayout, Map<Symbol, Type> types)
    {
        ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), getFirst(inputLayout.get(symbol)));
            projections.add(function);
            outputMappings.put(symbol, new Input(channel, 0)); // one field per channel
            channel++;
        }

        return new IdentityProjectionInfo(outputMappings.build(), projections);
    }

    /**
     * Inserts a projection if the provided symbols are not in a single channel by themselves
     */
    private PhysicalOperation packIfNecessary(List<Symbol> symbols, PhysicalOperation source, Map<Symbol, Type> types)
    {
        List<Integer> channels = getChannelsForSymbols(symbols, source.getLayout());
        List<TupleInfo> tupleInfos = source.getTupleInfos();
        if (channels.size() > 1 || tupleInfos.get(Iterables.getOnlyElement(channels)).getFieldCount() > 1) {
            source = pack(source, symbols, types);
        }
        return source;
    }

    /**
     * Inserts a Projection that places the requested symbols into the same channel in the order specified
     */
    private static PhysicalOperation pack(PhysicalOperation source, List<Symbol> symbols, Map<Symbol, Type> types)
    {
        checkArgument(!symbols.isEmpty(), "symbols is empty");

        List<Symbol> otherSymbols = ImmutableList.copyOf(Sets.difference(source.getLayout().keySet(), ImmutableSet.copyOf(symbols)));

        // split composite channels into one field per channel. TODO: Fix it so that it preserves the layout of channels for "otherSymbols"
        IdentityProjectionInfo mappings = computeIdentityMapping(otherSymbols, source.getLayout(), types);

        ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
        ImmutableList.Builder<ProjectionFunction> projections = ImmutableList.builder();

        outputMappings.putAll(mappings.getOutputLayout());
        projections.addAll(mappings.getProjections());

        // append a projection that packs all the input symbols into a single channel (it goes in the last channel)
        List<ProjectionFunction> packedProjections = new ArrayList<>();
        int channel = mappings.getProjections().size();
        int field = 0;
        for (Symbol symbol : symbols) {
            packedProjections.add(ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), getFirst(source.getLayout().get(symbol))));
            outputMappings.put(symbol, new Input(channel, field));
            field++;
        }
        projections.add(ProjectionFunctions.concat(packedProjections));

        NewOperatorFactory operatorFactory = new NewFilterAndProjectOperatorFactory(FilterFunctions.TRUE_FUNCTION, projections.build());
        return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Multimap<Symbol, Input> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(getFirst(layout.get(symbol)).getChannel());
        }
        return builder.build();
    }

    private static Set<Integer> getChannelSetForSymbols(List<Symbol> symbols, Multimap<Symbol, Input> layout)
    {
        return ImmutableSet.copyOf(getChannelsForSymbols(symbols, layout));
    }

    private static Map<Symbol, Input> convertLayoutToInputMap(Multimap<Symbol, Input> layout)
    {
        Builder<Symbol, Input> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Input>> entry : layout.asMap().entrySet()) {
            builder.put(entry.getKey(), getFirst(entry.getValue()));
        }
        return builder.build();
    }

    private static <T> T getFirst(Iterable<T> iterable)
    {
        return iterable.iterator().next();
    }

    private static class IdentityProjectionInfo
    {
        private final Multimap<Symbol, Input> layout;
        private final List<ProjectionFunction> projections;

        public IdentityProjectionInfo(Multimap<Symbol, Input> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = checkNotNull(outputLayout, "outputLayout is null");
            this.projections = checkNotNull(projections, "projections is null");
        }

        public Multimap<Symbol, Input> getOutputLayout()
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
        private final List<NewOperatorFactory> operatorFactories;
        private final Multimap<Symbol, Input> layout;
        private final List<TupleInfo> tupleInfos;

        public PhysicalOperation(NewOperatorFactory operatorFactory, Multimap<Symbol, Input> layout)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");

            this.operatorFactories = ImmutableList.of(operatorFactory);
            this.layout = ImmutableMultimap.copyOf(layout);
            this.tupleInfos = operatorFactory.getTupleInfos();
        }

        public PhysicalOperation(NewOperatorFactory operatorFactory, Multimap<Symbol, Input> layout, PhysicalOperation source)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");
            checkNotNull(source, "source is null");

            this.operatorFactories = ImmutableList.<NewOperatorFactory>builder().addAll(source.getOperatorFactories()).add(operatorFactory).build();
            this.layout = ImmutableMultimap.copyOf(layout);
            this.tupleInfos = operatorFactory.getTupleInfos();
        }

        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        public Multimap<Symbol, Input> getLayout()
        {
            return layout;
        }

        private List<NewOperatorFactory> getOperatorFactories()
        {
            return operatorFactories;
        }
    }

    private static Ordering<Input> inputOrdering()
    {
        return Ordering.from(new Comparator<Input>()
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
    }
}
