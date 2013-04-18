package com.facebook.presto.split;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.ImportTableHandle;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MapTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.InetAddresses;
import com.google.inject.Inject;
import org.weakref.jmx.Managed;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.metadata.ImportColumnHandle.columnNameGetter;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpression.matchesPattern;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.or;
import static com.google.common.collect.Maps.uniqueIndex;

public class SplitManager
{
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final ImportClientManager importClientManager;
    private final Metadata metadata;

    private final AtomicLong scheduleLocal = new AtomicLong();
    private final AtomicLong scheduleRack = new AtomicLong();
    private final AtomicLong scheduleRandom = new AtomicLong();

    @Inject
    public SplitManager(NodeManager nodeManager, ShardManager shardManager, ImportClientManager importClientManager, Metadata metadata)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Managed
    public long getScheduleLocal()
    {
        return scheduleLocal.get();
    }

    @Managed
    public long getScheduleRack()
    {
        return scheduleRack.get();
    }

    @Managed
    public long getScheduleRandom()
    {
        return scheduleRandom.get();
    }

    @Managed
    public void reset()
    {
        scheduleLocal.set(0);
        scheduleRack.set(0);
        scheduleRandom.set(0);
    }

    public Iterable<SplitAssignments> getSplitAssignments(PlanNodeId planNodeId,
            Session session,
            TableHandle handle,
            Expression predicate,
            Predicate<PartitionInfo> partitionPredicate,
            Map<Symbol, ColumnHandle> mappings)
    {
        checkNotNull(planNodeId, "planNodeId is null");
        checkNotNull(session, "session is null");
        checkNotNull(handle, "handle is null");
        checkNotNull(predicate, "predicate is null");
        checkNotNull(partitionPredicate, "partitionPredicate is null");
        checkNotNull(mappings, "mappings is null");

        switch (handle.getDataSourceType()) {
            case NATIVE:
                return getNativeSplitAssignments(planNodeId, (NativeTableHandle) handle);
            case INTERNAL:
                return getInternalSplitAssignments(planNodeId, (InternalTableHandle) handle, predicate, mappings);
            case IMPORT:
                return getImportSplitAssignments(planNodeId, session, (ImportTableHandle) handle, predicate, partitionPredicate, mappings);
            case TPCH: // testing code only. This should really be pluggable...
                return getTpchSplitAssignments(planNodeId, session, (TpchTableHandle) handle, mappings);
            default:
                throw new IllegalArgumentException("unsupported handle type: " + handle);
        }
    }

    private Iterable<SplitAssignments> getNativeSplitAssignments(PlanNodeId planNodeId, NativeTableHandle handle)
    {
        Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
        Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodes(handle.getTableId());

        ImmutableList.Builder<SplitAssignments> splitAssignments = ImmutableList.builder();
        for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
            Split split = new NativeSplit(entry.getKey());
            List<Node> nodes = getNodes(nodeMap, entry.getValue());
            splitAssignments.add(new SplitAssignments(ImmutableMap.of(planNodeId, split), nodes));
        }
        return splitAssignments.build();
    }

    private Iterable<SplitAssignments> getInternalSplitAssignments(PlanNodeId planNodeId, InternalTableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        Map<Symbol, InternalColumnHandle> symbols = filterValueInstances(mappings, InternalColumnHandle.class);
        Split split = new InternalSplit(handle, extractFilters(predicate, symbols));

        Optional<Node> currentNode = nodeManager.getCurrentNode();
        Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");

        List<Node> nodes = ImmutableList.of(currentNode.get());
        return ImmutableList.of(new SplitAssignments(ImmutableMap.of(planNodeId, split), nodes));
    }

    private static <K, V, V1 extends V> Map<K, V1> filterValueInstances(Map<K, V> map, Class<V1> clazz)
    {
        return MapTransformer.of(map).filterValues(instanceOf(clazz)).castValues(clazz).map();
    }

    private static List<Node> getNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(Iterables.transform(nodeIdentifiers, Functions.forMap(nodeMap)));
    }

    private static Map<String, Node> getNodeMap(Set<Node> nodes)
    {
        return uniqueIndex(nodes, Node.getIdentifierFunction());
    }

    private Iterable<SplitAssignments> getImportSplitAssignments(PlanNodeId planNodeId,
            Session session,
            ImportTableHandle handle,
            Expression predicate,
            Predicate<PartitionInfo> partitionPredicate,
            Map<Symbol, ColumnHandle> mappings)
    {
        final String sourceName = handle.getSourceName();
        final String databaseName = handle.getDatabaseName();
        final String tableName = handle.getTableName();

        final List<String> partitions = getPartitions(session, sourceName, databaseName, tableName, predicate, partitionPredicate, mappings);
        final List<String> columns = IterableTransformer.on(mappings.values())
                .transform(MoreFunctions.<ColumnHandle, ImportColumnHandle>cast(ImportColumnHandle.class))
                .transform(columnNameGetter())
                .list();

        Iterable<PartitionChunk> chunks = retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<Iterable<PartitionChunk>>()
                {
                    @Override
                    public Iterable<PartitionChunk> call()
                            throws Exception
                    {
                        ImportClient importClient = importClientManager.getClient(sourceName);
                        return importClient.getPartitionChunks(databaseName, tableName, partitions, columns);
                    }
                });

        return Iterables.transform(chunks, createImportSplitFunction(planNodeId, sourceName));
    }

    private List<String> getPartitions(Session session,
            String sourceName,
            String databaseName,
            String tableName,
            Expression predicate,
            Predicate<PartitionInfo> partitionPredicate,
            Map<Symbol, ColumnHandle> mappings)
    {
        BiMap<Symbol, String> symbolToColumn = MapTransformer.of(mappings)
                .transformValues(MoreFunctions.<ColumnHandle, ImportColumnHandle>cast(ImportColumnHandle.class))
                .transformValues(columnNameGetter())
                .biMap();

        // First find candidate partitions -- try to push down the predicate to the underlying API
        Iterable<PartitionInfo> partitions = getCandidatePartitions(sourceName, databaseName, tableName, predicate, partitionPredicate, symbolToColumn);

        // Next, prune the list in case we got more partitions that necessary because parts of the predicate
        // could not be pushed down
        List<PartitionInfo> prunedPartitions = prunePartitions(session, partitions, predicate, symbolToColumn.inverse());

        return Lists.transform(prunedPartitions, partitionNameGetter());
    }

    /**
     * Get candidate partitions from underlying API and make a best effort to push down any relevant parts of the provided predicate
     */
    private Iterable<PartitionInfo> getCandidatePartitions(final String sourceName,
            final String databaseName,
            final String tableName,
            Expression predicate,
            final Predicate<PartitionInfo> partitionPredicate,
            Map<Symbol, String> symbolToColumnName)
    {
        // Look for any sub-expression in an AND expression of the form <partition key> = 'value'
        Set<ComparisonExpression> comparisons = IterableTransformer.on(extractConjuncts(predicate))
                .select(instanceOf(ComparisonExpression.class))
                .cast(ComparisonExpression.class)
                .select(or(
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, StringLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, StringLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, LongLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, LongLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, DoubleLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, DoubleLiteral.class, QualifiedNameReference.class)))
                .set();

        Map<String, SchemaField> partitionKeys = uniqueIndex(getPartitionKeys(sourceName, databaseName, tableName), fieldNameGetter());

        final Map<String, Object> bindings = new HashMap<>(); // map of columnName -> value
        for (ComparisonExpression comparison : comparisons) {
            // Record binding if condition is an equality comparison over a partition key
            QualifiedNameReference reference = extractReference(comparison);
            Symbol symbol = Symbol.fromQualifiedName(reference.getName());

            String columnName = symbolToColumnName.get(symbol);
            SchemaField field = partitionKeys.get(columnName);
            if (columnName != null && field != null) {
                Literal literal = extractLiteral(comparison);

                SchemaField.Type expectedType;
                Object value;
                if (literal instanceof DoubleLiteral) {
                    value = ((DoubleLiteral) literal).getValue();
                    expectedType = SchemaField.Type.DOUBLE;
                }
                else if (literal instanceof LongLiteral) {
                    value = ((LongLiteral) literal).getValue();
                    expectedType = SchemaField.Type.LONG;
                }
                else if (literal instanceof StringLiteral) {
                    value = ((StringLiteral) literal).getValue();
                    expectedType = SchemaField.Type.STRING;
                }
                else {
                    throw new AssertionError(String.format("Literal type (%s) not currently handled", literal.getClass().getName()));
                }

                if (field.getPrimitiveType() != expectedType) {
                    // TODO: this should really be an analyzer error -- types don't match
                    // TODO: add basic coercions for numeric types (long to double, etc)
                    return ImmutableList.of();
                }

                Object previous = bindings.get(columnName);
                if (previous != null && !previous.equals(value)) {
                    // Another conjunct has a different value for this column, so the predicate is necessarily false.
                    // Nothing to do here...
                    return ImmutableList.of();
                }
                bindings.put(columnName, value);
            }
        }

        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<Iterable<PartitionInfo>>()
                {
                    @Override
                    public Iterable<PartitionInfo> call()
                            throws Exception
                    {
                        ImportClient importClient = importClientManager.getClient(sourceName);
                        return Iterables.filter(importClient.getPartitions(databaseName, tableName, bindings), partitionPredicate);
                    }
                });
    }

    private QualifiedNameReference extractReference(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getLeft();
        }
        else if (expression.getRight() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type QualifiedNameReference");
    }

    private Literal extractLiteral(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof Literal) {
            return (Literal) expression.getLeft();
        }
        else if (expression.getRight() instanceof Literal) {
            return (Literal) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type Literal");
    }

    private List<SchemaField> getPartitionKeys(final String sourceName, final String databaseName, final String tableName)
    {
        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<SchemaField>>()
                {
                    @Override
                    public List<SchemaField> call()
                            throws Exception
                    {
                        ImportClient client = importClientManager.getClient(sourceName);
                        return client.getPartitionKeys(databaseName, tableName);
                    }
                });
    }

    private List<PartitionInfo> prunePartitions(Session session, Iterable<PartitionInfo> partitions, Expression predicate, Map<String, Symbol> columnNameToSymbol)
    {
        ImmutableList.Builder<PartitionInfo> builder = ImmutableList.builder();
        for (PartitionInfo partition : partitions) {
            // translate assignments from column->value to symbol->value
            // only bind partition keys that appear in the predicate
            Map<String, String> relevantFields = Maps.filterKeys(partition.getKeyFields(), in(columnNameToSymbol.keySet()));

            ImmutableMap.Builder<Symbol, Object> assignments = ImmutableMap.builder();
            for (Map.Entry<String, String> entry : relevantFields.entrySet()) {
                Symbol symbol = columnNameToSymbol.get(entry.getKey());
                assignments.put(symbol, entry.getValue());
            }

            SymbolResolver resolver = new LookupSymbolResolver(assignments.build());
            Object optimized = ExpressionInterpreter.expressionOptimizer(resolver, metadata, session).process(predicate, null);
            if (!Boolean.FALSE.equals(optimized) && optimized != null) {
                builder.add(partition);
            }
        }

        return builder.build();
    }

    private static <T> Map<T, String> extractFilters(Expression predicate, Map<Symbol, T> mappings)
    {
        // Look for any sub-expression in an AND expression of the form <name> = 'value'
        Set<ComparisonExpression> comparisons = IterableTransformer.on(extractConjuncts(predicate))
                .select(instanceOf(ComparisonExpression.class))
                .cast(ComparisonExpression.class)
                .select(or(matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, StringLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, StringLiteral.class, QualifiedNameReference.class)))
                .set();

        ImmutableMap.Builder<T, String> filters = ImmutableMap.builder();
        for (ComparisonExpression comparison : comparisons) {
            QualifiedNameReference reference;
            StringLiteral literal;

            if (comparison.getLeft() instanceof QualifiedNameReference) {
                reference = (QualifiedNameReference) comparison.getLeft();
                literal = (StringLiteral) comparison.getRight();
            }
            else {
                reference = (QualifiedNameReference) comparison.getRight();
                literal = (StringLiteral) comparison.getRight();
            }

            Symbol symbol = Symbol.fromQualifiedName(reference.getName());
            String value = literal.getValue();

            T columnHandle = mappings.get(symbol);
            if (columnHandle != null) {
                filters.put(columnHandle, value);
            }
        }
        return filters.build();
    }

    private Function<PartitionChunk, SplitAssignments> createImportSplitFunction(final PlanNodeId planNodeId, final String sourceName)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        final Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(new Supplier<NodeMap>()
        {
            @Override
            public NodeMap get()
            {
                ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
                ImmutableSetMultimap.Builder<Rack, Node> byRack = ImmutableSetMultimap.builder();

                for (Node node : nodeManager.getActiveDatasourceNodes(sourceName)) {
                    try {
                        InetAddress host = InetAddress.getByName(node.getHttpUri().getHost());
                        byHost.put(host, node);

                        byRack.put(Rack.of(host), node);
                    }
                    catch (UnknownHostException e) {
                        // ignore
                    }
                }

                return new NodeMap(byHost.build(), byRack.build());
            }
        }, 5, TimeUnit.SECONDS);

        return new Function<PartitionChunk, SplitAssignments>()
        {
            @Override
            public SplitAssignments apply(PartitionChunk chunk)
            {
                ImportClient importClient = importClientManager.getClient(sourceName);
                Split split = new ImportSplit(sourceName,
                        chunk.getPartitionName(),
                        chunk.isLastChunk(),
                        SerializedPartitionChunk.create(importClient, chunk), chunk.getInfo());

                List<Node> nodes = chooseNodes(nodeMap.get(), chunk.getHosts(), 10);
                Preconditions.checkState(!nodes.isEmpty(), "No active %s data nodes", sourceName);
                return new SplitAssignments(ImmutableMap.of(planNodeId, split), nodes);
            }
        };
    }

    private List<Node> chooseNodes(NodeMap nodeMap, List<InetAddress> hints, int minCount)
    {
        Set<Node> chosen = new LinkedHashSet<>(minCount);

        for (InetAddress hint : hints) {
            for (Node node : nodeMap.getNodesByHost().get(hint)) {
                if (chosen.add(node)) {
                    scheduleLocal.incrementAndGet();
                }
            }

            for (Node node : nodeMap.getNodesByRack().get(Rack.of(hint))) {
                if (chosen.add(node)) {
                    scheduleRack.incrementAndGet();
                }
            }
        }

        // add some random nodes if below the minimum count
        if (chosen.size() < minCount) {
            for (Node node : lazyShuffle(nodeMap.getNodesByHost().values())) {
                if (chosen.add(node)) {
                    scheduleRandom.incrementAndGet();
                }

                if (chosen.size() == minCount) {
                    break;
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    private static Function<SchemaField, String> fieldNameGetter()
    {
        return new Function<SchemaField, String>()
        {
            @Override
            public String apply(SchemaField input)
            {
                return input.getFieldName();
            }
        };
    }

    private static Function<PartitionInfo, String> partitionNameGetter()
    {
        return new Function<PartitionInfo, String>()
        {
            @Override
            public String apply(PartitionInfo input)
            {
                return input.getName();
            }
        };
    }

    private static <T> Iterable<T> lazyShuffle(final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new AbstractIterator<T>()
                {
                    List<T> list = Lists.newArrayList(iterable);
                    int limit = list.size();

                    @Override
                    protected T computeNext()
                    {
                        if (limit == 0) {
                            return endOfData();
                        }

                        int position = ThreadLocalRandom.current().nextInt(limit);

                        T result = list.get(position);
                        list.set(position, list.get(limit - 1));
                        limit--;

                        return result;
                    }
                };
            }
        };
    }

    private Iterable<SplitAssignments> getTpchSplitAssignments(PlanNodeId planNodeId,
            Session session,
            TpchTableHandle handle,
            Map<Symbol, ColumnHandle> mappings)
    {
        Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
        ImmutableList.Builder<SplitAssignments> splitAssignments = ImmutableList.builder();

        int totalParts = nodeMap.size();
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        for (Map.Entry<String, Node> entry : nodeMap.entrySet()) {
            TpchSplit tpchSplit = new TpchSplit(handle, partNumber++, totalParts);
            splitAssignments.add(new SplitAssignments(ImmutableMap.of(planNodeId, tpchSplit), ImmutableList.of(entry.getValue())));
        }

        return splitAssignments.build();
    }

    private static class NodeMap
    {
        private final SetMultimap<InetAddress, Node> nodesByHost;
        private final SetMultimap<Rack, Node> nodesByRack;

        public NodeMap(SetMultimap<InetAddress, Node> nodesByHost, SetMultimap<Rack, Node> nodesByRack)
        {
            this.nodesByHost = nodesByHost;
            this.nodesByRack = nodesByRack;
        }

        public SetMultimap<InetAddress, Node> getNodesByHost()
        {
            return nodesByHost;
        }

        public SetMultimap<Rack, Node> getNodesByRack()
        {
            return nodesByRack;
        }
    }

    private static class Rack
    {
        private int id;

        public static Rack of(InetAddress address)
        {
            // TODO: this needs to be pluggable
            int id = InetAddresses.coerceToInteger(address) & 0xFF_FF_FF_00;
            return new Rack(id);
        }

        private Rack(int id)
        {
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Rack rack = (Rack) o;

            if (id != rack.id) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return id;
        }
    }
}
