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
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MapTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
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
    private final DistributionStat chooseNodesTime = new DistributionStat();

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
    @Nested
    public DistributionStat getChooseNodesTime()
    {
        return chooseNodesTime;
    }

    @Managed
    public void reset()
    {
        scheduleLocal.set(0);
        scheduleRack.set(0);
        scheduleRandom.set(0);
    }

    public Iterable<SplitAssignments> getSplitAssignments(Session session, TableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        switch (handle.getDataSourceType()) {
            case NATIVE:
                return getNativeSplitAssignments((NativeTableHandle) handle);
            case INTERNAL:
                return getInternalSplitAssignments((InternalTableHandle) handle, predicate, mappings);
            case IMPORT:
                return getImportSplitAssignments(session, (ImportTableHandle) handle, predicate, mappings);
            default:
                throw new IllegalArgumentException("unsupported handle type: " + handle);
        }
    }

    private Iterable<SplitAssignments> getNativeSplitAssignments(NativeTableHandle handle)
    {
        Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
        Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodes(handle.getTableId());

        ImmutableList.Builder<SplitAssignments> splitAssignments = ImmutableList.builder();
        for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
            Split split = new NativeSplit(entry.getKey());
            List<Node> nodes = getNodes(nodeMap, entry.getValue());
            splitAssignments.add(new SplitAssignments(split, nodes));
        }
        return splitAssignments.build();
    }

    private Iterable<SplitAssignments> getInternalSplitAssignments(InternalTableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        Map<Symbol, InternalColumnHandle> symbols = filterValueInstances(mappings, InternalColumnHandle.class);
        Split split = new InternalSplit(handle, extractFilters(predicate, symbols));
        List<Node> nodes = ImmutableList.of(nodeManager.getCurrentNode());
        return ImmutableList.of(new SplitAssignments(split, nodes));
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

    private Iterable<SplitAssignments> getImportSplitAssignments(Session session, ImportTableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        final String sourceName = handle.getSourceName();
        final String databaseName = handle.getDatabaseName();
        final String tableName = handle.getTableName();

        final List<String> partitions = getPartitions(session, sourceName, databaseName, tableName, predicate, mappings);
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

        return Iterables.transform(chunks, createImportSplitFunction(sourceName));
    }

    private List<String> getPartitions(Session session, String sourceName, String databaseName, String tableName, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        BiMap<Symbol, String> symbolToColumn = MapTransformer.of(mappings)
                .transformValues(MoreFunctions.<ColumnHandle, ImportColumnHandle>cast(ImportColumnHandle.class))
                .transformValues(columnNameGetter())
                .biMap();

        // First find candidate partitions -- try to push down the predicate to the underlying API
        List<PartitionInfo> partitions = getCandidatePartitions(sourceName, databaseName, tableName, predicate, symbolToColumn);

        // Next, prune the list in case we got more partitions that necessary because parts of the predicate
        // could not be pushed down
        partitions = prunePartitions(session, partitions, predicate, symbolToColumn.inverse());

        return Lists.transform(partitions, partitionNameGetter());
    }

    /**
     * Get candidate partitions from underlying API and make a best effort to push down any relevant parts of the provided predicate
     */
    private List<PartitionInfo> getCandidatePartitions(final String sourceName, final String databaseName, final String tableName, Expression predicate, Map<Symbol, String> symbolToColumnName)
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
                .runUnchecked(new Callable<List<PartitionInfo>>()
                {
                    @Override
                    public List<PartitionInfo> call()
                            throws Exception
                    {
                        ImportClient importClient = importClientManager.getClient(sourceName);
                        return importClient.getPartitions(databaseName, tableName, bindings);
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

    private List<PartitionInfo> prunePartitions(Session session, List<PartitionInfo> partitions, Expression predicate, Map<String, Symbol> columnNameToSymbol)
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

    private Function<PartitionChunk, SplitAssignments> createImportSplitFunction(final String sourceName)
    {
        return new Function<PartitionChunk, SplitAssignments>()
        {
            @Override
            public SplitAssignments apply(PartitionChunk chunk)
            {
                ImportClient importClient = importClientManager.getClient(sourceName);
                Split split = new ImportSplit(sourceName, SerializedPartitionChunk.create(importClient, chunk), chunk.getInfo());

                List<Node> nodes = chooseNodes(sourceName, chunk.getHosts(), 3);
                Preconditions.checkState(!nodes.isEmpty(), "No active %s data nodes", sourceName);
                return new SplitAssignments(split, nodes);
            }
        };
    }

    private List<Node> chooseNodes(String sourceName, List<InetAddress> hosts, int limit)
    {
        long startTime = System.nanoTime();
        try {
            ArrayListMultimap<InetAddress, Node> nodesByHost = indexNodesByHost(nodeManager.getActiveDatasourceNodes(sourceName));

            List<Node> chosen = new ArrayList<>(limit);
            List<InetAddress> nonLocalHosts = new ArrayList<>();
            for (InetAddress host : hosts) {
                List<Node> nodes = nodesByHost.get(host);
                if (nodes != null && !nodes.isEmpty()) {
                    // only allow host to be used once
                    Node node = nodes.remove(0);
                    scheduleLocal.incrementAndGet();
                    chosen.add(node);
                    if (chosen.size() == limit) {
                        return ImmutableList.copyOf(chosen);
                    }
                } else {
                    // remember the hosts we did not find
                    nonLocalHosts.add(host);
                }
            }

            // look for a host "near" the the non-local hosts
            for (InetAddress nonLocalHost : nonLocalHosts) {
                for (Entry<InetAddress, Node> entry : ImmutableList.copyOf(nodesByHost.entries())) {
                    if (isRackLocal(nonLocalHost, entry.getKey())) {
                        // only allow host to be used once
                        nodesByHost.remove(entry.getKey(), entry.getValue());
                        scheduleRack.incrementAndGet();
                        chosen.add(entry.getValue());
                        if (chosen.size() == limit) {
                            return ImmutableList.copyOf(chosen);
                        }
                    }
                }
            }

            // add some random nodes if below the limit
            if (chosen.size() < limit) {
                int randomCount = limit - chosen.size();
                scheduleRandom.addAndGet(randomCount);
                chosen.addAll(limit(shuffle(nodesByHost.values()), randomCount));
            }

            return ImmutableList.copyOf(chosen);
        }
        finally {
            chooseNodesTime.add(System.nanoTime() - startTime);
        }
    }

    private ArrayListMultimap<InetAddress, Node> indexNodesByHost(Set<Node> nodes)
    {
        ArrayListMultimap<InetAddress, Node> nodesByHost = ArrayListMultimap.create();
        for (Node node : nodes) {
            try {
                nodesByHost.put(InetAddress.getByName(node.getHttpUri().getHost()), node);
            }
            catch (UnknownHostException e) {
                // skip nodes with a bad inet address
            }
        }
        return nodesByHost;
    }

    private static boolean isRackLocal(InetAddress inetAddress1, InetAddress inetAddress2)
    {
        byte[] address1 = inetAddress1.getAddress();
        byte[] address2 = inetAddress2.getAddress();

        // if the first 24 bits match, assume it is rack local
        return address1[0] == address2[0] && address1[1] == address2[1] && address1[2] == address2[2];
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

    private static <T> List<T> limit(Iterable<T> iterable, int limitSize)
    {
        return ImmutableList.copyOf(Iterables.limit(iterable, limitSize));
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
