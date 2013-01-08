package com.facebook.presto.split;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.ImportTableHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MapTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.facebook.presto.metadata.ImportColumnHandle.columnNameGetter;
import static com.facebook.presto.sql.tree.ComparisonExpression.matchesPattern;
import static com.facebook.presto.util.IterableUtils.limit;
import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.or;

public class SplitManager
{
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final ImportClientFactory importClientFactory;
    private final Metadata metadata;

    @Inject
    public SplitManager(NodeManager nodeManager, ShardManager shardManager, ImportClientFactory importClientFactory, Metadata metadata)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public Iterable<SplitAssignments> getSplitAssignments(TableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        switch (handle.getDataSourceType()) {
            case NATIVE:
                return getNativeSplitAssignments((NativeTableHandle) handle);
            case INTERNAL:
                return getInternalSplitAssignments((InternalTableHandle) handle);
            case IMPORT:
                return getImportSplitAssignments((ImportTableHandle) handle, predicate, mappings);
        }
        throw new IllegalArgumentException("unsupported handle type: " + handle);
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

    private Iterable<SplitAssignments> getInternalSplitAssignments(InternalTableHandle handle)
    {
        Split split = new InternalSplit(handle);
        List<Node> nodes = ImmutableList.of(nodeManager.getCurrentNode());
        return ImmutableList.of(new SplitAssignments(split, nodes));
    }

    private static List<Node> getNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(Iterables.transform(nodeIdentifiers, Functions.forMap(nodeMap)));
    }

    private static Map<String, Node> getNodeMap(Set<Node> nodes)
    {
        return Maps.uniqueIndex(nodes, Node.getIdentifierFunction());
    }

    private Iterable<SplitAssignments> getImportSplitAssignments(ImportTableHandle handle, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        final String sourceName = handle.getSourceName();
        final String databaseName = handle.getDatabaseName();
        final String tableName = handle.getTableName();

        final List<String> partitions = getPartitions(sourceName, databaseName, tableName, predicate, mappings);
        final List<String> columns = IterableTransformer.on(mappings.values())
                .transform(MoreFunctions.<ColumnHandle, ImportColumnHandle>cast(ImportColumnHandle.class))
                .transform(columnNameGetter())
                .list();

        Iterable<List<PartitionChunk>> chunks = runWithRetryUnchecked(new Callable<Iterable<List<PartitionChunk>>>()
        {
            @Override
            public Iterable<List<PartitionChunk>> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getPartitionChunks(databaseName, tableName, partitions, columns);
            }
        });

        return Iterables.transform(Iterables.concat(chunks), createImportSplitFunction(sourceName));
    }

    private List<String> getPartitions(String sourceName, String databaseName, String tableName, Expression predicate, Map<Symbol, ColumnHandle> mappings)
    {
        BiMap<Symbol, String> symbolToColumn = MapTransformer.of(mappings)
                .transformValues(MoreFunctions.<ColumnHandle, ImportColumnHandle>cast(ImportColumnHandle.class))
                .transformValues(columnNameGetter())
                .biMap();

        // First find candidate partitions -- try to push down the predicate to the underlying API
        List<PartitionInfo> partitions = getCandidatePartitions(sourceName, databaseName, tableName, predicate, symbolToColumn);

        // Next, prune the list in case we got more partitions that necessary because parts of the predicate
        // could not be pushed down
        partitions = prunePartitions(partitions, predicate, symbolToColumn.inverse());

        return Lists.transform(partitions, partitionNameGetter());
    }

    /**
     * Get candidate partitions from underlying API and make a best effort to push down any relevant parts of the provided predicate
     */
    private List<PartitionInfo> getCandidatePartitions(final String sourceName, final String databaseName, final String tableName, Expression predicate, Map<Symbol, String> symbolToColumnName)
    {
        List<String> partitionKeys = getPartitionKeys(sourceName, databaseName, tableName);

        // Look for any sub-expression in an AND expression of the form <partition key> = 'value'
        Set<ComparisonExpression> comparisons = IterableTransformer.on(extractConjuncts(predicate))
                .select(instanceOf(ComparisonExpression.class))
                .cast(ComparisonExpression.class)
                .select(or(matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, StringLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, StringLiteral.class, QualifiedNameReference.class)))
                .set();

        // TODO: handle non-string values
        final Map<String, Object> bindings = new HashMap<>(); // map of columnName -> value
        for (ComparisonExpression comparison : comparisons) {
            // Record binding if condition is an equality comparison over a partition key
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

            String columnName = symbolToColumnName.get(symbol);
            if (columnName != null && partitionKeys.contains(columnName)) {
                Object previous = bindings.get(columnName);
                if (previous != null && !previous.equals(value)) {
                    // Another conjunct has a different value for this column, so the predicate is necessarily false.
                    // Nothing to do here...
                    return ImmutableList.of();
                }
                bindings.put(columnName, value);
            }
        }

        return runWithRetryUnchecked(new Callable<List<PartitionInfo>>()
        {
            @Override
            public List<PartitionInfo> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getPartitions(databaseName, tableName, bindings);
            }
        });
    }

    private List<String> getPartitionKeys(final String sourceName, final String databaseName, final String tableName)
    {
        return runWithRetryUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                ImportClient client = importClientFactory.getClient(sourceName);
                return Lists.transform(client.getPartitionKeys(databaseName, tableName), fieldNameGetter());
            }
        });
    }

    private List<PartitionInfo> prunePartitions(List<PartitionInfo> partitions, Expression predicate, Map<String, Symbol> columnNameToSymbol)
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
            Object optimized = new ExpressionInterpreter(resolver, metadata).process(predicate, null);
            if (!Boolean.FALSE.equals(optimized) && optimized != null) {
                builder.add(partition);
            }
        }

        return builder.build();
    }

    private ImmutableList<Expression> extractConjuncts(Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getType() == LogicalBinaryExpression.Type.AND) {
            LogicalBinaryExpression and = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractConjuncts(and.getLeft()))
                    .addAll(extractConjuncts(and.getRight()))
                    .build();
        }

        return ImmutableList.of(expression);
    }

    private Function<PartitionChunk, SplitAssignments> createImportSplitFunction(final String sourceName)
    {
        return new Function<PartitionChunk, SplitAssignments>()
        {
            @Override
            public SplitAssignments apply(PartitionChunk chunk)
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                Split split = new ImportSplit(sourceName, SerializedPartitionChunk.create(importClient, chunk));
                List<Node> nodes = limit(shuffle(nodeManager.getActiveDatasourceNodes(sourceName)), 3);
                Preconditions.checkState(!nodes.isEmpty(), "No active %s data nodes", sourceName);
                return new SplitAssignments(split, nodes);
            }
        };
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
}
