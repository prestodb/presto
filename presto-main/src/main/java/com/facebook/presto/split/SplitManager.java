package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.filter;

public class SplitManager
{
    private final Metadata metadata;
    private final Set<ConnectorSplitManager> splitManagers = Sets.newSetFromMap(new ConcurrentHashMap<ConnectorSplitManager, Boolean>());

    @Inject
    public SplitManager(Metadata metadata, Set<ConnectorSplitManager> splitManagers)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManagers.addAll(splitManagers);
    }

    public void addConnectorSplitManager(ConnectorSplitManager connectorSplitManager)
    {
        splitManagers.add(connectorSplitManager);
    }

    public DataSource getSplits(Session session, TableHandle handle, Expression predicate, Predicate<Partition> partitionPredicate, Map<Symbol, ColumnHandle> mappings)
    {
        List<Partition> partitions = getPartitions(session, handle, predicate, partitionPredicate, mappings);
        ConnectorSplitManager connectorSplitManager = getConnectorSplitManager(handle);

        return connectorSplitManager.getPartitionSplits(partitions, ImmutableList.copyOf(mappings.values()));
    }

    private List<Partition> getPartitions(Session session,
            TableHandle table,
            Expression predicate,
            Predicate<Partition> partitionPredicate,
            Map<Symbol, ColumnHandle> mappings)
    {
        BiMap<Symbol, ColumnHandle> symbolToColumn = ImmutableBiMap.copyOf(mappings);

        // First find candidate partitions -- try to push down the predicate to the underlying API
        List<Partition> partitions = getCandidatePartitions(table, predicate, symbolToColumn);

        // filter partitions using the specified predicate
        partitions = ImmutableList.copyOf(filter(partitions, partitionPredicate));

        // Next, prune the list in case we got more partitions that necessary because parts of the predicate
        // could not be pushed down
        partitions = prunePartitions(session, partitions, predicate, symbolToColumn.inverse());

        return partitions;
    }

    /**
     * Get candidate partitions from underlying API and make a best effort to push down any relevant parts of the provided predicate
     */
    private List<Partition> getCandidatePartitions(final TableHandle table, Expression predicate, Map<Symbol, ColumnHandle> symbolToColumnName)
    {
        Optional<Map<ColumnHandle, Object>> bindings = ExpressionUtil.extractConstantValues(predicate, symbolToColumnName);

        // if bindings could not be build, no partitions will match
        if (!bindings.isPresent()) {
            return ImmutableList.of();
        }

        return getPartitions(table, bindings);
    }

    public List<Partition> getPartitions(TableHandle table, Optional<Map<ColumnHandle, Object>> bindings)
    {
        checkNotNull(table, "table is null");
        return getConnectorSplitManager(table).getPartitions(table, bindings.get());
    }

    private List<Partition> prunePartitions(Session session, List<Partition> partitions, Expression predicate, Map<ColumnHandle, Symbol> columnToSymbol)
    {
        ImmutableList.Builder<Partition> builder = ImmutableList.builder();
        for (Partition partition : partitions) {
            // translate assignments from column->value to symbol->value
            // only bind partition keys that appear in the predicate
            Map<ColumnHandle, String> relevantFields = Maps.filterKeys(partition.getKeys(), in(columnToSymbol.keySet()));

            ImmutableMap.Builder<Symbol, Object> assignments = ImmutableMap.builder();
            for (Map.Entry<ColumnHandle, String> entry : relevantFields.entrySet()) {
                Symbol symbol = columnToSymbol.get(entry.getKey());
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

    private ConnectorSplitManager getConnectorSplitManager(TableHandle handle)
    {
        for (ConnectorSplitManager connectorSplitManager : splitManagers) {
            if (connectorSplitManager.canHandle(handle)) {
                return connectorSplitManager;
            }
        }
        throw new IllegalArgumentException("No split manager for " + handle);
    }
}
