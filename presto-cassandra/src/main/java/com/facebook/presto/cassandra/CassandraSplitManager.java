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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Host;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.cassandra.util.HostAddressFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.EXTERNAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

public class CassandraSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(CassandraSplitManager.class);

    private final String connectorId;
    private final CassandraSession cassandraSession;
    private final CachingCassandraSchemaProvider schemaProvider;
    private final int partitionSizeForBatchSelect;
    private final CassandraTokenSplitManager tokenSplitMgr;
    private final ListeningExecutorService executor;

    @Inject
    public CassandraSplitManager(CassandraConnectorId connectorId,
            CassandraClientConfig cassandraClientConfig,
            CassandraSession cassandraSession,
            CachingCassandraSchemaProvider schemaProvider,
            CassandraTokenSplitManager tokenSplitMgr,
            @ForCassandra ExecutorService executor)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.partitionSizeForBatchSelect = cassandraClientConfig.getPartitionSizeForBatchSelect();
        this.tokenSplitMgr = tokenSplitMgr;
        this.executor = listeningDecorator(executor);
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        CassandraTableHandle cassandraTableHandle = checkType(tableHandle, CassandraTableHandle.class, "tableHandle");
        checkNotNull(tupleDomain, "tupleDomain is null");
        CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<CassandraPartition> allPartitions = getCassandraPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<ConnectorPartition> partitions = FluentIterable.from(allPartitions)
                .filter(partitionMatches(tupleDomain))
                .filter(ConnectorPartition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && ((CassandraPartition) partitions.get(0)).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionColumns))));
            }
        }

        // push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        if (partitions.size() == 1 && ((CassandraPartition) partitions.get(0)).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains();
            List<ColumnHandle> indexedColumns = Lists.newArrayList();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle column = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.isIndexed() && domain.isSingleValue()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()))
                      .append(" = ")
                      .append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(entry.getValue().getSingleValue()), column.getCassandraType()));
                    indexedColumns.add(column);
                    // Only one indexed column predicate can be pushed down.
                    break;
                }
            }
            if (sb.length() > 0) {
                CassandraPartition partition = (CassandraPartition) partitions.get(0);
                TupleDomain<ColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains(), not(in(indexedColumns))));
                partitions = Lists.newArrayList();
                partitions.add(new CassandraPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true));
                return new ConnectorPartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new ConnectorPartitionResult(partitions, remainingTupleDomain);
    }

    private List<CassandraPartition> getCassandraPartitions(final CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of();
        }

        Set<List<Comparable<?>>> partitionKeysSet = getPartitionKeysSet(table, tupleDomain);

        // empty filter means, all partitions
        if (partitionKeysSet.isEmpty()) {
            return schemaProvider.getAllPartitions(table);
        }

        ImmutableList.Builder<ListenableFuture<List<CassandraPartition>>> getPartitionResults = ImmutableList.builder();
        for (final List<Comparable<?>> partitionKeys : partitionKeysSet) {
            getPartitionResults.add(executor.submit(new Callable<List<CassandraPartition>>()
            {
                @Override
                public List<CassandraPartition> call()
                {
                    return schemaProvider.getPartitions(table, partitionKeys);
                }
            }));
        }

        ImmutableList.Builder<CassandraPartition> partitions = ImmutableList.builder();
        for (ListenableFuture<List<CassandraPartition>> result : getPartitionResults.build()) {
            try {
                partitions.addAll(result.get());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                throw new PrestoException(EXTERNAL, "Error fetching cassandra partitions", e);
            }
        }

        return partitions.build();
    }

    private static Set<List<Comparable<?>>> getPartitionKeysSet(CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<Set<Comparable<?>>> partitionColumnValues = ImmutableList.builder();
        for (CassandraColumnHandle columnHandle : table.getPartitionKeyColumns()) {
            Domain domain = tupleDomain.getDomains().get(columnHandle);

            // if there is no constraint on a partition key, return an empty set
            if (domain == null) {
                return ImmutableSet.of();
            }

            // todo does cassandra allow null partition keys?
            if (domain.isNullAllowed()) {
                return ImmutableSet.of();
            }

            ImmutableSet.Builder<Comparable<?>> columnValues = ImmutableSet.builder();
            for (Range range : domain.getRanges()) {
                // if the range is not a single value, we can not perform partition pruning
                if (!range.isSingleValue()) {
                    return ImmutableSet.of();
                }
                Comparable<?> value = range.getSingleValue();

                CassandraType valueType = columnHandle.getCassandraType();
                columnValues.add(valueType.getValueForPartitionKey(value));

            }
            partitionColumnValues.add(columnValues.build());
        }
        return Sets.cartesianProduct(partitionColumnValues.build());
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        CassandraTableHandle cassandraTableHandle = checkType(tableHandle, CassandraTableHandle.class, "tableHandle");

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            ConnectorPartition partition = partitions.get(0);
            CassandraPartition cassandraPartition = checkType(partition, CassandraPartition.class, "partition");

            if (cassandraPartition.isUnpartitioned() || cassandraPartition.isIndexedColumnPredicatePushdown()) {
                CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
                List<ConnectorSplit> splits = getSplitsByTokenRange(table, cassandraPartition.getPartitionId());
                return new FixedSplitSource(connectorId, splits);
            }
        }

        return new FixedSplitSource(connectorId, getSplitsForPartitions(cassandraTableHandle, partitions));
    }

    private List<ConnectorSplit> getSplitsByTokenRange(CassandraTable table, String partitionId)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        List<CassandraTokenSplitManager.TokenSplit> tokenSplits;
        try {
            tokenSplits = tokenSplitMgr.getSplits(schema, tableName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (CassandraTokenSplitManager.TokenSplit tokenSplit : tokenSplits) {
            String condition = buildTokenCondition(tokenExpression, tokenSplit.getStartToken(), tokenSplit.getEndToken());
            List<HostAddress> addresses = new HostAddressFactory().AddressNamesToHostAddressList(tokenSplit.getHosts());
            CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, addresses);
            builder.add(split);
        }

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, String startToken, String endToken)
    {
        return tokenExpression + " > " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    private List<ConnectorSplit> getSplitsForPartitions(CassandraTableHandle cassTableHandle, List<ConnectorPartition> partitions)
    {
        String schema = cassTableHandle.getSchemaName();
        String table = cassTableHandle.getTableName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        // For single partition key column table, we can merge multiple partitions into a single split
        // by using IN CLAUSE in a single select query if the partitions have the same host list.
        // For multiple partition key columns table, we can't merge them into a single select query, so
        // keep them in a separate split.
        boolean singlePartitionKeyColumn = true;
        String partitionKeyColumnName = null;
        if (!partitions.isEmpty()) {
            singlePartitionKeyColumn = partitions.get(0).getTupleDomain().getNullableColumnDomains().size() == 1;
            if (singlePartitionKeyColumn) {
                String partitionId = partitions.get(0).getPartitionId();
                partitionKeyColumnName = partitionId.substring(0, partitionId.lastIndexOf("=") - 1);
            }
        }
        Map<Set<String>, Set<String>> hostsToPartitionKeys = Maps.newHashMap();
        Map<Set<String>, List<HostAddress>> hostMap = Maps.newHashMap();

        for (ConnectorPartition partition : partitions) {
            CassandraPartition cassandraPartition = checkType(partition, CassandraPartition.class, "partition");
            Set<Host> hosts = cassandraSession.getReplicas(schema, cassandraPartition.getKeyAsByteBuffer());
            List<HostAddress> addresses = hostAddressFactory.toHostAddressList(hosts);
            if (singlePartitionKeyColumn) {
                // host ip addresses
                ImmutableSet.Builder<String> sb = ImmutableSet.builder();
                for (HostAddress address : addresses) {
                    sb.add(address.getHostText());
                }
                Set<String> hostAddresses = sb.build();
                // partition key values
                Set<String> values = hostsToPartitionKeys.get(hostAddresses);
                if (values == null) {
                    values = Sets.newHashSet();
                }
                String partitionId = cassandraPartition.getPartitionId();
                values.add(partitionId.substring(partitionId.lastIndexOf("=") + 2));
                hostsToPartitionKeys.put(hostAddresses, values);
                hostMap.put(hostAddresses, addresses);
            }
            else {
                CassandraSplit split = new CassandraSplit(connectorId, schema, table, cassandraPartition.getPartitionId(), null, addresses);
                builder.add(split);
            }
        }
        if (singlePartitionKeyColumn) {
            for (Map.Entry<Set<String>, Set<String>> entry : hostsToPartitionKeys.entrySet()) {
                StringBuilder sb = new StringBuilder(partitionSizeForBatchSelect);
                int size = 0;
                for (String value : entry.getValue()) {
                    if (size > 0) {
                        sb.append(",");
                    }
                    sb.append(value);
                    size++;
                    if (size > partitionSizeForBatchSelect) {
                        String partitionId = String.format("%s in (%s)", partitionKeyColumnName, sb.toString());
                        CassandraSplit split = new CassandraSplit(connectorId, schema, table, partitionId, null, hostMap.get(entry.getKey()));
                        builder.add(split);
                        size = 0;
                        sb.setLength(0);
                        sb.trimToSize();
                    }
                }
                if (size > 0) {
                    String partitionId = String.format("%s in (%s)", partitionKeyColumnName, sb.toString());
                    CassandraSplit split = new CassandraSplit(connectorId, schema, table, partitionId, null, hostMap.get(entry.getKey()));
                    builder.add(split);
                }
            }
        }
        return builder.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<CassandraPartition> partitionMatches(final TupleDomain<ColumnHandle> tupleDomain)
    {
        return new Predicate<CassandraPartition>()
        {
            @Override
            public boolean apply(CassandraPartition partition)
            {
                return tupleDomain.overlaps(partition.getTupleDomain());
            }
        };
    }
}
