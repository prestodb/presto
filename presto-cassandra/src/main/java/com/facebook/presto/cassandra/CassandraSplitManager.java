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

import static com.facebook.presto.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.EXTERNAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.Host;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.cassandra.util.HostAddressFactory;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Marker.Bound;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
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
import com.google.inject.Inject;

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
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
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
        TupleDomain<ConnectorColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && ((CassandraPartition) partitions.get(0)).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ConnectorColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionColumns))));
            }
        }

        // opt 1: push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        // opt 2: push down clustered columns predicates if all previous columns in order of ordinalPosition have predicates, too
        if (partitions.size() == 1) {
            boolean partitioned = !((CassandraPartition) partitions.get(0)).isUnpartitioned();
            Map<ConnectorColumnHandle, Domain> domains = tupleDomain.getDomains();
            List<ConnectorColumnHandle> indexedColumns = Lists.newArrayList();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            if (partitioned) {
                sb.append(((CassandraPartition) partitions.get(0)).getPartitionId());
            }
            boolean indexedColumnUsed = false;
            for (Map.Entry<ConnectorColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle column = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                indexedColumnUsed |= addCQLRequestPredicate(sb, column, domains, domain, indexedColumns, indexedColumnUsed, partitioned);
            }
            if (sb.length() > 0) {
                CassandraPartition partition = (CassandraPartition) partitions.get(0);
                TupleDomain<ConnectorColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains(), not(in(indexedColumns))));
                partitions = Lists.newArrayList();
                partitions.add(new CassandraPartition(partition.getKey(), sb.toString(), filterIndexedColumn, indexedColumnUsed));
                return new ConnectorPartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new ConnectorPartitionResult(partitions, remainingTupleDomain);
    }

    /**
     * checks if this is a predicate which should be processed by Cassandra and adds it to a query
     * @param sb contains the query to potentially append to
     * @param column is the column which needs to be investigated
     * @param domains the list of all TupleDomains for this query
     * @param domain is the domain which needs to be investigated
     * @param addedColumns is a list of already processed columns, which have been added to the query
     * @param indexedColumnUsed is a marker whether an indexed column has been added already
     * @param partitioned indicates whether this is a partitioned query
     * @return whether an indexed column has been used
     */
    private static boolean addCQLRequestPredicate(StringBuilder sb, CassandraColumnHandle column, Map<ConnectorColumnHandle, Domain> domains, Domain domain, List<ConnectorColumnHandle> addedColumns, boolean indexedColumnUsed, boolean partitioned)
    {
        boolean result = indexedColumnUsed;
        if (!partitioned && column.isIndexed() && domain.isSingleValue() && !indexedColumnUsed) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(CassandraCqlUtils.validColumnName(column.getName()))
              .append(" = ")
              .append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(domain.getSingleValue()), column.getCassandraType()));
            addedColumns.add(column);
            result = true;
        }
        if (partitioned && column.isClusteringKey() && (domain.getRanges() != null) && (domain.getRanges().getRangeCount() == 1) && checkPreviousClusteredKey(column, domains)) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            for (int i = 0; i < domain.getRanges().getRangeCount(); i++) {
                boolean first = true;
                if (!domain.getRanges().getRanges().get(i).getHigh().isUpperUnbounded()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()));
                    if (domain.getRanges().getRanges().get(i).getHigh().getBound() == Bound.EXACTLY) {
                        sb.append(" <= ");
                    }
                    else {
                        sb.append(" < ");
                    }
                    sb.append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(domain.getRanges().getRanges().get(i).getHigh().getValue()), column.getCassandraType()));
                    first = false;
                }
                if (!domain.getRanges().getRanges().get(i).getLow().isLowerUnbounded()) {
                    if (!first) {
                        sb.append(" AND ");
                    }
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()));
                    if (domain.getRanges().getRanges().get(i).getLow().getBound() == Bound.EXACTLY) {
                        sb.append(" >= ");
                    }
                    else {
                        sb.append(" > ");
                    }
                    sb.append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(domain.getRanges().getRanges().get(i).getLow().getValue()), column.getCassandraType()));
                }
            }
            addedColumns.add(column);
        }
        return result;
    }

    /**
     * checks if column with previous ordinal number is included as a predicate
     * using a loop over TupleDomains
     */
    private static boolean checkPreviousClusteredKey(CassandraColumnHandle column, Map<ConnectorColumnHandle, Domain> domains)
    {
        boolean result = true;
        for (int i = 2; i < column.getOrdinalPosition(); i++) {
            boolean found = false;
            for (Map.Entry<ConnectorColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle tmpColumn = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (tmpColumn.getOrdinalPosition() == i && tmpColumn.isClusteringKey()) {
                    boolean rangeRestricted = false;
                    if (domain.getRanges() != null && domain.getRanges().getRangeCount() == 1) {
                        for (int j = 0; j < domain.getRanges().getRangeCount(); j++) {
                            if (domain.getRanges().getRanges().get(j).getHigh().getValue() != null
                                    || domain.getRanges().getRanges().get(j).getLow().getValue() != null) {
                                // we have bounds, so this is a match
                                rangeRestricted = true;
                            }
                        }
                    }
                    if (domain.isSingleValue() || rangeRestricted) {
                        found = true;
                    }
                }
            }
            if (!found) {
                result = false;
                break;
            }
        }
        return result;
    }

    private List<CassandraPartition> getCassandraPartitions(final CassandraTable table, TupleDomain<ConnectorColumnHandle> tupleDomain)
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
                throw new PrestoException(EXTERNAL.toErrorCode(), "Error fetching cassandra partitions", e);
            }
        }

        return partitions.build();
    }

    private static Set<List<Comparable<?>>> getPartitionKeysSet(CassandraTable table, TupleDomain<ConnectorColumnHandle> tupleDomain)
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

                // todo should we just skip partition pruning instead of throwing an exception?
                checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                        "Only Boolean, String, Double and Long partition keys are supported");

                columnValues.add(value);
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
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<CassandraPartition> partitionMatches(final TupleDomain<ConnectorColumnHandle> tupleDomain)
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
