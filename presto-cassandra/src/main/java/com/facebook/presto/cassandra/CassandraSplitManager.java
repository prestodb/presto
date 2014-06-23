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
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public class CassandraSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(ConnectorSplitManager.class);

    private final String connectorId;
    private final CassandraSession cassandraSession;
    private final CachingCassandraSchemaProvider schemaProvider;
    private final int unpartitionedSplits;

    @Inject
    public CassandraSplitManager(CassandraConnectorId connectorId,
            CassandraClientConfig cassandraClientConfig,
            CassandraSession cassandraSession,
            CachingCassandraSchemaProvider schemaProvider)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.unpartitionedSplits = cassandraClientConfig.getUnpartitionedSplits();
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;
        CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<CassandraPartition> allPartitions = getAllPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<ConnectorPartition> partitions = FluentIterable.from(allPartitions)
                .filter(partitionMatches(tupleDomain))
                .filter(ConnectorPartition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ConnectorColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            List<ConnectorColumnHandle> partitionColumns = (List) partitionKeys;
            remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionColumns))));
        }

        return new ConnectorPartitionResult(partitions, remainingTupleDomain);
    }

    /** get all partitions for the domain.*/
    private List<CassandraPartition> getAllPartitions(CassandraTable table, TupleDomain tupleDomain)
    {
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();
        List<Comparable<?>> emptyPrefix = new ArrayList<>();
        Stack<List<Comparable<?>>> stack = new Stack<>();
        stack.add(emptyPrefix);
        boolean partialPartitionKeys = false;
        if (!tupleDomain.isNone()) {
            for (int i = 0; i < partitionKeys.size(); i++) {
                CassandraColumnHandle columnHandle = partitionKeys.get(i);
                Domain domain = tupleDomain.getDomains().get(columnHandle);
                if (domain != null) {
                    List<Range> ranges = domain.getRanges().getRanges();
                    Stack<List<Comparable<?>>> tempStack = new Stack<>();
                    while (!stack.isEmpty()) {
                        if (stack.peek().size() == i) {
                            List<Comparable<?>> filter = stack.pop();
                            int j = 0;
                            //multiple ranges
                            for (Range range : ranges) {
                                if (range.isSingleValue()) {
                                    Comparable<?> value = range.getSingleValue();
                                    checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                                            "Only Boolean, String, Double and Long partition keys are supported");
                                    if (j == ranges.size() - 1) {
                                        filter.add(value);
                                        tempStack.add(filter);
                                    }
                                    else {
                                        List<Comparable<?>> filterCopy = new ArrayList<>(filter);
                                        if (filter.size() > 0) {
                                            Collections.copy(filterCopy, filter);
                                        }
                                        filterCopy.add(value);
                                        tempStack.add(filterCopy);
                                    }
                                }
                            }
                        }
                        else {
                            partialPartitionKeys = true;
                            break;
                        }
                    }
                    // if there is only partial partition keys, return empty filter
                    if (partialPartitionKeys) {
                        stack.clear();
                        stack.add(emptyPrefix);
                        break;
                    }
                    if (tempStack.size() != 0) {
                        stack = tempStack;
                    }
                }
                else {
                    stack.clear();
                    stack.add(emptyPrefix);
                    break;
                }
            }
        }

        List<CassandraPartition> allPartitions = new ArrayList<>();
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        List<ListenableFuture<List<CassandraPartition>>> getPartitionResults = Lists.newArrayList();
        final CassandraTable cassandraTable = table;
        while (!stack.isEmpty()) {
            final List<Comparable<?>> filter = stack.pop();
            getPartitionResults.add(service.submit(new Callable<List<CassandraPartition>>()
                    {
                        public List<CassandraPartition> call()
                        {
                            return schemaProvider.getPartitions(cassandraTable, filter);
                        }
                    }));
        }

        for (ListenableFuture<List<CassandraPartition>> result : getPartitionResults) {
            try {
                Collections.addAll(allPartitions, result.get().toArray(new CassandraPartition[0]));
            }
            catch (InterruptedException | ExecutionException e) {
                log.error("Error in get partitions", e);
            }
        }

        return allPartitions;
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of CassandraTableHandle");
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            ConnectorPartition partition = partitions.get(0);
            checkArgument(partition instanceof CassandraPartition, "partitions are no CassandraPartitions");
            CassandraPartition cassandraPartition = (CassandraPartition) partition;

            if (cassandraPartition.isUnpartitioned()) {
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

        List<HostAddress> addresses = new HostAddressFactory().toHostAddressList(cassandraSession.getAllHosts());

        BigInteger start = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger end = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger one = BigInteger.valueOf(1);
        BigInteger splits = BigInteger.valueOf(unpartitionedSplits);
        long delta = end.subtract(start).subtract(one).divide(splits).longValue();
        long startToken = start.longValue();

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (int i = 0; i < unpartitionedSplits - 1; i++) {
            long endToken = startToken + delta;
            String condition = buildTokenCondition(tokenExpression, startToken, endToken);

            CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, addresses);
            builder.add(split);

            startToken = endToken + 1;
        }

        // special handling for last split
        String condition = buildTokenCondition(tokenExpression, startToken, end.longValue());
        CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, addresses);
        builder.add(split);

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, long startToken, long endToken)
    {
        return tokenExpression + " >= " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    private List<ConnectorSplit> getSplitsForPartitions(CassandraTableHandle cassTableHandle, List<ConnectorPartition> partitions)
    {
        String schema = cassTableHandle.getSchemaName();
        String table = cassTableHandle.getTableName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (ConnectorPartition partition : partitions) {
            checkArgument(partition instanceof CassandraPartition, "partitions are no CassandraPartitions");
            CassandraPartition cassandraPartition = (CassandraPartition) partition;

            Set<Host> hosts = cassandraSession.getReplicas(schema, cassandraPartition.getKeyAsByteBuffer());
            List<HostAddress> addresses = hostAddressFactory.toHostAddressList(hosts);
            CassandraSplit split = new CassandraSplit(connectorId, schema, table, cassandraPartition.getPartitionId(), null, addresses);
            builder.add(split);
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
