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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.cassandra.CassandraSessionProperties.getSplitsPerNode;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CassandraSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final CassandraSession cassandraSession;
    private final int partitionSizeForBatchSelect;
    private final CassandraTokenSplitManager tokenSplitMgr;

    @Inject
    public CassandraSplitManager(
            CassandraConnectorId connectorId,
            CassandraClientConfig cassandraClientConfig,
            CassandraSession cassandraSession,
            CassandraTokenSplitManager tokenSplitMgr)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.partitionSizeForBatchSelect = cassandraClientConfig.getPartitionSizeForBatchSelect();
        this.tokenSplitMgr = tokenSplitMgr;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        CassandraTableLayoutHandle layoutHandle = (CassandraTableLayoutHandle) layout;
        CassandraTableHandle cassandraTableHandle = layoutHandle.getTable();
        List<CassandraPartition> partitions = layoutHandle.getPartitions();

        if (partitions.isEmpty()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            CassandraPartition cassandraPartition = partitions.get(0);
            if (cassandraPartition.isUnpartitioned() || cassandraPartition.isIndexedColumnPredicatePushdown()) {
                CassandraTable table = cassandraSession.getTable(cassandraTableHandle.getSchemaTableName());
                List<ConnectorSplit> splits = getSplitsByTokenRange(table, cassandraPartition.getPartitionId(), getSplitsPerNode(session));
                return new FixedSplitSource(splits);
            }
        }

        return new FixedSplitSource(getSplitsForPartitions(cassandraTableHandle, partitions, layoutHandle.getClusteringPredicates()));
    }

    private List<ConnectorSplit> getSplitsByTokenRange(CassandraTable table, String partitionId, Optional<Long> sessionSplitsPerNode)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        List<CassandraTokenSplitManager.TokenSplit> tokenSplits = tokenSplitMgr.getSplits(schema, tableName, sessionSplitsPerNode);
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

    private List<ConnectorSplit> getSplitsForPartitions(CassandraTableHandle cassTableHandle, List<CassandraPartition> partitions, String clusteringPredicates)
    {
        String schema = cassTableHandle.getSchemaName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        // For single partition key column table, we can merge multiple partitions into a single split
        // by using IN CLAUSE in a single select query if the partitions have the same host list.
        // For multiple partition key columns table, we can't merge them into a single select query, so
        // keep them in a separate split.
        boolean singlePartitionKeyColumn = true;
        String partitionKeyColumnName = null;
        if (!partitions.isEmpty()) {
            singlePartitionKeyColumn = partitions.get(0).getTupleDomain().getDomains().get().size() == 1;
            if (singlePartitionKeyColumn) {
                String partitionId = partitions.get(0).getPartitionId();
                partitionKeyColumnName = partitionId.substring(0, partitionId.lastIndexOf('=') - 1);
            }
        }
        Map<Set<String>, Set<String>> hostsToPartitionKeys = new HashMap<>();
        Map<Set<String>, List<HostAddress>> hostMap = new HashMap<>();

        for (CassandraPartition cassandraPartition : partitions) {
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
                    values = new HashSet<>();
                }
                String partitionId = cassandraPartition.getPartitionId();
                values.add(partitionId.substring(partitionId.lastIndexOf('=') + 2));
                hostsToPartitionKeys.put(hostAddresses, values);
                hostMap.put(hostAddresses, addresses);
            }
            else {
                builder.add(createSplitForClusteringPredicates(cassTableHandle, cassandraPartition.getPartitionId(), addresses, clusteringPredicates));
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
                        builder.add(createSplitForClusteringPredicates(cassTableHandle, partitionId, hostMap.get(entry.getKey()), clusteringPredicates));
                        size = 0;
                        sb.setLength(0);
                        sb.trimToSize();
                    }
                }
                if (size > 0) {
                    String partitionId = String.format("%s in (%s)", partitionKeyColumnName, sb.toString());
                    builder.add(createSplitForClusteringPredicates(cassTableHandle, partitionId, hostMap.get(entry.getKey()), clusteringPredicates));
                }
            }
        }
        return builder.build();
    }

    private CassandraSplit createSplitForClusteringPredicates(
            CassandraTableHandle tableHandle,
            String partitionId,
            List<HostAddress> hosts,
            String clusteringPredicates)
    {
        String schema = tableHandle.getSchemaName();
        String table = tableHandle.getTableName();

        if (clusteringPredicates.isEmpty()) {
            return new CassandraSplit(connectorId, schema, table, partitionId, null, hosts);
        }

        return new CassandraSplit(connectorId, schema, table, partitionId, clusteringPredicates, hosts);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }
}
