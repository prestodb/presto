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

import static com.facebook.presto.cassandra.CassandraColumnHandle.columnMetadataGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.transform;
import io.airlift.log.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

public class CassandraClient
        implements ConnectorMetadata, ConnectorSplitManager, ConnectorRecordSetProvider, ConnectorHandleResolver
{
    private static final Logger log = Logger.get(CassandraClient.class);

    private final String connectorId;
    private final CassandraSession cassandraSession;
    private final CachingCassandraSchemaProvider schemaProvider;
    private final int unpartitionedSplits;
    
    @Inject
    public CassandraClient(CassandraConnectorId connectorId,
    		CassandraClientConfig cassandraClientConfig,
    		CassandraSession cassandraSession,
            CachingCassandraSchemaProvider schemaProvider)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        
        this.unpartitionedSplits = cassandraClientConfig.getUnpartitionedSplits();
    }

    public CachingCassandraSchemaProvider getSchemaProvider()
    {
        return schemaProvider;
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return schemaProvider.getAllSchemas();
    }

    @Override
    public CassandraTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try {
            final CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
            schemaProvider.getTable(tableHandle);
			return tableHandle;
        }
        catch (NoSuchObjectException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of HiveTableHandle");
        return ((CassandraTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            final CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
            CassandraTable table = schemaProvider.getTable(tableHandle);
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(table.getColumns(), columnMetadataGetter()));
            return new ConnectorTableMetadata(tableName, columns);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(schemaNameOrNull)) {
            try {
                for (String tableName : schemaProvider.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase()));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames();
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnName, "columnName is null");
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        try {
            CassandraTable table = schemaProvider.getTable((CassandraTableHandle) tableHandle);
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            for (CassandraColumnHandle columnHandle : table.getColumns()) {
                columnHandles.put(CassandraCqlUtils.cqlNameToSqlName(columnHandle.getName()), columnHandle);
            }
            return columnHandles.build();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(getTableName(tableHandle));
        }
    }

	@Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of HiveTableHandle");
        checkArgument(columnHandle instanceof CassandraColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
        return ((CassandraColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }
    
/*
	@Override
	public OutputTableHandle beginCreateTable(ConnectorTableMetadata arg0) {
        throw new UnsupportedOperationException();
	}

	@Override
	public boolean canHandle(OutputTableHandle arg0) {
		return false;
	}

	@Override
	public void commitCreateTable(OutputTableHandle arg0, Collection<String> arg1) {
        throw new UnsupportedOperationException();
	}
*/
    
    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;

        final CassandraTable table;
        
        try {
            table = schemaProvider.getTable(cassandraTableHandle);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(getTableName(tableHandle));
        }

        final List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();
        List<Comparable<?>> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
        	CassandraColumnHandle columnHandle = partitionKeys.get(i);

            // only add to prefix if all previous keys have a value
            if (filterPrefix.size() == i && !tupleDomain.isNone()) {
                Domain domain = tupleDomain.getDomains().get(columnHandle);
                if (domain != null && domain.getRanges().getRangeCount() == 1) {
                    // We intentionally ignore whether NULL is in the domain since partition keys can never be NULL
                    Range range = Iterables.getOnlyElement(domain.getRanges());
                    if (range.isSingleValue()) {
                        Comparable<?> value = range.getLow().getValue();
                        checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                                "Only Boolean, String, Double and Long partition keys are supported");
                        filterPrefix.add(value);
                    }
                }
            }
        }

        final List<CassandraPartition> allPartitions;
        // fetch the partitions
        try {
        	allPartitions = schemaProvider.getPartitions(table, filterPrefix);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(getTableName(tableHandle));
        }
        
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<Partition> partitions = FluentIterable.from(allPartitions)
                .filter(partitionMatches(tupleDomain))
                .filter(Partition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
			final List<ColumnHandle> x = (List) partitionKeys;
			remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(x))));
        }

        return new PartitionResult(partitions, remainingTupleDomain);
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");

        Partition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return ImmutableList.of();
        }
        
        CassandraTableHandle cassTableHandle = (CassandraTableHandle) tableHandle;
        
        checkArgument(partition instanceof CassandraPartition, "partitions are no CassandraPartitions");
        CassandraPartition cpartition = (CassandraPartition) partition;
        final List<Split> splits;
        if (cpartition.isUnpartitioned()) {
        	CassandraTable table;
        	try {
        		table = schemaProvider.getTable(cassTableHandle);
	        }
	        catch (NoSuchObjectException e) {
	            throw new TableNotFoundException(getTableName(tableHandle));
	        }
        	splits = getSplitsByTokenRange(table, cpartition.getPartitionId());
        } else {
        	splits = getSplits(cassTableHandle, partitions);
        }
    	return splits;
    }


	private List<Split> getSplitsByTokenRange(CassandraTable table,
			String partitionId) {
		final String schema = table.getTableHandle().getSchemaName();
		final String tableName = table.getTableHandle().getTableName();
		CassandraCqlUtils util = new CassandraCqlUtils();
		ImmutableList.Builder<Split> builder = ImmutableList.builder();
		
		final String tokenExpression = table.getTokenExpression();
		List<HostAddress> addresses = util.toHostAddressList(cassandraSession.getAllHosts());
		BigInteger start = BigInteger.valueOf(Long.MIN_VALUE);
		BigInteger end = BigInteger.valueOf(Long.MAX_VALUE);
		final BigInteger one = BigInteger.valueOf(1);
		final BigInteger splits = BigInteger.valueOf(unpartitionedSplits);
		long delta = end.subtract(start).subtract(one).divide(splits).longValue();
		long startToken = start.longValue();
		long endToken;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < unpartitionedSplits - 1; i++) {
			endToken = startToken + delta;
			String condition = buildTokenCondition(sb, tokenExpression, startToken, endToken);
			CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, false, addresses);
			builder.add(split);
			startToken = endToken + 1;
		}
		String condition = buildTokenCondition(sb, tokenExpression, startToken, end.longValue());
		CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, true, addresses);
		builder.add(split);
		
		return builder.build();
	}

	private String buildTokenCondition(StringBuilder sb, String tokenExpression, long startToken,
			long endToken) {
		sb.setLength(0);
		sb.append(tokenExpression).append(" >= ").append(startToken);
		sb.append(" AND ");
		sb.append(tokenExpression).append(" <= ").append(endToken);
		return sb.toString();
	}

	private List<Split> getSplits(CassandraTableHandle cassTableHandle, List<Partition> partitions) {
		final String schema = cassTableHandle.getSchemaName();
		final String table = cassTableHandle.getTableName();
		CassandraCqlUtils util = new CassandraCqlUtils();
		ImmutableList.Builder<Split> builder = ImmutableList.builder();
		for (Partition p: partitions) {
			CassandraPartition partition = (CassandraPartition) p;
			Set<Host> hosts = cassandraSession.getReplicas(schema, partition.getKeyAsByteBuffer());
			List<HostAddress> addresses = util.toHostAddressList(hosts);
			CassandraSplit split = new CassandraSplit(connectorId, schema, table, partition.getPartitionId(), null, true, addresses);
			builder.add(split);
		}
		return builder.build();
	}


    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(split instanceof CassandraSplit, "expected instance of %s: %s", CassandraSplit.class, split.getClass());

        List<CassandraColumnHandle> cassandraColumns = ImmutableList.copyOf(transform(columns, CassandraColumnHandle.cassandraColumnHandle()));
        
        CassandraSplit cassandraSplit = (CassandraSplit) split;
        
        String selectCql = CassandraCqlUtils.selectFrom(cassandraSplit.getCassandraTableHandle(), cassandraColumns).getQueryString();
        StringBuilder sb = new StringBuilder(selectCql);
        if (sb.charAt(sb.length() - 1) == ';')
        	sb.setLength(sb.length() - 1);
        sb.append(cassandraSplit.getWhereClause());
        String cql = sb.toString();
        log.debug("Creating record set: %s", cql);
        return new CassandraRecordSet(cql, cassandraColumns, cassandraSession);
    }
    
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof CassandraTableHandle && ((CassandraTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof CassandraColumnHandle && ((CassandraColumnHandle) columnHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof CassandraSplit && ((CassandraSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return CassandraTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return CassandraColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return CassandraSplit.class;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<CassandraPartition> partitionMatches(final TupleDomain tupleDomain)
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
