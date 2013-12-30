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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

public class CassandraSession {
	protected final String connectorId;
	private final Session session;
    private final int limitForPartitionKeySelect;
    private final int fetchSizeForPartitionKeySelect;
    
	public CassandraSession(String connectorId, Session session, CassandraClientConfig config) {
		this.connectorId = connectorId;
		this.session = session;
		this.fetchSizeForPartitionKeySelect = config.getFetchSizeForPartitionKeySelect();
        this.limitForPartitionKeySelect = config.getLimitForPartitionKeySelect();
	}

	public Set<Host> getReplicas(String schema, ByteBuffer partitionKey) {
		return session.getCluster().getMetadata().getReplicas(schema, partitionKey);
	}

	public ResultSet executeQuery(String cql) {
		return session.execute(cql);
	}

	public Collection<Host> getAllHosts() {
		return session.getCluster().getMetadata().getAllHosts();
	}

	public List<String> getAllSchemas() {
		ImmutableList.Builder<String> builder = ImmutableList.builder();
		for (KeyspaceMetadata meta: session.getCluster().getMetadata().getKeyspaces()) {
			builder.add(meta.getName());
		}
		return builder.build();
	}

	public List<String> getAllTables(String schema) throws NoSuchObjectException {
		KeyspaceMetadata meta = getCheckedKeyspaceMetadata(schema);
		ImmutableList.Builder<String> builder = ImmutableList.builder();
		for (TableMetadata tableMeta: meta.getTables()) {
			builder.add(tableMeta.getName());
		}
		return builder.build();
	}

	private KeyspaceMetadata getCheckedKeyspaceMetadata(String schema) throws NoSuchObjectException {
		KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(schema);
		if (meta == null)
			throw new NoSuchObjectException("Schema " + schema + " does not exist");
		return meta;
	}

	public void getSchema(String schema) throws NoSuchObjectException {
		getCheckedKeyspaceMetadata(schema);
	}

	public CassandraTable getTable(SchemaTableName tableName) throws NoSuchObjectException {
		String schema = tableName.getSchemaName();
		String table = tableName.getTableName();
		KeyspaceMetadata meta = getCheckedKeyspaceMetadata(schema);
		TableMetadata tableMeta = meta.getTable(table);
		if (tableMeta == null) {
			// search for table with different upper/lowercase name
			for (TableMetadata tmpMeta: meta.getTables()) {
				if (tmpMeta.getName().equalsIgnoreCase(table)) {
					tableMeta = tmpMeta;
					break;
				}
			}
			if (tableMeta == null)
				throw new NoSuchObjectException("Table " + schema + "." + table + " does not exist");
		}

		ArrayList<CassandraColumnHandle> list = new ArrayList<>();
		Set<String> primaryKeySet = new HashSet<>();
		int index = 0;
		for (ColumnMetadata columnMeta: tableMeta.getPartitionKey()) {
			primaryKeySet.add(columnMeta.getName());
			CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, true, false, index++);
			list.add(columnHandle);
		}
		index = 0;
		for (ColumnMetadata columnMeta: tableMeta.getClusteringColumns()) {
			primaryKeySet.add(columnMeta.getName());
			CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, false, true, index++);
			list.add(columnHandle);
		}
		for (ColumnMetadata columnMeta: tableMeta.getColumns()) {
			if (!primaryKeySet.contains(columnMeta.getName())) {
				CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, false, false, 0);
				list.add(columnHandle);
			}
		}
		CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableMeta.getKeyspace().getName(), tableMeta.getName());
		return new CassandraTable(tableHandle, list);
	}

	private CassandraColumnHandle buildColumnHandle(ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int index) {
		CassandraType cassandraType = CassandraType.getCassandraType(columnMeta.getType().getName());
		List<CassandraType> typeArguments = null;
		if (cassandraType != null && cassandraType.getTypeArgumentSize() > 0) {
			final List<DataType> typeArgs = columnMeta.getType().getTypeArguments();
			switch (cassandraType.getTypeArgumentSize()) {
			case 1:
				typeArguments = ImmutableList.of(CassandraType.getCassandraType(typeArgs.get(0).getName()));
				break;
			case 2:
				typeArguments = ImmutableList.of(CassandraType.getCassandraType(typeArgs.get(0).getName()), CassandraType.getCassandraType(typeArgs.get(1).getName()));
				break;
			}
		}
		return new CassandraColumnHandle(connectorId, columnMeta.getName(), index, cassandraType, typeArguments,
				partitionKey, clusteringKey);
	}

	public List<CassandraPartition> getPartitions(CassandraTable table, List<Comparable<?>> filterPrefix)
			throws NoSuchObjectException {
		Iterable<Row> rows = queryPartitionKeys(table, filterPrefix);
		if (rows == null) {
			// just split the whole partition range
			return ImmutableList.of(CassandraPartition.UNPARTITIONED);
		} else {
			final List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();
			final int count = partitionKeyColumns.size();
			ImmutableList.Builder<CassandraPartition> builder = ImmutableList.builder();
			ByteBuffer buffer = ByteBuffer.allocate(1000);
			HashMap<ColumnHandle, Comparable<?>> map = new HashMap<>();
			Set<String> uniquePartitionIds = new HashSet<>();
			StringBuilder sb = new StringBuilder();
			boolean isComposite = count > 1;
			for (Row row : rows) {
				buffer.clear();
				map.clear();
				sb.setLength(0);
				for (int i = 0; i < count; i++) {
					final ByteBuffer component = row.getBytesUnsafe(i);
					if (isComposite) {
						// build composite key
						short len = (short) component.limit();
						buffer.putShort(len);
						buffer.put(component);
						buffer.put((byte) 0);
					} else {
						buffer.put(component);
					}
					CassandraColumnHandle columnHandle = partitionKeyColumns.get(i);
					Comparable<?> keyPart = CassandraType.getColumnValue(row, i, columnHandle.getCassandraType(), columnHandle.getTypeArguments());
					map.put(columnHandle, keyPart);
					if (i > 0)
						sb.append(" AND ");
					sb.append(CassandraCqlUtils.validColumnName(columnHandle.getName()));
					sb.append(" = ");
					sb.append(CassandraType.getColumnValueForCql(row, i, columnHandle.getCassandraType()));
				}
				buffer.flip();
				byte[] key = new byte[buffer.limit()];
				buffer.get(key);
				TupleDomain tupleDomain = TupleDomain.withFixedValues(map);
				String partitionId = sb.toString();
				if (uniquePartitionIds.add(partitionId))
					builder.add(new CassandraPartition(key, partitionId, tupleDomain));
			}
			return builder.build();
		}
	}

	protected Iterable<Row> queryPartitionKeys(CassandraTable table, List<Comparable<?>> filterPrefix) {
		final CassandraTableHandle tableHandle = table.getTableHandle();
		final List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();
		boolean fullPartitionKey = filterPrefix.size() == partitionKeyColumns.size();
		final ResultSetFuture countFuture;
		if (!fullPartitionKey) {
			Select countAll = CassandraCqlUtils.selectCountAllFrom(tableHandle).limit(limitForPartitionKeySelect);
			countFuture = session.executeAsync(countAll);
		} else {
			// no need to count if partition key is completely known
			countFuture = null;
		}
		
		int limit = fullPartitionKey ? 1 : limitForPartitionKeySelect;
		Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
		partitionKeys.limit(limit);
		partitionKeys.setFetchSize(fetchSizeForPartitionKeySelect);
		addWhereClause(partitionKeys.where(), partitionKeyColumns, filterPrefix);
		final ResultSetFuture partitionKeyFuture = session.executeAsync(partitionKeys);

		if (!fullPartitionKey) {
			long count = countFuture.getUninterruptibly().one().getLong(0);
			if (count == limitForPartitionKeySelect) {
				partitionKeyFuture.cancel(true);
				return null; // too much effort to query all partition keys
			}
		}
		return partitionKeyFuture.getUninterruptibly();
	}

	private void addWhereClause(Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Comparable<?>> filterPrefix) {
		for (int i = 0; i < filterPrefix.size(); i++) {
			CassandraColumnHandle column = partitionKeyColumns.get(i);
			Object value = column.getCassandraType().getJavaValue(filterPrefix.get(i));
			Clause clause = QueryBuilder.eq(CassandraCqlUtils.validColumnName(column.getName()), value);
			where.and(clause);
		}
	}

}
