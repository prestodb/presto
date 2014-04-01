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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.Select.Where;

public class CassandraSession
{
    protected final String connectorId;
    private final Cluster.Builder clusterBuilder;
    private final int fetchSizeForPartitionKeySelect;
    private final int limitForPartitionKeySelect;

    private Session session;

    public CassandraSession(String connectorId, Cluster.Builder clusterBuilder, int fetchSizeForPartitionKeySelect, int limitForPartitionKeySelect)
    {
        this.connectorId = connectorId;
        this.clusterBuilder = clusterBuilder;
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;

        if (clusterBuilder != null) {
            this.session = clusterBuilder.build().connect();
        }
    }

    public Set<Host> getReplicas(String schema, ByteBuffer partitionKey)
    {
        return session.getCluster().getMetadata().getReplicas(schema, partitionKey);
    }

    public ResultSet executeQuery(String cql)
    {
        try {
            return session.execute(cql);
        }
        catch (NoHostAvailableException e) {
            // Something happened with our client connection.  We need to
            // re-establish the connection using our contact points.
            session = clusterBuilder.build().connect();
            return session.execute(cql);
        }
    }

    public Collection<Host> getAllHosts()
    {
        return session.getCluster().getMetadata().getAllHosts();
    }

    public List<String> getAllSchemas()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (KeyspaceMetadata meta : session.getCluster().getMetadata().getKeyspaces()) {
            builder.add(meta.getName());
        }
        return builder.build();
    }

    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata meta = getCheckedKeyspaceMetadata(schema);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata tableMeta : meta.getTables()) {
            builder.add(tableMeta.getName());
        }
        return builder.build();
    }

    private KeyspaceMetadata getCheckedKeyspaceMetadata(String schema)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(schema);
        if (meta == null) {
            throw new SchemaNotFoundException(schema);
        }
        return meta;
    }

    public void getSchema(String schema)
            throws SchemaNotFoundException
    {
        getCheckedKeyspaceMetadata(schema);
    }

    public CassandraTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        TableMetadata tableMeta = getTableMetadata(tableName);

        ImmutableList.Builder<CassandraColumnHandle> columnHandles = ImmutableList.builder();

        // add primary keys first
        Set<String> primaryKeySet = new HashSet<>();
        int index = 0;
        for (ColumnMetadata columnMeta : tableMeta.getPartitionKey()) {
            primaryKeySet.add(columnMeta.getName());
            CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, true, false, index++);
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        index = 0;
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns()) {
            primaryKeySet.add(columnMeta.getName());
            CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, false, true, index++);
            columnHandles.add(columnHandle);
        }

        // add other columns
        for (ColumnMetadata columnMeta : tableMeta.getColumns()) {
            if (!primaryKeySet.contains(columnMeta.getName())) {
                CassandraColumnHandle columnHandle = buildColumnHandle(columnMeta, false, false, 0);
                columnHandles.add(columnHandle);
            }
        }

        CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableMeta.getKeyspace().getName(), tableMeta.getName());
        return new CassandraTable(tableHandle, columnHandles.build());
    }

    private TableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        KeyspaceMetadata keyspaceMetadata = getCheckedKeyspaceMetadata(schemaName);
        TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
        if (tableMetadata != null) {
            return tableMetadata;
        }

        for (TableMetadata table : keyspaceMetadata.getTables()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        throw new TableNotFoundException(schemaTableName);
    }

    private CassandraColumnHandle buildColumnHandle(ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int index)
    {
        CassandraType cassandraType = CassandraType.getCassandraType(columnMeta.getType().getName());
        List<CassandraType> typeArguments = null;
        if (cassandraType != null && cassandraType.getTypeArgumentSize() > 0) {
            List<DataType> typeArgs = columnMeta.getType().getTypeArguments();
            switch (cassandraType.getTypeArgumentSize()) {
                case 1:
                    typeArguments = ImmutableList.of(CassandraType.getCassandraType(typeArgs.get(0).getName()));
                    break;
                case 2:
                    typeArguments = ImmutableList.of(CassandraType.getCassandraType(typeArgs.get(0).getName()), CassandraType.getCassandraType(typeArgs.get(1).getName()));
                    break;
                default:
                    throw new IllegalArgumentException("Invalid type arguments: " + typeArgs);
            }
        }
        boolean indexed = columnMeta.getIndex() != null;
        return new CassandraColumnHandle(connectorId, columnMeta.getName(), index, cassandraType, typeArguments, partitionKey, clusteringKey, indexed);
    }

    public List<CassandraPartition> getPartitions(CassandraTable table, List<Comparable<?>> filterPrefix)
    {
        Iterable<Row> rows;
        try {
            rows = queryPartitionKeys(table, filterPrefix);
        }
        catch (NoHostAvailableException e) {
            session = clusterBuilder.build().connect();
            rows = queryPartitionKeys(table, filterPrefix);
        }
        if (rows == null) {
            // just split the whole partition range
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
        }

        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        HashMap<ConnectorColumnHandle, Comparable<?>> map = new HashMap<>();
        Set<String> uniquePartitionIds = new HashSet<>();
        StringBuilder stringBuilder = new StringBuilder();

        boolean isComposite = partitionKeyColumns.size() > 1;

        ImmutableList.Builder<CassandraPartition> partitions = ImmutableList.builder();
        for (Row row : rows) {
            buffer.clear();
            map.clear();
            stringBuilder.setLength(0);
            for (int i = 0; i < partitionKeyColumns.size(); i++) {
                ByteBuffer component = row.getBytesUnsafe(i);
                if (isComposite) {
                    // build composite key
                    short len = (short) component.limit();
                    buffer.putShort(len);
                    buffer.put(component);
                    buffer.put((byte) 0);
                }
                else {
                    buffer.put(component);
                }
                CassandraColumnHandle columnHandle = partitionKeyColumns.get(i);
                Comparable<?> keyPart = CassandraType.getColumnValue(row, i, columnHandle.getCassandraType(), columnHandle.getTypeArguments());
                map.put(columnHandle, keyPart);
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }
                stringBuilder.append(CassandraCqlUtils.validColumnName(columnHandle.getName()));
                stringBuilder.append(" = ");
                stringBuilder.append(CassandraType.getColumnValueForCql(row, i, columnHandle.getCassandraType()));
            }
            buffer.flip();
            byte[] key = new byte[buffer.limit()];
            buffer.get(key);
            TupleDomain<ConnectorColumnHandle> tupleDomain = TupleDomain.withFixedValues(map);
            String partitionId = stringBuilder.toString();
            if (uniquePartitionIds.add(partitionId)) {
                partitions.add(new CassandraPartition(key, partitionId, tupleDomain, false));
            }
        }
        return partitions.build();
    }

    protected Iterable<Row> queryPartitionKeys(CassandraTable table, List<Comparable<?>> filterPrefix)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        boolean fullPartitionKey = filterPrefix.size() == partitionKeyColumns.size();
        ResultSetFuture countFuture;
        if (!fullPartitionKey) {
            Select countAll = CassandraCqlUtils.selectCountAllFrom(tableHandle).limit(limitForPartitionKeySelect);
            countFuture = session.executeAsync(countAll);
        }
        else {
            // no need to count if partition key is completely known
            countFuture = null;
        }

        int limit = fullPartitionKey ? 1 : limitForPartitionKeySelect;
        Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        partitionKeys.limit(limit);
        partitionKeys.setFetchSize(fetchSizeForPartitionKeySelect);

        if (!fullPartitionKey) {
            addWhereClause(partitionKeys.where(), partitionKeyColumns, new ArrayList<Comparable<?>>());
            ResultSetFuture partitionKeyFuture = session.executeAsync(partitionKeys);
            long count = countFuture.getUninterruptibly().one().getLong(0);
            if (count == limitForPartitionKeySelect) {
                partitionKeyFuture.cancel(true);
                return null; // too much effort to query all partition keys
            }
            else {
                return partitionKeyFuture.getUninterruptibly();
            }
        }
        else {
            addWhereClause(partitionKeys.where(), partitionKeyColumns, filterPrefix);
            ResultSetFuture partitionKeyFuture = session.executeAsync(partitionKeys);
            return partitionKeyFuture.getUninterruptibly();
        }
    }

    private static void addWhereClause(Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Comparable<?>> filterPrefix)
    {
        for (int i = 0; i < filterPrefix.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            Object value = column.getCassandraType().getJavaValue(filterPrefix.get(i));
            Clause clause = QueryBuilder.eq(CassandraCqlUtils.validColumnName(column.getName()), value);
            where.and(clause);
        }
    }

    public ResultSet execute(String query, Object... values)
    {
        return session.execute(query, values);
    }
}
