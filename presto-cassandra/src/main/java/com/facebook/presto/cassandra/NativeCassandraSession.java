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
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.Select.Where;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

// TODO: Refactor this class to make it be "single responsibility"
public class NativeCassandraSession
        implements CassandraSession
{
    private static final Logger log = Logger.get(NativeCassandraSession.class);

    static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    protected final String connectorId;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final Cluster cluster;
    private final Supplier<Session> session;
    private final Duration noHostAvailableRetryTimeout;

    public NativeCassandraSession(String connectorId, JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec, Cluster cluster, Duration noHostAvailableRetryTimeout)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
        this.cluster = requireNonNull(cluster, "cluster is null");
        this.noHostAvailableRetryTimeout = requireNonNull(noHostAvailableRetryTimeout, "noHostAvailableRetryTimeout is null");
        this.session = memoize(cluster::connect);
    }

    @Override
    public Set<Host> getReplicas(String schemaName, ByteBuffer partitionKey)
    {
        return executeWithSession(session -> session.getCluster().getMetadata().getReplicas(schemaName, partitionKey));
    }

    @Override
    public List<String> getAllSchemas()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        for (KeyspaceMetadata meta : keyspaces) {
            builder.add(meta.getName());
        }
        return builder.build();
    }

    @Override
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
        KeyspaceMetadata keyspaceMetadata = executeWithSession(session -> session.getCluster().getMetadata().getKeyspace(schema));
        if (keyspaceMetadata == null) {
            throw new SchemaNotFoundException(schema);
        }
        return keyspaceMetadata;
    }

    @Override
    public void getSchema(String schema)
            throws SchemaNotFoundException
    {
        getCheckedKeyspaceMetadata(schema);
    }

    @Override
    public CassandraTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        TableMetadata tableMeta = getTableMetadata(tableName);

        List<String> columnNames = new ArrayList<>();
        for (ColumnMetadata columnMetadata : tableMeta.getColumns()) {
            columnNames.add(columnMetadata.getName());
        }

        // check if there is a comment to establish column ordering
        String comment = tableMeta.getOptions().getComment();
        Set<String> hiddenColumns = ImmutableSet.of();
        if (comment != null && comment.startsWith(PRESTO_COMMENT_METADATA)) {
            String columnOrderingString = comment.substring(PRESTO_COMMENT_METADATA.length());

            // column ordering
            List<ExtraColumnMetadata> extras = extraColumnMetadataCodec.fromJson(columnOrderingString);
            List<String> explicitColumnOrder = new ArrayList<>(ImmutableList.copyOf(transform(extras, ExtraColumnMetadata::getName)));
            hiddenColumns = ImmutableSet.copyOf(transform(filter(extras, ExtraColumnMetadata::isHidden), ExtraColumnMetadata::getName));

            // add columns not in the comment to the ordering
            Iterables.addAll(explicitColumnOrder, filter(columnNames, not(in(explicitColumnOrder))));

            // sort the actual columns names using the explicit column order (this allows for missing columns)
            columnNames = Ordering.explicit(explicitColumnOrder).sortedCopy(columnNames);
        }

        ImmutableList.Builder<CassandraColumnHandle> columnHandles = ImmutableList.builder();

        // add primary keys first
        Set<String> primaryKeySet = new HashSet<>();
        for (ColumnMetadata columnMeta : tableMeta.getPartitionKey()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName());
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, true, false, columnNames.indexOf(columnMeta.getName()), hidden);
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName());
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, true, columnNames.indexOf(columnMeta.getName()), hidden);
            columnHandles.add(columnHandle);
        }

        // add other columns
        for (ColumnMetadata columnMeta : tableMeta.getColumns()) {
            if (!primaryKeySet.contains(columnMeta.getName())) {
                boolean hidden = hiddenColumns.contains(columnMeta.getName());
                CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, false, columnNames.indexOf(columnMeta.getName()), hidden);
                columnHandles.add(columnHandle);
            }
        }

        List<CassandraColumnHandle> sortedColumnHandles = columnHandles.build().stream()
                .sorted(comparing(CassandraColumnHandle::getOrdinalPosition))
                .collect(toList());

        CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableMeta.getKeyspace().getName(), tableMeta.getName());
        return new CassandraTable(tableHandle, sortedColumnHandles);
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

    private CassandraColumnHandle buildColumnHandle(TableMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
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
        boolean indexed = false;
        for (IndexMetadata idx : tableMetadata.getIndexes()) {
            if (idx.getTarget().equals(columnMeta.getName())) {
                indexed = true;
                break;
            }
        }
        return new CassandraColumnHandle(connectorId, columnMeta.getName(), ordinalPosition, cassandraType, typeArguments, partitionKey, clusteringKey, indexed, hidden);
    }

    @Override
    public List<CassandraPartition> getPartitions(CassandraTable table, List<Object> filterPrefix)
    {
        Iterable<Row> rows = queryPartitionKeys(table, filterPrefix);
        if (rows == null) {
            // just split the whole partition range
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
        }

        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        HashMap<ColumnHandle, NullableValue> map = new HashMap<>();
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
                NullableValue keyPart = CassandraType.getColumnValueForPartitionKey(row, i, columnHandle.getCassandraType(), columnHandle.getTypeArguments());
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
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(map);
            String partitionId = stringBuilder.toString();
            if (uniquePartitionIds.add(partitionId)) {
                partitions.add(new CassandraPartition(key, partitionId, tupleDomain, false));
            }
        }
        return partitions.build();
    }

    protected Iterable<Row> queryPartitionKeys(CassandraTable table, List<Object> filterPrefix)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        if (filterPrefix.size() != partitionKeyColumns.size()) {
            return null;
        }

        Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        addWhereClause(partitionKeys.where(), partitionKeyColumns, filterPrefix);
        return executeWithSession(session -> session.execute(partitionKeys)).all();
    }

    @Override
    public <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        ReconnectionPolicy reconnectionPolicy = cluster.getConfiguration().getPolicies().getReconnectionPolicy();
        ReconnectionSchedule schedule = reconnectionPolicy.newSchedule();
        long deadline = System.currentTimeMillis() + noHostAvailableRetryTimeout.toMillis();
        while (true) {
            try {
                return sessionCallable.executeWithSession(session.get());
            }
            catch (NoHostAvailableException e) {
                long timeLeft = deadline - System.currentTimeMillis();
                if (timeLeft <= 0) {
                    throw e;
                }
                else {
                    long delay = Math.min(schedule.nextDelayMs(), timeLeft);
                    log.warn(e.getCustomMessage(10, true, true));
                    log.warn("Reconnecting in %dms", delay);
                    try {
                        Thread.sleep(delay);
                    }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("interrupted", interrupted);
                    }
                }
            }
        }
    }

    private static void addWhereClause(Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Object> filterPrefix)
    {
        for (int i = 0; i < filterPrefix.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            Object value = column.getCassandraType().getJavaValue(filterPrefix.get(i));
            Clause clause = QueryBuilder.eq(CassandraCqlUtils.validColumnName(column.getName()), value);
            where.and(clause);
        }
    }
}
