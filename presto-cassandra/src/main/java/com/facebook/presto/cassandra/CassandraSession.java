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

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
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
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.datastax.driver.core.querybuilder.Select.Where;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class CassandraSession
{
    static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    protected final String connectorId;
    private final int fetchSizeForPartitionKeySelect;
    private final int limitForPartitionKeySelect;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final int noHostAvailableRetryCount;

    private LoadingCache<String, Session> sessionBySchema;

    public CassandraSession(String connectorId,
            final Builder clusterBuilder,
            int fetchSizeForPartitionKeySelect,
            int limitForPartitionKeySelect,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            int noHostAvailableRetryCount)
    {
        this.connectorId = connectorId;
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;
        this.extraColumnMetadataCodec = extraColumnMetadataCodec;
        this.noHostAvailableRetryCount = noHostAvailableRetryCount;

        sessionBySchema = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Session>()
                {
                    @Override
                    public Session load(String key)
                            throws Exception
                    {
                        return clusterBuilder.build().connect();
                    }
                });
    }

    public Set<Host> getReplicas(final String schemaName, final ByteBuffer partitionKey)
    {
        return executeWithSession(schemaName, new SessionCallable<Set<Host>>()
        {
            @Override
            public Set<Host> executeWithSession(Session session)
            {
                return session.getCluster().getMetadata().getReplicas(schemaName, partitionKey);
            }
        });
    }

    private Session getSession(String schemaName)
    {
        try {
            return sessionBySchema.get(schemaName);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public ResultSet executeQuery(String schemaName, final String cql)
    {
        return executeWithSession(schemaName, new SessionCallable<ResultSet>() {
            @Override
            public ResultSet executeWithSession(Session session)
            {
                return session.execute(cql);
            }
        });
    }

    public ResultSet execute(String schemaName, final String cql, final Object... values)
    {
        return executeWithSession(schemaName, new SessionCallable<ResultSet>()
        {
            @Override
            public ResultSet executeWithSession(Session session)
            {
                return session.execute(cql, values);
            }
        });
    }

    public Collection<Host> getAllHosts()
    {
        return executeWithSession("", new SessionCallable<Collection<Host>>() {
            @Override
            public Collection<Host> executeWithSession(Session session)
            {
                return session.getCluster().getMetadata().getAllHosts();
            }
        });
    }

    public List<String> getAllSchemas()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<KeyspaceMetadata> keyspaces = executeWithSession("", new SessionCallable<List<KeyspaceMetadata>>() {
            @Override
            public List<KeyspaceMetadata> executeWithSession(Session session)
            {
                return session.getCluster().getMetadata().getKeyspaces();
            }
        });
        for (KeyspaceMetadata meta : keyspaces) {
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

    private KeyspaceMetadata getCheckedKeyspaceMetadata(final String schema)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspaceMetadata = executeWithSession(schema, new SessionCallable<KeyspaceMetadata>() {
            @Override
            public KeyspaceMetadata executeWithSession(Session session)
            {
                return session.getCluster().getMetadata().getKeyspace(schema);
            }
        });
        if (keyspaceMetadata == null) {
            throw new SchemaNotFoundException(schema);
        }
        return keyspaceMetadata;
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
        String schemaName = tableHandle.getSchemaName();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        boolean fullPartitionKey = filterPrefix.size() == partitionKeyColumns.size();
        ResultSetFuture countFuture;
        if (!fullPartitionKey) {
            final Select countAll = CassandraCqlUtils.selectCountAllFrom(tableHandle).limit(limitForPartitionKeySelect);
            countFuture = executeWithSession(schemaName, new SessionCallable<ResultSetFuture>() {
                @Override
                public ResultSetFuture executeWithSession(Session session)
                {
                    return session.executeAsync(countAll);
                }
            });
        }
        else {
            // no need to count if partition key is completely known
            countFuture = null;
        }

        int limit = fullPartitionKey ? 1 : limitForPartitionKeySelect;
        final Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        partitionKeys.limit(limit);
        partitionKeys.setFetchSize(fetchSizeForPartitionKeySelect);

        if (!fullPartitionKey) {
            Set<Host> clusterHosts = getSession(schemaName).getCluster().getMetadata().getAllHosts();
            if (!clusterHosts.isEmpty()) {
                VersionNumber version = clusterHosts.iterator().next().getCassandraVersion();
                // Cassandra 2.2 changes the functionality of COUNT(*) to be
                // more like SQL standard. COUNT(*) can no longer
                // be used to see if a table has < limitForPartitionKeySelect partitions.
                // See CASSANDRA-8216
                if (version.getMajor() >= 3 || (version.getMajor() == 2 && version.getMinor() >= 2)) {
                    return null;
                }
            }

            addWhereClause(partitionKeys.where(), partitionKeyColumns, new ArrayList<>());
            ResultSetFuture partitionKeyFuture = executeWithSession(schemaName, new SessionCallable<ResultSetFuture>() {
                @Override
                public ResultSetFuture executeWithSession(Session session)
                {
                    return session.executeAsync(partitionKeys);
                }
            });

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
            ResultSetFuture partitionKeyFuture = executeWithSession(schemaName, new SessionCallable<ResultSetFuture>() {
                @Override
                public ResultSetFuture executeWithSession(Session session)
                {
                    return session.executeAsync(partitionKeys);
                }
            });
            return partitionKeyFuture.getUninterruptibly();
        }
    }

    public <T> T executeWithSession(String schemaName, SessionCallable<T> sessionCallable)
    {
        NoHostAvailableException lastException = null;
        for (int i = 0; i <= noHostAvailableRetryCount; i++) {
            Session session = getSession(schemaName);
            try {
                return sessionCallable.executeWithSession(session);
            }
            catch (NoHostAvailableException e) {
                lastException = e;

                // Something happened with our client connection.  We need to
                // re-establish the connection using our contact points.
                sessionBySchema.asMap().remove(schemaName, session);
            }
        }
        throw lastException;
    }

    private interface SessionCallable<T>
    {
        T executeWithSession(Session session);
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
