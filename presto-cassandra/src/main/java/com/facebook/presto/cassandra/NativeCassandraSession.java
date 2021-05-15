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

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.Select.Where;
import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_VERSION_ERROR;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class NativeCassandraSession
        implements CassandraSession
{
    private static final Logger log = Logger.get(NativeCassandraSession.class);
    private final LoadingCache<String, KeyspaceMetadata> keyspaceCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, MINUTES)
            .build(new CacheLoader<String, KeyspaceMetadata>()
            {
                @Override
                public KeyspaceMetadata load(String key)
                        throws Exception
                {
                    return getKeyspaceByCaseInsensitiveName0(key);
                }
            });

    private static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    private static final String SYSTEM = "system";
    private static final String SIZE_ESTIMATES = "size_estimates";
    private static final VersionNumber PARTITION_FETCH_WITH_IN_PREDICATE_VERSION = VersionNumber.parse("2.2");

    private final String connectorId;
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
    public VersionNumber getCassandraVersion()
    {
        ResultSet result = executeWithSession(session -> session.execute("select release_version from system.local"));
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new PrestoException(CASSANDRA_VERSION_ERROR, "The cluster version is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }
        return VersionNumber.parse(versionRow.getString("release_version"));
    }

    @Override
    public String getPartitioner()
    {
        return executeWithSession(session -> session.getCluster().getMetadata().getPartitioner());
    }

    @Override
    public Set<TokenRange> getTokenRanges()
    {
        return executeWithSession(session -> session.getCluster().getMetadata().getTokenRanges());
    }

    @Override
    public Set<Host> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(tokenRange, "tokenRange is null");
        return executeWithSession(session ->
                session.getCluster().getMetadata().getReplicas(validSchemaName(caseSensitiveSchemaName), tokenRange));
    }

    @Override
    public Set<Host> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(partitionKey, "partitionKey is null");
        return executeWithSession(session ->
                session.getCluster().getMetadata().getReplicas(validSchemaName(caseSensitiveSchemaName), partitionKey));
    }

    @Override
    public String getCaseSensitiveSchemaName(String caseInsensitiveSchemaName)
    {
        return getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName).getName();
    }

    @Override
    public List<String> getCaseSensitiveSchemaNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        for (KeyspaceMetadata meta : keyspaces) {
            builder.add(meta.getName());
        }
        return builder.build();
    }

    @Override
    public List<String> getCaseSensitiveTableNames(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata table : keyspace.getTables()) {
            builder.add(table.getName());
        }
        for (MaterializedViewMetadata materializedView : keyspace.getMaterializedViews()) {
            builder.add(materializedView.getName());
        }
        return builder.build();
    }

    @Override
    public CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(schemaTableName.getSchemaName());
        AbstractTableMetadata tableMeta = getTableMetadata(keyspace, schemaTableName.getTableName());

        List<String> columnNames = new ArrayList<>();
        List<ColumnMetadata> columns = tableMeta.getColumns();
        checkColumnNames(columns);
        for (ColumnMetadata columnMetadata : columns) {
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
        for (ColumnMetadata columnMeta : columns) {
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

    private KeyspaceMetadata getKeyspaceByCaseInsensitiveName(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        try {
            return keyspaceCache.get(caseInsensitiveSchemaName);
        }
        catch (UncheckedExecutionException | ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SchemaNotFoundException) {
                throw (SchemaNotFoundException) cause;
            }

            if (cause instanceof PrestoException) {
                throw (PrestoException) cause;
            }

            throw new RuntimeException(cause);
        }
    }

    private KeyspaceMetadata getKeyspaceByCaseInsensitiveName0(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = Ordering.from(comparing(KeyspaceMetadata::getName)).immutableSortedCopy(keyspaces);
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            if (keyspace.getName().equalsIgnoreCase(caseInsensitiveSchemaName)) {
                if (result != null) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("More than one keyspace has been found for the case insensitive schema name: %s -> (%s, %s)",
                                    caseInsensitiveSchemaName, result.getName(), keyspace.getName()));
                }
                result = keyspace;
            }
        }
        if (result == null) {
            throw new SchemaNotFoundException(caseInsensitiveSchemaName);
        }
        return result;
    }

    private static AbstractTableMetadata getTableMetadata(KeyspaceMetadata keyspace, String caseInsensitiveTableName)
    {
        List<AbstractTableMetadata> tables = Stream.concat(
                keyspace.getTables().stream(),
                keyspace.getMaterializedViews().stream())
                .filter(table -> table.getName().equalsIgnoreCase(caseInsensitiveTableName))
                .collect(toImmutableList());
        if (tables.size() == 0) {
            throw new TableNotFoundException(new SchemaTableName(keyspace.getName(), caseInsensitiveTableName));
        }
        else if (tables.size() == 1) {
            return tables.get(0);
        }
        String tableNames = tables.stream()
                .map(AbstractTableMetadata::getName)
                .sorted()
                .collect(joining(", "));
        throw new PrestoException(
                NOT_SUPPORTED,
                format("More than one table has been found for the case insensitive table name: %s -> (%s)",
                        caseInsensitiveTableName, tableNames));
    }

    public boolean isMaterializedView(SchemaTableName schemaTableName)
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(schemaTableName.getSchemaName());
        return keyspace.getMaterializedView(schemaTableName.getTableName()) != null;
    }

    private static void checkColumnNames(List<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> lowercaseNameToColumnMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            String lowercaseName = column.getName().toLowerCase(ENGLISH);
            if (lowercaseNameToColumnMap.containsKey(lowercaseName)) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("More than one column has been found for the case insensitive column name: %s -> (%s, %s)",
                                lowercaseName, lowercaseNameToColumnMap.get(lowercaseName).getName(), column.getName()));
            }
            lowercaseNameToColumnMap.put(lowercaseName, column);
        }
    }

    private CassandraColumnHandle buildColumnHandle(AbstractTableMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
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
        SchemaTableName schemaTableName = new SchemaTableName(tableMetadata.getKeyspace().getName(), tableMetadata.getName());
        if (!isMaterializedView(schemaTableName)) {
            TableMetadata table = (TableMetadata) tableMetadata;
            for (IndexMetadata idx : table.getIndexes()) {
                if (idx.getTarget().equals(columnMeta.getName())) {
                    indexed = true;
                    break;
                }
            }
        }
        return new CassandraColumnHandle(connectorId, columnMeta.getName(), ordinalPosition, cassandraType, typeArguments, partitionKey, clusteringKey, indexed, hidden);
    }

    @Override
    public List<CassandraPartition> getPartitions(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        if (filterPrefixes.size() != partitionKeyColumns.size()) {
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
        }

        Iterable<Row> rows;
        if (getCassandraVersion().compareTo(PARTITION_FETCH_WITH_IN_PREDICATE_VERSION) > 0) {
            log.debug("Using IN predicate to fetch partitions.");
            rows = queryPartitionKeysWithInClauses(table, filterPrefixes);
        }
        else {
            log.debug("Using combination of partition values to fetch partitions.");
            rows = queryPartitionKeysLegacyWithMultipleQueries(table, filterPrefixes);
        }

        if (rows == null) {
            // just split the whole partition range
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
        }

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

    @Override
    public ResultSet execute(String cql, Object... values)
    {
        return executeWithSession(session -> session.execute(cql, values));
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement)
    {
        return executeWithSession(session -> session.prepare(statement));
    }

    @Override
    public ResultSet execute(Statement statement)
    {
        return executeWithSession(session -> session.execute(statement));
    }

    private Iterable<Row> queryPartitionKeysWithInClauses(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        addWhereInClauses(partitionKeys.where(), partitionKeyColumns, filterPrefixes);

        return execute(partitionKeys).all();
    }

    private Iterable<Row> queryPartitionKeysLegacyWithMultipleQueries(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Set<List<Object>> filterCombinations = Sets.cartesianProduct(filterPrefixes);

        ImmutableList.Builder<Row> rowList = ImmutableList.builder();
        for (List<Object> combination : filterCombinations) {
            Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
            addWhereClause(partitionKeys.where(), partitionKeyColumns, combination);

            List<Row> resultRows = execute(partitionKeys).all();
            if (resultRows != null && !resultRows.isEmpty()) {
                rowList.addAll(resultRows);
            }
        }

        return rowList.build();
    }

    private static void addWhereInClauses(Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Set<Object>> filterPrefixes)
    {
        for (int i = 0; i < filterPrefixes.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            List<Object> values = filterPrefixes.get(i)
                    .stream()
                    .map(value -> column.getCassandraType().getJavaValue(value))
                    .collect(toList());
            Clause clause = QueryBuilder.in(CassandraCqlUtils.validColumnName(column.getName()), values);
            where.and(clause);
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

    @Override
    public List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName)
    {
        checkSizeEstimatesTableExist();
        Statement statement = select("range_start", "range_end", "mean_partition_size", "partitions_count")
                .from(SYSTEM, SIZE_ESTIMATES)
                .where(eq("keyspace_name", keyspaceName))
                .and(eq("table_name", tableName));

        ResultSet result = executeWithSession(session -> session.execute(statement));
        ImmutableList.Builder<SizeEstimate> estimates = ImmutableList.builder();
        for (Row row : result.all()) {
            SizeEstimate estimate = new SizeEstimate(
                    row.getString("range_start"),
                    row.getString("range_end"),
                    row.getLong("mean_partition_size"),
                    row.getLong("partitions_count"));
            estimates.add(estimate);
        }

        return estimates.build();
    }

    private void checkSizeEstimatesTableExist()
    {
        KeyspaceMetadata keyspaceMetadata = executeWithSession(session -> session.getCluster().getMetadata().getKeyspace(SYSTEM));
        checkState(keyspaceMetadata != null, "system keyspace metadata must not be null");
        TableMetadata table = keyspaceMetadata.getTable(SIZE_ESTIMATES);
        if (table == null) {
            throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
        }
    }

    private <T> T executeWithSession(SessionCallable<T> sessionCallable)
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

    private interface SessionCallable<T>
    {
        T executeWithSession(Session session);
    }
}
