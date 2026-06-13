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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.annotations.VisibleForTesting;
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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_VERSION_ERROR;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class NativeCassandraSession
        implements CassandraSession
{
    private static final Logger log = Logger.get(NativeCassandraSession.class);
    // Use expireAfterWrite (not expireAfterAccess) so the cached entry is reloaded periodically
    // even under continuous access. In driver 4.x, Session.getMetadata() returns an immutable
    // snapshot of the schema (unlike driver 3.x, where the cached KeyspaceMetadata was a live view
    // that auto-updated). Caching that snapshot with expireAfterAccess would pin a stale view for
    // as long as it is polled (e.g. SHOW TABLES in a tight loop), so schema objects created
    // out-of-band (such as a materialized view created by another client) would never appear.
    // The driver refreshes its own metadata within ~1s of a schema-change event, so a short
    // write-based TTL keeps the connector's view fresh while still avoiding redundant lookups.
    private final LoadingCache<String, KeyspaceMetadata> keyspaceCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, SECONDS)
            .build(new CacheLoader<String, KeyspaceMetadata>()
            {
                @Override
                public KeyspaceMetadata load(String key)
                        throws Exception
                {
                    return getKeyspaceByCaseSensitiveName0(key);
                }
            });

    private static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    private static final String SYSTEM = "system";
    private static final String SIZE_ESTIMATES = "size_estimates";
    private static final String PARTITION_FETCH_WITH_IN_PREDICATE_VERSION = "2.2";

    private final String connectorId;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final ReopeningSession reopeningSession;
    private final Supplier<CqlSession> session;
    private final Duration noHostAvailableRetryTimeout;
    private static boolean caseSensitiveNameMatchingEnabled;
    private final int vectorMaxDimensions;

    // The Cassandra release version is immutable for the lifetime of a session, so resolve it once and
    // cache it. Previously getCassandraVersion() ran a `SELECT release_version FROM system.local` query
    // on every call, including the hot path in getPartitions(), adding a network round-trip per query.
    private final Supplier<String> cassandraVersion = memoize(this::resolveCassandraVersion);

    public NativeCassandraSession(
            String connectorId,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            ReopeningSession reopeningSession,
            Duration noHostAvailableRetryTimeout,
            boolean caseSensitiveNameMatchingEnabled,
            int vectorMaxDimensions)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
        this.reopeningSession = requireNonNull(reopeningSession, "reopeningSession is null");
        this.noHostAvailableRetryTimeout = requireNonNull(noHostAvailableRetryTimeout, "noHostAvailableRetryTimeout is null");
        // Do NOT memoize: ReopeningSession.getSession() is the component that detects a closed delegate
        // and transparently rebuilds a fresh CqlSession. Memoizing would pin the first session forever
        // and silently defeat that reopening behavior. getSession() is synchronized and cheap, so it is
        // safe to resolve the live session on every call.
        this.session = reopeningSession::getSession;
        this.caseSensitiveNameMatchingEnabled = caseSensitiveNameMatchingEnabled;
        this.vectorMaxDimensions = vectorMaxDimensions;
    }

    @Override
    public String getCassandraVersion()
    {
        return cassandraVersion.get();
    }

    private String resolveCassandraVersion()
    {
        // Prefer the version already present in the driver's cached node metadata to avoid a network
        // round-trip. In a mixed-version cluster take the lowest version so feature gating stays safe.
        Optional<Version> nodeVersion = executeWithSession(session -> session.getMetadata().getNodes().values().stream()
                .map(Node::getCassandraVersion)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder()));
        if (nodeVersion.isPresent()) {
            return nodeVersion.get().toString();
        }

        // Fall back to querying system.local if node metadata does not expose a version.
        ResultSet result = executeWithSession(session -> session.execute("select release_version from system.local"));
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new PrestoException(CASSANDRA_VERSION_ERROR, "The cluster version is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }
        return versionRow.getString("release_version");
    }

    @Override
    public String getPartitioner()
    {
        return executeWithSession(session -> session.getMetadata().getTokenMap()
                .orElseThrow(() -> new IllegalStateException("Token map is not available"))
                .getPartitionerName());
    }

    @Override
    public Set<TokenRange> getTokenRanges()
    {
        return executeWithSession(session -> {
            TokenMap tokenMap = session.getMetadata().getTokenMap()
                    .orElseThrow(() -> new IllegalStateException("Token map is not available"));
            return tokenMap.getTokenRanges();
        });
    }

    @Override
    public Set<Node> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(tokenRange, "tokenRange is null");
        return executeWithSession(session -> {
            TokenMap tokenMap = session.getMetadata().getTokenMap()
                    .orElseThrow(() -> new IllegalStateException("Token map is not available"));
            return tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), tokenRange);
        });
    }

    @Override
    public Set<Node> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(partitionKey, "partitionKey is null");
        return executeWithSession(session -> {
            TokenMap tokenMap = session.getMetadata().getTokenMap()
                    .orElseThrow(() -> new IllegalStateException("Token map is not available"));
            return tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), partitionKey);
        });
    }

    @Override
    public String getCaseSensitiveSchemaName(String caseSensitiveSchemaName)
    {
        return getKeyspaceByCaseSensitiveName(caseSensitiveSchemaName).getName().toString();
    }

    @Override
    public List<String> getCaseSensitiveSchemaNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
                executeWithSession(session -> session.getMetadata().getKeyspaces());
        for (KeyspaceMetadata meta : keyspaces.values()) {
            builder.add(meta.getName().toString());
        }
        return builder.build();
    }

    @Override
    public List<String> getCaseSensitiveTableNames(String caseSensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseSensitiveName(caseSensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata table : keyspace.getTables().values()) {
            builder.add(table.getName().toString());
        }
        for (ViewMetadata materializedView : keyspace.getViews().values()) {
            builder.add(materializedView.getName().toString());
        }
        return builder.build();
    }

    @Override
    public CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseSensitiveName(schemaTableName.getSchemaName());
        RelationMetadata tableMeta = getTableMetadata(keyspace, schemaTableName.getTableName());

        List<String> columnNames = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>(tableMeta.getColumns().values());
        if (!caseSensitiveNameMatchingEnabled) {
            checkColumnNames(columns);
        }

        for (ColumnMetadata columnMetadata : columns) {
            columnNames.add(columnMetadata.getName().toString());
        }

        // check if there is a comment to establish column ordering
        // In driver 4.x, getOptions() returns Map<CqlIdentifier, Object>
        // We need to get the comment and convert it to String
        Object commentObj = tableMeta.getOptions().get(CqlIdentifier.fromCql("comment"));
        Optional<String> comment = commentObj != null ? Optional.of(commentObj.toString()) : Optional.empty();
        Set<String> hiddenColumns = ImmutableSet.of();
        if (comment.isPresent() && comment.get().startsWith(PRESTO_COMMENT_METADATA)) {
            String columnOrderingString = comment.get().substring(PRESTO_COMMENT_METADATA.length());

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
            String colName = columnMeta.getName().toString();
            primaryKeySet.add(colName);
            boolean hidden = hiddenColumns.contains(colName);
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, true, false, columnNames.indexOf(colName), hidden);
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns().keySet()) {
            String colName = columnMeta.getName().toString();
            primaryKeySet.add(colName);
            boolean hidden = hiddenColumns.contains(colName);
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, true, columnNames.indexOf(colName), hidden);
            columnHandles.add(columnHandle);
        }

        // add other columns
        for (ColumnMetadata columnMeta : columns) {
            String colName = columnMeta.getName().toString();
            if (!primaryKeySet.contains(colName)) {
                boolean hidden = hiddenColumns.contains(colName);
                CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, false, columnNames.indexOf(colName), hidden);
                columnHandles.add(columnHandle);
            }
        }

        List<CassandraColumnHandle> sortedColumnHandles = columnHandles.build().stream()
                .sorted(comparing(CassandraColumnHandle::getOrdinalPosition))
                .collect(toList());

        CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableMeta.getKeyspace().toString(), tableMeta.getName().toString());
        return new CassandraTable(tableHandle, sortedColumnHandles);
    }

    private KeyspaceMetadata getKeyspaceByCaseSensitiveName(String caseSensitiveSchemaName)
            throws SchemaNotFoundException
    {
        try {
            return keyspaceCache.get(caseSensitiveSchemaName);
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

    private KeyspaceMetadata getKeyspaceByCaseSensitiveName0(String caseSensitiveSchemaName)
            throws SchemaNotFoundException
    {
        Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
                executeWithSession(session -> session.getMetadata().getKeyspaces());
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = Ordering.from(comparing((KeyspaceMetadata ks) -> ks.getName().asInternal()))
                .immutableSortedCopy(keyspaces.values());
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            if (namesMatch(keyspace.getName().asInternal(), caseSensitiveSchemaName, caseSensitiveNameMatchingEnabled)) {
                if (caseSensitiveNameMatchingEnabled) {
                    result = keyspace;
                    break;
                }
                if (result != null) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("More than one keyspace has been found for the schema name: %s -> (%s, %s)",
                                    caseSensitiveSchemaName.toLowerCase(ROOT), result.getName().asInternal(), keyspace.getName().asInternal()));
                }
                result = keyspace;
            }
        }

        if (result == null) {
            throw new SchemaNotFoundException(caseSensitiveSchemaName);
        }
        return result;
    }

    private static boolean namesMatch(String actualName, String expectedName, boolean caseSensitive)
    {
        return caseSensitive
                ? actualName.equals(expectedName)
                : actualName.equalsIgnoreCase(expectedName);
    }

    private static RelationMetadata getTableMetadata(KeyspaceMetadata keyspace, String caseSensitiveTableName)
    {
        // Both TableMetadata and ViewMetadata extend RelationMetadata in driver 4.x (ViewMetadata does
        // NOT extend TableMetadata), so we resolve against the common supertype. A materialized view's
        // name is registered under getViews(), not getTables(); looking it up via getTable() would
        // silently miss it and surface as "table does not exist" when the view is queried.
        List<RelationMetadata> tables = Stream.<RelationMetadata>concat(
                keyspace.getTables().values().stream(),
                keyspace.getViews().values().stream())
                .filter(table -> namesMatch(table.getName().toString(), caseSensitiveTableName, caseSensitiveNameMatchingEnabled))
                .collect(toImmutableList());
        if (tables.size() == 0) {
            throw new TableNotFoundException(new SchemaTableName(keyspace.getName().toString(), caseSensitiveTableName));
        }
        else if (tables.size() == 1) {
            return tables.get(0);
        }
        String tableNames = tables.stream()
                .map(table -> table.getName().toString())
                .sorted()
                .collect(joining(", "));
        throw new PrestoException(
                NOT_SUPPORTED,
                format("More than one table has been found for the case insensitive table name: %s -> (%s)",
                        caseSensitiveTableName.toLowerCase(ROOT), tableNames));
    }

    public boolean isMaterializedView(SchemaTableName schemaTableName)
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseSensitiveName(schemaTableName.getSchemaName());
        CqlIdentifier tableName = CqlIdentifier.fromCql(schemaTableName.getTableName());
        return keyspace.getView(tableName).isPresent();
    }

    private static void checkColumnNames(List<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> lowercaseNameToColumnMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            String columnNameKey = column.getName().toString().toLowerCase(ROOT);
            if (lowercaseNameToColumnMap.containsKey(columnNameKey)) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("More than one column has been found for the case insensitive column name: %s -> (%s, %s)",
                                columnNameKey, lowercaseNameToColumnMap.get(columnNameKey).getName().toString(), column.getName().toString()));
            }
            lowercaseNameToColumnMap.put(columnNameKey, column);
        }
    }

    private CassandraColumnHandle buildColumnHandle(RelationMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
    {
        DataType dataType = columnMeta.getType();
        CassandraType cassandraType = CassandraType.getCassandraType(dataType);
        List<CassandraType> typeArguments = null;
        if (cassandraType.getTypeArgumentSize() > 0) {
            // In driver 4.x, parameterized types need to be cast to access type arguments
            if (dataType instanceof ListType) {
                ListType listType = (ListType) dataType;
                typeArguments = ImmutableList.of(CassandraType.getCassandraType(listType.getElementType()));
            }
            else if (dataType instanceof SetType) {
                SetType setType = (SetType) dataType;
                typeArguments = ImmutableList.of(CassandraType.getCassandraType(setType.getElementType()));
            }
            else if (dataType instanceof MapType) {
                MapType mapType = (MapType) dataType;
                typeArguments = ImmutableList.of(
                        CassandraType.getCassandraType(mapType.getKeyType()),
                        CassandraType.getCassandraType(mapType.getValueType()));
            }
            else if (dataType instanceof VectorType) {
                VectorType vectorType = (VectorType) dataType;
                int dimensions = vectorType.getDimensions();
                if (dimensions > vectorMaxDimensions) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("Cassandra vector column '%s' has %s dimensions, which exceeds the configured maximum of %s (cassandra.vector.max-dimensions)",
                                    columnMeta.getName(), dimensions, vectorMaxDimensions));
                }
                typeArguments = ImmutableList.of(CassandraType.getCassandraType(vectorType.getElementType()));
            }
        }
        boolean indexed = false;
        SchemaTableName schemaTableName = new SchemaTableName(tableMetadata.getKeyspace().toString(), tableMetadata.getName().toString());
        // Indexes only exist on base tables (getIndexes() is defined on TableMetadata, not on the
        // RelationMetadata supertype shared with ViewMetadata), so the cast is safe under this guard.
        if (!isMaterializedView(schemaTableName) && tableMetadata instanceof TableMetadata) {
            for (IndexMetadata idx : ((TableMetadata) tableMetadata).getIndexes().values()) {
                if (idx.getTarget().equals(columnMeta.getName().asCql(true))) {
                    indexed = true;
                    break;
                }
            }
        }
        return new CassandraColumnHandle(connectorId, columnMeta.getName().toString(), ordinalPosition, cassandraType, typeArguments, partitionKey, clusteringKey, indexed, hidden);
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
            ((Buffer) buffer).clear();
            map.clear();
            stringBuilder.setLength(0);
            for (int i = 0; i < partitionKeyColumns.size(); i++) {
                // Driver 4.x getBytesUnsafe() returns the row's internal ByteBuffer (driver 3.x returned a
                // duplicate). Consuming it below via buffer.put(component) would advance its position to the
                // limit, causing later getString()/value reads of the same column to return empty. Duplicate
                // it to keep the original buffer's position intact for the subsequent value extractions.
                ByteBuffer component = row.getBytesUnsafe(i).duplicate();
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
            ((Buffer) buffer).flip();
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
        return executeWithSession(session -> session.execute(SimpleStatement.newInstance(cql, values)));
    }

    @Override
    public PreparedStatement prepare(String statement)
    {
        return executeWithSession(session -> session.prepare(statement));
    }

    @Override
    public ResultSet execute(Statement<?> statement)
    {
        return executeWithSession(session -> session.execute(statement));
    }

    private Iterable<Row> queryPartitionKeysWithInClauses(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        SimpleStatement partitionKeys = buildPartitionKeyInQuery(
                table.getTableHandle(), table.getPartitionKeyColumns(), filterPrefixes);
        return execute(partitionKeys).all();
    }

    private Iterable<Row> queryPartitionKeysLegacyWithMultipleQueries(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Set<List<Object>> filterCombinations = Sets.cartesianProduct(filterPrefixes);

        ImmutableList.Builder<Row> rowList = ImmutableList.builder();
        for (List<Object> combination : filterCombinations) {
            SimpleStatement partitionKeys = buildPartitionKeyEqualityQuery(tableHandle, partitionKeyColumns, combination);

            List<Row> resultRows = execute(partitionKeys).all();
            if (resultRows != null && !resultRows.isEmpty()) {
                rowList.addAll(resultRows);
            }
        }

        return rowList.build();
    }

    /**
     * Builds {@code SELECT DISTINCT <pk> FROM <table> WHERE pk0 IN ? AND pk1 IN ? ...} using the
     * driver's query builder. Each marker is bound to the per-column list of values, converted from
     * Presto native values via {@link CassandraType#getJavaValue}.
     */
    @VisibleForTesting
    static SimpleStatement buildPartitionKeyInQuery(CassandraTableHandle tableHandle, List<CassandraColumnHandle> partitionKeyColumns, List<Set<Object>> filterPrefixes)
    {
        Select select = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < partitionKeyColumns.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            select = select.whereColumn(CqlIdentifier.fromInternal(column.getName())).in(bindMarker());
            List<Object> columnValues = filterPrefixes.get(i)
                    .stream()
                    .map(value -> column.getCassandraType().getJavaValue(value))
                    .collect(toList());
            values.add(columnValues);
        }
        return select.build(values.toArray());
    }

    /**
     * Builds {@code SELECT DISTINCT <pk> FROM <table> WHERE pk0 = ? AND pk1 = ? ...} using the driver's
     * query builder, binding each marker to a single value converted via {@link CassandraType#getJavaValue}.
     */
    @VisibleForTesting
    static SimpleStatement buildPartitionKeyEqualityQuery(CassandraTableHandle tableHandle, List<CassandraColumnHandle> partitionKeyColumns, List<Object> filterPrefix)
    {
        Select select = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < filterPrefix.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            select = select.whereColumn(CqlIdentifier.fromInternal(column.getName())).isEqualTo(bindMarker());
            values.add(column.getCassandraType().getJavaValue(filterPrefix.get(i)));
        }
        return select.build(values.toArray());
    }

    @Override
    public List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName)
    {
        checkSizeEstimatesTableExist();
        SimpleStatement statement = SimpleStatement.newInstance(
                "SELECT range_start, range_end, mean_partition_size, partitions_count " +
                "FROM system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
                keyspaceName, tableName);

        ResultSet result = executeWithSession(session -> session.execute(statement));
        ImmutableList.Builder<SizeEstimate> estimates = ImmutableList.builder();
        for (Row row : result) {
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
        // Try to get system keyspace metadata
        Optional<KeyspaceMetadata> keyspaceMetadata = executeWithSession(session ->
                session.getMetadata().getKeyspace(CqlIdentifier.fromCql(SYSTEM)));

        // If metadata is available, check for the table
        if (keyspaceMetadata.isPresent()) {
            Optional<TableMetadata> table = keyspaceMetadata.get().getTable(CqlIdentifier.fromCql(SIZE_ESTIMATES));
            if (!table.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
            }
            return;
        }

        // If metadata is not available (filtered out), query the system table directly to check existence
        // This is a fallback for when schema metadata filtering excludes system keyspaces
        // Cassandra 2.1.x uses system.schema_columnfamilies, 3.0+ uses system_schema.tables
        try {
            // Try Cassandra 3.0+ format first (system_schema.tables)
            SimpleStatement statement = SimpleStatement.newInstance(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                    SYSTEM, SIZE_ESTIMATES);
            ResultSet result = executeWithSession(session -> session.execute(statement));
            if (!result.iterator().hasNext()) {
                throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
            }
        }
        catch (Exception e) {
            // If the 3.0+ query fails, try Cassandra 2.1.x format (schema_columnfamilies)
            try {
                SimpleStatement statement = SimpleStatement.newInstance(
                        "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ? AND columnfamily_name = ?",
                        SYSTEM, SIZE_ESTIMATES);
                ResultSet result = executeWithSession(session -> session.execute(statement));
                if (!result.iterator().hasNext()) {
                    throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
                }
            }
            catch (Exception e2) {
                // Both queries failed, throw error
                throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported", e2);
            }
        }
    }

    @Override
    public void refreshSchema()
    {
        executeWithSession(session -> {
            // Driver 4.x: Force metadata refresh by accessing keyspaces
            // This triggers a schema refresh in the driver
            session.getMetadata().getKeyspaces();
            // Clear our local cache to force reload on next access
            keyspaceCache.invalidateAll();
            log.info("Schema metadata cache refreshed");
            return null;
        });
    }

    private <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        long deadline = System.currentTimeMillis() + noHostAvailableRetryTimeout.toMillis();
        long delay = 1000; // Start with 1 second delay
        while (true) {
            try {
                return sessionCallable.executeWithSession(session.get());
            }
            // In driver 4.x, NoNodeAvailableException extends AllNodesFailedException
            // So we only need to catch AllNodesFailedException
            catch (AllNodesFailedException e) {
                long timeLeft = deadline - System.currentTimeMillis();
                if (timeLeft <= 0) {
                    throw e;
                }
                else {
                    long actualDelay = Math.min(delay, timeLeft);
                    log.warn("No hosts available, retrying in %dms", actualDelay);
                    try {
                        Thread.sleep(actualDelay);
                        delay = Math.min(delay * 2, 60000); // Exponential backoff, max 60 seconds
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
        T executeWithSession(CqlSession session);
    }
}
