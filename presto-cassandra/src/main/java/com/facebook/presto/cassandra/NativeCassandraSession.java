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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
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
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
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
import static java.util.Locale.ROOT;
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
                    return getKeyspaceByCaseSensitiveName0(key);
                }
            });

    private static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    private static final String SYSTEM = "system";
    private static final String SIZE_ESTIMATES = "size_estimates";
    private static final Version PARTITION_FETCH_WITH_IN_PREDICATE_VERSION = Version.parse("2.2");

    private final String connectorId;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final ReopeningSession reopeningSession;
    private final Duration noHostAvailableRetryTimeout;
    private static boolean caseSensitiveNameMatchingEnabled;

    public NativeCassandraSession(String connectorId, JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec, ReopeningSession reopeningSession, Duration noHostAvailableRetryTimeout, boolean caseSensitiveNameMatchingEnabled)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
        this.reopeningSession = requireNonNull(reopeningSession, "reopeningSession is null");
        this.noHostAvailableRetryTimeout = requireNonNull(noHostAvailableRetryTimeout, "noHostAvailableRetryTimeout is null");
        this.caseSensitiveNameMatchingEnabled = caseSensitiveNameMatchingEnabled;
    }

    private CqlSession getSession()
    {
        return reopeningSession.get();
    }

    @Override
    public Version getCassandraVersion()
    {
        ResultSet result = getSession().execute("select release_version from system.local");
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new PrestoException(CASSANDRA_VERSION_ERROR, "The cluster version is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }
        return Version.parse(versionRow.getString("release_version"));
    }

    @Override
    public String getPartitioner()
    {
        // Driver 4.x: Metadata is immutable, get fresh copy
        return getSession().getMetadata().getTokenMap()
                .map(tokenMap -> tokenMap.getPartitionerName())
                .orElse("Unknown");
    }

    @Override
    public Set<TokenRange> getTokenRanges()
    {
        // Driver 4.x: TokenMap is Optional
        return getSession().getMetadata().getTokenMap()
                .map(tokenMap -> tokenMap.getTokenRanges())
                .orElse(ImmutableSet.of());
    }

    @Override
    public Set<Node> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(tokenRange, "tokenRange is null");
        
        // Driver 4.x: Use TokenMap API
        return getSession().getMetadata().getTokenMap()
                .map(tokenMap -> tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), tokenRange))
                .orElse(ImmutableSet.of());
    }

    @Override
    public Set<Node> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey)
    {
        requireNonNull(caseSensitiveSchemaName, "keyspace is null");
        requireNonNull(partitionKey, "partitionKey is null");
        
        // Driver 4.x: Use TokenMap API
        return getSession().getMetadata().getTokenMap()
                .map(tokenMap -> tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), partitionKey))
                .orElse(ImmutableSet.of());
    }

    @Override
    public String getCaseSensitiveSchemaName(String caseSensitiveSchemaName)
    {
        return getKeyspaceByCaseSensitiveName(caseSensitiveSchemaName).getName().asInternal();
    }

    @Override
    public List<String> getCaseSensitiveSchemaNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        // Driver 4.x: Metadata.getKeyspaces() returns Map<CqlIdentifier, KeyspaceMetadata>
        for (KeyspaceMetadata meta : getSession().getMetadata().getKeyspaces().values()) {
            builder.add(meta.getName().asInternal());
        }
        return builder.build();
    }

    @Override
    public List<String> getCaseSensitiveTableNames(String caseSensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseSensitiveName(caseSensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        // Driver 4.x: getTables() and getViews() return Map<CqlIdentifier, TableMetadata/ViewMetadata>
        for (TableMetadata table : keyspace.getTables().values()) {
            builder.add(table.getName().asInternal());
        }
        for (ViewMetadata view : keyspace.getViews().values()) {
            builder.add(view.getName().asInternal());
        }
        return builder.build();
    }

    @Override
    public CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseSensitiveName(schemaTableName.getSchemaName());
        TableMetadata tableMeta = getTableMetadata(keyspace, schemaTableName.getTableName());

        List<String> columnNames = new ArrayList<>();
        // Driver 4.x: getColumns() returns Map<CqlIdentifier, ColumnMetadata>
        List<ColumnMetadata> columns = new ArrayList<>(tableMeta.getColumns().values());
        if (!caseSensitiveNameMatchingEnabled) {
            checkColumnNames(columns);
        }

        for (ColumnMetadata columnMetadata : columns) {
            columnNames.add(columnMetadata.getName().asInternal());
        }

        // check if there is a comment to establish column ordering
        // Driver 4.x: getOptions() returns TableOptions (not TableOptionsMetadata)
        Object commentObj = tableMeta.getOptions().get(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("comment"));
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
        // Driver 4.x: getPartitionKey() returns List<ColumnMetadata>
        for (ColumnMetadata columnMeta : tableMeta.getPartitionKey()) {
            String columnName = columnMeta.getName().asInternal();
            primaryKeySet.add(columnName);
            boolean hidden = hiddenColumns.contains(columnName);
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, true, false, columnNames.indexOf(columnName), hidden);
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        // Driver 4.x: getClusteringColumns() returns Map<ColumnMetadata, ClusteringOrder>
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns().keySet()) {
            String columnName = columnMeta.getName().asInternal();
            primaryKeySet.add(columnName);
            boolean hidden = hiddenColumns.contains(columnName);
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, true, columnNames.indexOf(columnName), hidden);
            columnHandles.add(columnHandle);
        }

        // add other columns
        for (ColumnMetadata columnMeta : columns) {
            String columnName = columnMeta.getName().asInternal();
            if (!primaryKeySet.contains(columnName)) {
                boolean hidden = hiddenColumns.contains(columnName);
                CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, false, false, columnNames.indexOf(columnName), hidden);
                columnHandles.add(columnHandle);
            }
        }

        List<CassandraColumnHandle> sortedColumnHandles = columnHandles.build().stream()
                .sorted(comparing(CassandraColumnHandle::getOrdinalPosition))
                .collect(toList());

        CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, tableMeta.getKeyspace().asInternal(), tableMeta.getName().asInternal());
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
        // Driver 4.x: getKeyspaces() returns Map<CqlIdentifier, KeyspaceMetadata>
        List<KeyspaceMetadata> keyspaces = new ArrayList<>(getSession().getMetadata().getKeyspaces().values());
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = Ordering.from(comparing((KeyspaceMetadata ks) -> ks.getName().asInternal())).immutableSortedCopy(keyspaces);
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            String keyspaceName = keyspace.getName().asInternal();
            if (namesMatch(keyspaceName, caseSensitiveSchemaName, caseSensitiveNameMatchingEnabled)) {
                if (caseSensitiveNameMatchingEnabled) {
                    result = keyspace;
                    break;
                }
                if (result != null) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("More than one keyspace has been found for the schema name: %s -> (%s, %s)",
                                    caseSensitiveSchemaName.toLowerCase(ROOT), result.getName().asInternal(), keyspaceName));
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

    private static TableMetadata getTableMetadata(KeyspaceMetadata keyspace, String caseSensitiveTableName)
    {
        // Driver 4.x: getTables() and getViews() return Map<CqlIdentifier, TableMetadata/ViewMetadata>
        // Combine tables and views for lookup
        List<TableMetadata> tables = Stream.concat(
                keyspace.getTables().values().stream(),
                keyspace.getViews().values().stream()
                        .map(view -> (TableMetadata) view))  // ViewMetadata extends TableMetadata in 4.x
                .filter(table -> namesMatch(table.getName().asInternal(), caseSensitiveTableName, caseSensitiveNameMatchingEnabled))
                .collect(toImmutableList());
        
        if (tables.size() == 0) {
            throw new TableNotFoundException(new SchemaTableName(keyspace.getName().asInternal(), caseSensitiveTableName));
        }
        else if (tables.size() == 1) {
            return tables.get(0);
        }
        String tableNames = tables.stream()
                .map(table -> table.getName().asInternal())
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
        // Driver 4.x: getView() returns Optional<ViewMetadata>
        return keyspace.getView(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(schemaTableName.getTableName())).isPresent();
    }

    private static void checkColumnNames(List<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> lowercaseNameToColumnMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            String columnName = column.getName().asInternal();
            String columnNameKey = columnName.toLowerCase(ROOT);
            if (lowercaseNameToColumnMap.containsKey(columnNameKey)) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("More than one column has been found for the case insensitive column name: %s -> (%s, %s)",
                                columnNameKey, lowercaseNameToColumnMap.get(columnNameKey).getName().asInternal(), columnName));
            }
            lowercaseNameToColumnMap.put(columnNameKey, column);
        }
    }

    private CassandraColumnHandle buildColumnHandle(TableMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
    {
        // Driver 4.x: Use DataType directly instead of DataType.Name
        CassandraType cassandraType = CassandraType.getCassandraType(columnMeta.getType());
        List<CassandraType> typeArguments = null;
        if (cassandraType.getTypeArgumentSize() > 0) {
            // Driver 4.x: Collection types need special handling
            DataType dataType = columnMeta.getType();
            if (dataType instanceof com.datastax.oss.driver.api.core.type.ListType) {
                com.datastax.oss.driver.api.core.type.ListType listType = (com.datastax.oss.driver.api.core.type.ListType) dataType;
                typeArguments = ImmutableList.of(CassandraType.getCassandraType(listType.getElementType()));
            }
            else if (dataType instanceof com.datastax.oss.driver.api.core.type.SetType) {
                com.datastax.oss.driver.api.core.type.SetType setType = (com.datastax.oss.driver.api.core.type.SetType) dataType;
                typeArguments = ImmutableList.of(CassandraType.getCassandraType(setType.getElementType()));
            }
            else if (dataType instanceof com.datastax.oss.driver.api.core.type.MapType) {
                com.datastax.oss.driver.api.core.type.MapType mapType = (com.datastax.oss.driver.api.core.type.MapType) dataType;
                typeArguments = ImmutableList.of(
                        CassandraType.getCassandraType(mapType.getKeyType()),
                        CassandraType.getCassandraType(mapType.getValueType()));
            }
        }
        boolean indexed = false;
        SchemaTableName schemaTableName = new SchemaTableName(tableMetadata.getKeyspace().asInternal(), tableMetadata.getName().asInternal());
        if (!isMaterializedView(schemaTableName)) {
            // Driver 4.x: getIndexes() returns Map<CqlIdentifier, IndexMetadata>
            for (IndexMetadata idx : tableMetadata.getIndexes().values()) {
                // Driver 4.x: IndexMetadata.getTarget() returns String
                if (idx.getTarget().equals(columnMeta.getName().asInternal())) {
                    indexed = true;
                    break;
                }
            }
        }
        return new CassandraColumnHandle(connectorId, columnMeta.getName().asInternal(), ordinalPosition, cassandraType, typeArguments, partitionKey, clusteringKey, indexed, hidden);
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
        // Driver 4.x: Use SimpleStatement with positional values
        SimpleStatement statement = SimpleStatement.newInstance(cql, values);
        return getSession().execute(statement);
    }

    @Override
    public PreparedStatement prepare(SimpleStatement statement)
    {
        // Driver 4.x: prepare() takes SimpleStatement or String
        return getSession().prepare(statement);
    }

    @Override
    public ResultSet execute(BoundStatement statement)
    {
        return getSession().execute(statement);
    }

    @Override
    public ResultSet execute(SimpleStatement statement)
    {
        return getSession().execute(statement);
    }

    private Iterable<Row> queryPartitionKeysWithInClauses(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
        partitionKeys = addWhereInClauses(partitionKeys, partitionKeyColumns, filterPrefixes);

        // Driver 4.x: Convert Select to SimpleStatement
        return execute(partitionKeys.build()).all();
    }

    private Iterable<Row> queryPartitionKeysLegacyWithMultipleQueries(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Set<List<Object>> filterCombinations = Sets.cartesianProduct(filterPrefixes);

        ImmutableList.Builder<Row> rowList = ImmutableList.builder();
        for (List<Object> combination : filterCombinations) {
            Select partitionKeys = CassandraCqlUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
            partitionKeys = addWhereClause(partitionKeys, partitionKeyColumns, combination);

            // Driver 4.x: Convert Select to SimpleStatement
            List<Row> resultRows = execute(partitionKeys.build()).all();
            if (resultRows != null && !resultRows.isEmpty()) {
                rowList.addAll(resultRows);
            }
        }

        return rowList.build();
    }

    private static Select addWhereInClauses(Select select, List<CassandraColumnHandle> partitionKeyColumns, List<Set<Object>> filterPrefixes)
    {
        // Driver 4.x: Query builder is immutable, build WHERE clauses step by step
        for (int i = 0; i < filterPrefixes.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            List<Object> values = filterPrefixes.get(i)
                    .stream()
                    .map(value -> column.getCassandraType().getJavaValue(value))
                    .collect(toList());
            // Driver 4.x: Use Relation.in() with Term objects
            List<Term> terms = values.stream()
                    .map(QueryBuilder::literal)
                    .collect(toList());
            select = select.whereColumn(CassandraCqlUtils.validColumnName(column.getName())).in(terms);
        }
        return select;
    }

    private static Select addWhereClause(Select select, List<CassandraColumnHandle> partitionKeyColumns, List<Object> filterPrefix)
    {
        // Driver 4.x: Query builder is immutable, build WHERE clauses step by step
        for (int i = 0; i < filterPrefix.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            Object value = column.getCassandraType().getJavaValue(filterPrefix.get(i));
            // Driver 4.x: Use whereColumn().isEqualTo() instead of QueryBuilder.eq()
            select = select.whereColumn(CassandraCqlUtils.validColumnName(column.getName())).isEqualTo(bindMarker());
        }
        return select;
    }

    @Override
    public List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName)
    {
        checkSizeEstimatesTableExist();
        
        // Driver 4.x: Use query builder to construct the statement
        SimpleStatement statement = selectFrom(SYSTEM, SIZE_ESTIMATES)
                .column("range_start")
                .column("range_end")
                .column("mean_partition_size")
                .column("partitions_count")
                .whereColumn("keyspace_name").isEqualTo(bindMarker())
                .whereColumn("table_name").isEqualTo(bindMarker())
                .build();

        ResultSet result = getSession().execute(statement.setPositionalValues(Arrays.asList(keyspaceName, tableName)));
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
        // Driver 4.x: getKeyspace() returns Optional<KeyspaceMetadata>
        KeyspaceMetadata keyspaceMetadata = getSession().getMetadata()
                .getKeyspace(SYSTEM)
                .orElseThrow(() -> new IllegalStateException("system keyspace metadata must not be null"));
        
        // Driver 4.x: getTable() returns Optional<TableMetadata>
        if (!keyspaceMetadata.getTable(SIZE_ESTIMATES).isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
        }
    }
}
