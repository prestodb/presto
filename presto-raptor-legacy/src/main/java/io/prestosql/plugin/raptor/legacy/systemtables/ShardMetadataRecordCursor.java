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
package io.prestosql.plugin.raptor.legacy.systemtables;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.plugin.raptor.legacy.metadata.MetadataDao;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.raptor.legacy.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_CORRUPT_METADATA;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.maxColumn;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.minColumn;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.shardIndexTable;
import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.metadataError;
import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.onDemandDao;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ShardMetadataRecordCursor
        implements RecordCursor
{
    private static final String SHARD_UUID = "shard_uuid";
    private static final String XXHASH64 = "xxhash64";
    private static final String SCHEMA_NAME = "table_schema";
    private static final String TABLE_NAME = "table_name";
    private static final String MIN_TIMESTAMP = "min_timestamp";
    private static final String MAX_TIMESTAMP = "max_timestamp";
    private static final String MIN_DATE = "min_date";
    private static final String MAX_DATE = "max_date";

    public static final SchemaTableName SHARD_METADATA_TABLE_NAME = new SchemaTableName("system", "shards");
    public static final ConnectorTableMetadata SHARD_METADATA = new ConnectorTableMetadata(
            SHARD_METADATA_TABLE_NAME,
            ImmutableList.of(
                    new ColumnMetadata(SCHEMA_NAME, createUnboundedVarcharType()),
                    new ColumnMetadata(TABLE_NAME, createUnboundedVarcharType()),
                    new ColumnMetadata(SHARD_UUID, SHARD_UUID_COLUMN_TYPE),
                    new ColumnMetadata("bucket_number", BIGINT),
                    new ColumnMetadata("uncompressed_size", BIGINT),
                    new ColumnMetadata("compressed_size", BIGINT),
                    new ColumnMetadata("row_count", BIGINT),
                    new ColumnMetadata(XXHASH64, createVarcharType(16)),
                    new ColumnMetadata(MIN_TIMESTAMP, TIMESTAMP),
                    new ColumnMetadata(MAX_TIMESTAMP, TIMESTAMP),
                    new ColumnMetadata(MIN_DATE, DATE),
                    new ColumnMetadata(MAX_DATE, DATE)));

    private static final List<ColumnMetadata> COLUMNS = SHARD_METADATA.getColumns();
    private static final List<Type> TYPES = COLUMNS.stream().map(ColumnMetadata::getType).collect(toList());

    private final IDBI dbi;
    private final MetadataDao metadataDao;

    private final Iterator<Long> tableIds;
    private final List<String> columnNames;
    private final TupleDomain<Integer> tupleDomain;

    private ResultSet resultSet;
    private Connection connection;
    private PreparedStatement statement;
    private final ResultSetValues resultSetValues;

    private boolean closed;
    private long completedBytes;

    public ShardMetadataRecordCursor(IDBI dbi, TupleDomain<Integer> tupleDomain)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.tableIds = getTableIds(dbi, tupleDomain);
        this.columnNames = createQualifiedColumnNames();
        this.resultSetValues = new ResultSetValues(TYPES);
        this.resultSet = getNextResultSet();
    }

    private static String constructSqlTemplate(List<String> columnNames, long tableId)
    {
        return format("SELECT %s\nFROM %s x\n" +
                        "JOIN shards ON (x.shard_id = shards.shard_id AND shards.table_id = %s)\n" +
                        "JOIN tables ON (tables.table_id = %s)\n",
                Joiner.on(", ").join(columnNames),
                shardIndexTable(tableId),
                tableId,
                tableId);
    }

    private static List<String> createQualifiedColumnNames()
    {
        return ImmutableList.<String>builder()
                .add("tables.schema_name")
                .add("tables.table_name")
                .add("shards" + "." + COLUMNS.get(2).getName())
                .add("shards" + "." + COLUMNS.get(3).getName())
                .add("shards" + "." + COLUMNS.get(4).getName())
                .add("shards" + "." + COLUMNS.get(5).getName())
                .add("shards" + "." + COLUMNS.get(6).getName())
                .add("shards" + "." + COLUMNS.get(7).getName())
                .add(MIN_TIMESTAMP)
                .add(MAX_TIMESTAMP)
                .add(MIN_DATE)
                .add(MAX_DATE)
                .build();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkPositionIndex(field, TYPES.size());
        return TYPES.get(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (resultSet == null) {
            close();
        }

        if (closed) {
            return false;
        }

        try {
            while (!resultSet.next()) {
                resultSet = getNextResultSet();
                if (resultSet == null) {
                    close();
                    return false;
                }
            }
            completedBytes += resultSetValues.extractValues(
                    resultSet,
                    ImmutableSet.of(getColumnIndex(SHARD_METADATA, SHARD_UUID)),
                    ImmutableSet.of(getColumnIndex(SHARD_METADATA, XXHASH64)));
            return true;
        }
        catch (SQLException | DBIException e) {
            throw metadataError(e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, boolean.class);
        return resultSetValues.getBoolean(field);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, long.class);
        return resultSetValues.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, double.class);
        return resultSetValues.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, Slice.class);
        return resultSetValues.getSlice(field);
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkPositionIndex(field, TYPES.size());
        return resultSetValues.isNull(field);
    }

    @Override
    public void close()
    {
        closed = true;
        closeCurrentResultSet();
    }

    @SuppressWarnings("unused")
    private void closeCurrentResultSet()
    {
        // use try-with-resources to close everything properly
        //noinspection EmptyTryBlock
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException ignored) {
        }
    }

    private ResultSet getNextResultSet()
    {
        closeCurrentResultSet();

        if (!tableIds.hasNext()) {
            return null;
        }

        Long tableId = tableIds.next();
        Long columnId = metadataDao.getTemporalColumnId(tableId);
        List<String> columnNames;

        if (columnId == null) {
            columnNames = getMappedColumnNames("null", "null", "null", "null");
        }
        else {
            Type temporalType = metadataDao.getTableColumn(tableId, columnId).getDataType();
            if (temporalType.equals(DATE)) {
                columnNames = getMappedColumnNames("null", "null", minColumn(columnId), maxColumn(columnId));
            }
            else if (temporalType.equals(TIMESTAMP)) {
                columnNames = getMappedColumnNames(minColumn(columnId), maxColumn(columnId), "null", "null");
            }
            else {
                throw new PrestoException(RAPTOR_CORRUPT_METADATA, "Temporal column should be of type date or timestamp, not " + temporalType.getDisplayName());
            }
        }

        try {
            connection = dbi.open().getConnection();
            statement = PreparedStatementBuilder.create(
                    connection,
                    constructSqlTemplate(columnNames, tableId),
                    columnNames,
                    TYPES,
                    ImmutableSet.of(getColumnIndex(SHARD_METADATA, SHARD_UUID)),
                    tupleDomain);
            return statement.executeQuery();
        }
        catch (SQLException | DBIException e) {
            close();
            throw metadataError(e);
        }
    }

    private List<String> getMappedColumnNames(String minTimestampColumn, String maxTimestampColumn, String minDateColumn, String maxDateColumn)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String column : columnNames) {
            switch (column) {
                case MIN_TIMESTAMP:
                    builder.add(minTimestampColumn);
                    break;
                case MAX_TIMESTAMP:
                    builder.add(maxTimestampColumn);
                    break;
                case MIN_DATE:
                    builder.add(minDateColumn);
                    break;
                case MAX_DATE:
                    builder.add(maxDateColumn);
                    break;
                default:
                    builder.add(column);
                    break;
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static Iterator<Long> getTableIds(IDBI dbi, TupleDomain<Integer> tupleDomain)
    {
        Map<Integer, Domain> domains = tupleDomain.getDomains().get();
        Domain schemaNameDomain = domains.get(getColumnIndex(SHARD_METADATA, SCHEMA_NAME));
        Domain tableNameDomain = domains.get(getColumnIndex(SHARD_METADATA, TABLE_NAME));

        List<String> values = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT table_id FROM tables ");
        if (schemaNameDomain != null || tableNameDomain != null) {
            sql.append("WHERE ");
            List<String> predicates = new ArrayList<>();
            if (tableNameDomain != null && tableNameDomain.isSingleValue()) {
                predicates.add("table_name = ?");
                values.add(getStringValue(tableNameDomain.getSingleValue()));
            }
            if (schemaNameDomain != null && schemaNameDomain.isSingleValue()) {
                predicates.add("schema_name = ?");
                values.add(getStringValue(schemaNameDomain.getSingleValue()));
            }
            sql.append(Joiner.on(" AND ").join(predicates));
        }

        ImmutableList.Builder<Long> tableIds = ImmutableList.builder();
        try (Connection connection = dbi.open().getConnection();
                PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.size(); i++) {
                statement.setString(i + 1, values.get(i));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    tableIds.add(resultSet.getLong("table_id"));
                }
            }
        }
        catch (SQLException | DBIException e) {
            throw metadataError(e);
        }
        return tableIds.build().iterator();
    }

    private static int getColumnIndex(ConnectorTableMetadata tableMetadata, String columnName)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(format("Column %s not found", columnName));
    }

    private void checkFieldType(int field, Class<?> clazz)
    {
        checkState(!closed, "cursor is closed");
        Type type = getType(field);
        checkArgument(type.getJavaType() == clazz, "Type %s cannot be read as %s", type, clazz.getSimpleName());
    }

    private static String getStringValue(Object value)
    {
        return ((Slice) value).toStringUtf8();
    }
}
