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
package com.facebook.presto.raptor.systemtables;

import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
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

import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.maxColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.minColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ShardMetadataRecordCursor
        implements RecordCursor
{
    private static final String SHARD_UUID = "shard_uuid";
    private static final String SCHEMA_NAME = "table_schema";
    private static final String TABLE_NAME = "table_name";
    private static final String MIN_TIMESTAMP = "min_timestamp";
    private static final String MAX_TIMESTAMP = "max_timestamp";

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
                    new ColumnMetadata(MIN_TIMESTAMP, TIMESTAMP),
                    new ColumnMetadata(MAX_TIMESTAMP, TIMESTAMP)));

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
        requireNonNull(dbi, "dbi is null");
        this.dbi = dbi;
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.tableIds = getTableIds(dbi, tupleDomain);
        this.columnNames = createQualifiedColumnNames();
        this.resultSetValues = new ResultSetValues(TYPES);
        this.resultSet = getNextResultSet();
    }

    private static String constructSqlTemplate(List<String> columnNames, String indexTableName)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT\n");
        sql.append(Joiner.on(",\n").join(columnNames));
        sql.append("\nFROM ").append(indexTableName).append(" x\n");
        sql.append("JOIN shards ON (x.shard_id = shards.shard_id)\n");
        sql.append("JOIN tables ON (shards.table_id = tables.table_id)\n");

        return sql.toString();
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
                .add("min_timestamp")
                .add("max_timestamp")
                .build();
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
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
            completedBytes += resultSetValues.extractValues(resultSet, ImmutableSet.of(getColumnIndex(SHARD_METADATA, SHARD_UUID)));
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

        String minColumn = (columnId == null) ? "null" : minColumn(columnId);
        String maxColumn = (columnId == null) ? "null" : maxColumn(columnId);

        List<String> columnNames = getMappedColumnNames(minColumn, maxColumn);
        try {
            connection = dbi.open().getConnection();
            statement = PreparedStatementBuilder.create(
                    connection,
                    constructSqlTemplate(columnNames, shardIndexTable(tableId)),
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

    private List<String> getMappedColumnNames(String minColumn, String maxColumn)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String column : columnNames) {
            switch (column) {
                case MIN_TIMESTAMP:
                    builder.add(minColumn);
                    break;
                case MAX_TIMESTAMP:
                    builder.add(maxColumn);
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

        StringBuilder sql = new StringBuilder("SELECT table_id FROM tables ");
        if (schemaNameDomain != null || tableNameDomain != null) {
            sql.append("WHERE ");
            List<String> predicates = new ArrayList<>();
            if (tableNameDomain != null && tableNameDomain.isSingleValue()) {
                predicates.add(format("table_name = '%s'", getStringValue(tableNameDomain.getSingleValue())));
            }
            if (schemaNameDomain != null && schemaNameDomain.isSingleValue()) {
                predicates.add(format("schema_name = '%s'", getStringValue(schemaNameDomain.getSingleValue())));
            }
            sql.append(Joiner.on(" AND ").join(predicates));
        }

        ImmutableList.Builder<Long> tableIds = ImmutableList.builder();
        try (Connection connection = dbi.open().getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql.toString())) {
            while (resultSet.next()) {
                tableIds.add(resultSet.getLong("table_id"));
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
        return checkType(value, Slice.class, "value").toStringUtf8();
    }
}
