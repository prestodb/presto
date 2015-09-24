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
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.IDBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.maxColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.minColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
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

    public static final SchemaTableName SHARD_METADATA_TABLE_NAME = new SchemaTableName("system", "shards");
    public static final ConnectorTableMetadata SHARD_METADATA = new ConnectorTableMetadata(
            SHARD_METADATA_TABLE_NAME,
            ImmutableList.of(
                    new ColumnMetadata(SCHEMA_NAME, VARCHAR, false),
                    new ColumnMetadata(TABLE_NAME, VARCHAR, false),
                    new ColumnMetadata(SHARD_UUID, VARCHAR, false),
                    new ColumnMetadata("uncompressed_size", BIGINT, false),
                    new ColumnMetadata("compressed_size", BIGINT, false),
                    new ColumnMetadata("row_count", BIGINT, false),
                    new ColumnMetadata("min_timestamp", TIMESTAMP, false),
                    new ColumnMetadata("max_timestamp", TIMESTAMP, false)));

    private static final List<ColumnMetadata> COLUMNS = SHARD_METADATA.getColumns();
    private static final List<Type> TYPES = COLUMNS.stream().map(ColumnMetadata::getType).collect(toList());

    private final MetadataDao metadataDao;
    private final Iterator<Long> tableIds;
    private final IDBI dbi;
    private final PreparedStatementBuilder preparedStatementBuilder;
    private final ResultSetValues resultSetValues;

    private boolean closed;
    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private long completedBytes;

    public ShardMetadataRecordCursor(IDBI dbi, TupleDomain<Integer> tupleDomain)
    {
        requireNonNull(dbi, "dbi is null");
        requireNonNull(tupleDomain, "tupleDomain is null");

        this.dbi = dbi;
        this.metadataDao = dbi.onDemand(MetadataDao.class);

        this.tableIds = getTableIds(metadataDao, tupleDomain);

        List<String> columnNames = createQualifiedColumnName();
        this.preparedStatementBuilder = new PreparedStatementBuilder(
                constructSqlTemplate(columnNames),
                columnNames,
                TYPES,
                ImmutableList.of(getColumnIndex(SHARD_METADATA, SHARD_UUID)),
                tupleDomain);
        this.resultSetValues = new ResultSetValues(TYPES);

        this.resultSet = getNextResultSet();
    }

    private static String constructSqlTemplate(List<String> columnNames)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT\n");

        for (int i = 0; i < columnNames.size() - 2; i++) {
            sql.append(columnNames.get(i) + ",\n");
        }
        sql.append("%s AS min_timestamp,\n");
        sql.append("%s AS max_timestamp\n");

        sql.append("FROM %s x\n");
        sql.append("JOIN shards ON (x.shard_id = shards.shard_id)\n");
        sql.append("JOIN tables ON (shards.table_id = tables.table_id)\n");

        return sql.toString();
    }

    private static List<String> createQualifiedColumnName()
    {
        return ImmutableList.<String>builder()
                .add("tables.schema_name")
                .add("tables.table_name")
                .add("shards" + "." + COLUMNS.get(2).getName())
                .add("shards" + "." + COLUMNS.get(3).getName())
                .add("shards" + "." + COLUMNS.get(4).getName())
                .add("shards" + "." + COLUMNS.get(5).getName())
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
        catch (SQLException e) {
            throw Throwables.propagate(e);
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

        try {
            connection = dbi.open().getConnection();
            statement = preparedStatementBuilder.create(connection, minColumn, maxColumn, shardIndexTable(tableId));
            return statement.executeQuery();
        }
        catch (SQLException e) {
            close();
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    private static Iterator<Long> getTableIds(MetadataDao metadataDao, TupleDomain<Integer> tupleDomain)
    {
        List<Long> tableIds = metadataDao.listTableIds();
        if (tupleDomain.isNone()) {
            return tableIds.iterator();
        }

        Domain schemaNameDomain = tupleDomain.getDomains().get(getColumnIndex(SHARD_METADATA, SHARD_UUID));
        Domain tableNameDomain = tupleDomain.getDomains().get(getColumnIndex(SHARD_METADATA, TABLE_NAME));

        if (tableNameDomain != null && tableNameDomain.isSingleValue()) {
            String tableName = getStringValue(tableNameDomain.getSingleValue());

            if (schemaNameDomain == null) {
                List<Table> tables = metadataDao.getTableInformation(tableName);
                if (tables == null) {
                    return ImmutableList.<Long>of().iterator();
                }
                tableIds = tables.stream().map(Table::getTableId).collect(toList());
            }
            else if (schemaNameDomain.isSingleValue()) {
                String schemaName = getStringValue(schemaNameDomain.getSingleValue());
                Table table = metadataDao.getTableInformation(schemaName, tableName);
                if (table == null) {
                    return ImmutableList.<Long>of().iterator();
                }
                tableIds = ImmutableList.of(table.getTableId());
            }
        }
        return tableIds.iterator();
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

    private static String getStringValue(Comparable<?> value)
    {
        return checkType(value, Slice.class, "value").toStringUtf8();
    }
}
