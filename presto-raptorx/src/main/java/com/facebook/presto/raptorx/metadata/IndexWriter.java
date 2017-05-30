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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.storage.ColumnStats;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Handle;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.StringJoiner;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.storage.ColumnStatsBuilder.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptorx.storage.ColumnStatsBuilder.truncateIndexValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class IndexWriter
        implements AutoCloseable
{
    private static final String INDEX_TABLE_PREFIX = "x_chunks_t";

    private final long commitId;
    private final List<ColumnInfo> columns;
    private final Map<Long, Integer> indexes;
    private final Map<Long, JDBCType> types;
    private final PreparedStatement statement;

    public IndexWriter(Connection connection, long commitId, long tableId, List<ColumnInfo> columns)
            throws SQLException
    {
        this.commitId = commitId;

        ImmutableList.Builder<ColumnInfo> columnBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Long, Integer> indexBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Long, JDBCType> typeBuilder = ImmutableMap.builder();
        StringJoiner nameJoiner = new StringJoiner(", ");
        StringJoiner valueJoiner = new StringJoiner(", ");
        int index = 1;

        for (String column : asList("start_commit_id", "chunk_id", "bucket_number")) {
            nameJoiner.add(column);
            valueJoiner.add("?");
            index++;
        }

        for (ColumnInfo column : columns) {
            JDBCType jdbcType = jdbcType(column.getType());
            if (jdbcType == null) {
                continue;
            }

            long columnId = column.getColumnId();
            columnBuilder.add(column);

            nameJoiner.add(minColumn(columnId));
            nameJoiner.add(maxColumn(columnId));
            valueJoiner.add("?").add("?");

            indexBuilder.put(columnId, index);
            index += 2;

            typeBuilder.put(columnId, jdbcType);
        }

        this.columns = columnBuilder.build();
        this.indexes = indexBuilder.build();
        this.types = typeBuilder.build();

        String sql = "" +
                "INSERT INTO " + chunkIndexTable(tableId) + "\n" +
                "(" + nameJoiner + ")\n" +
                "VALUES (" + valueJoiner + ")";

        this.statement = connection.prepareStatement(sql);
    }

    @Override
    public void close()
            throws SQLException
    {
        statement.close();
    }

    public void add(long chunkId, int bucketNumber, List<ColumnStats> stats)
            throws SQLException
    {
        statement.setLong(1, commitId);
        statement.setLong(2, chunkId);
        statement.setInt(3, bucketNumber);

        for (ColumnInfo column : columns) {
            int index = indexes.get(column.getColumnId());
            int type = types.get(column.getColumnId()).getVendorTypeNumber();
            statement.setNull(index, type);
            statement.setNull(index + 1, type);
        }

        for (ColumnStats column : stats) {
            int index = indexes.get(column.getColumnId());
            JDBCType type = types.get(column.getColumnId());
            bindValue(statement, type, column.getMin(), index);
            bindValue(statement, type, column.getMax(), index + 1);
        }

        statement.addBatch();
    }

    public void execute()
            throws SQLException
    {
        statement.executeBatch();
    }

    public static String chunkIndexTable(long tableId)
    {
        return INDEX_TABLE_PREFIX + tableId;
    }

    public static String minColumn(long columnId)
    {
        checkArgument(columnId >= 0, "invalid columnId %s", columnId);
        return format("c%s_min", columnId);
    }

    public static String maxColumn(long columnId)
    {
        checkArgument(columnId >= 0, "invalid columnId %s", columnId);
        return format("c%s_max", columnId);
    }

    public static void createIndexTable(Database.Type databaseType, Handle handle, long tableId, List<ColumnInfo> columns, OptionalLong temporalColumnId)
    {
        StringJoiner tableColumns = new StringJoiner(",\n  ", "  ", ",\n").setEmptyValue("");

        for (ColumnInfo column : columns) {
            String columnType = sqlColumnType(databaseType, column.getType());
            if (columnType != null) {
                tableColumns.add(format("%s %s", minColumn(column.getColumnId()), columnType));
                tableColumns.add(format("%s %s", maxColumn(column.getColumnId()), columnType));
            }
        }

        String coveringIndex = "";
        if (temporalColumnId.isPresent()) {
            // add the max temporal column first to accelerate queries that usually scan recent data
            coveringIndex = format(
                    "  UNIQUE (%s, %s, bucket_number, chunk_id, start_commit_id, end_commit_id),\n",
                    minColumn(temporalColumnId.getAsLong()),
                    maxColumn(temporalColumnId.getAsLong()));
        }

        String createTable = "" +
                "CREATE TABLE %s (\n" +
                "  start_commit_id BIGINT NOT NULL,\n" +
                "  end_commit_id BIGINT,\n" +
                "  chunk_id BIGINT NOT NULL,\n" +
                "  bucket_number INT NOT NULL,\n" +
                "%s" +
                "  PRIMARY KEY (bucket_number, chunk_id),\n" +
                "%s" +
                "  UNIQUE (chunk_id),\n" +
                "  UNIQUE (start_commit_id, chunk_id),\n" +
                "  UNIQUE (end_commit_id, chunk_id)\n" +
                ")";
        String sql = format(createTable, chunkIndexTable(tableId), tableColumns, coveringIndex);

        handle.execute(sql);
    }

    public static void addIndexTableColumn(Database.Type databaseType, Handle handle, long tableId, ColumnInfo column)
    {
        String columnType = sqlColumnType(databaseType, column.getType());
        if (columnType == null) {
            return;
        }

        String tableName = chunkIndexTable(tableId);
        String minColumn = format("%s %s", minColumn(column.getColumnId()), columnType);
        String maxColumn = format("%s %s", maxColumn(column.getColumnId()), columnType);

        String sql;
        if (databaseType == Database.Type.H2) {
            sql = format("ALTER TABLE %s ADD COLUMN (%s, %s)", tableName, minColumn, maxColumn);
        }
        else {
            sql = format("ALTER TABLE %s ADD COLUMN %s, ADD COLUMN %s", tableName, minColumn, maxColumn);
        }

        handle.execute(sql);
    }

    public static void dropIndexTable(Handle handle, long tableId)
    {
        handle.execute("DROP TABLE IF EXISTS " + chunkIndexTable(tableId));
    }

    public static void dropIndexTableColumn(Database.Type databaseType, Handle handle, long tableId, long columnId)
    {
        String tableName = chunkIndexTable(tableId);
        String minColumn = minColumn(columnId);
        String maxColumn = maxColumn(columnId);

        if (databaseType == Database.Type.H2) {
            handle.execute(format("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s", tableName, minColumn));
            handle.execute(format("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s", tableName, maxColumn));
        }
        else if (databaseType == Database.Type.MYSQL) {
            String selectColumns = "" +
                    "SELECT column_name\n" +
                    "FROM information_schema.columns\n" +
                    "WHERE table_schema = database()\n" +
                    "  AND table_name = ?";
            String dropColumns = handle.select(selectColumns, tableName).mapTo(String.class).stream()
                    .map(column -> column.toLowerCase(ENGLISH))
                    .filter(column -> column.equals(minColumn) || column.equals(maxColumn))
                    .map(column -> "DROP COLUMN " + column)
                    .collect(joining(", "));
            if (!dropColumns.isEmpty()) {
                handle.execute(format("ALTER TABLE %s %s", tableName, dropColumns));
            }
        }
        else if (databaseType == Database.Type.POSTGRESQL) {
            handle.execute(format("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s, DROP COLUMN IF EXISTS %s",
                    tableName, minColumn, maxColumn));
        }
        else {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + databaseType);
        }
    }

    public static JDBCType jdbcType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return JDBCType.BOOLEAN;
        }
        if (type.equals(BIGINT) || type.equals(DOUBLE) || type.equals(TIMESTAMP)) {
            return JDBCType.BIGINT;
        }
        if (type.equals(INTEGER) || type.equals(REAL) || type.equals(DATE) || type.equals(TIME)) {
            return JDBCType.INTEGER;
        }
        if (type.equals(SMALLINT)) {
            return JDBCType.SMALLINT;
        }
        if (type.equals(TINYINT)) {
            return JDBCType.TINYINT;
        }
        if (isVarcharType(type) || type.equals(VARBINARY)) {
            return JDBCType.VARBINARY;
        }
        return null;
    }

    public static void bindValue(PreparedStatement statement, JDBCType type, Object value, int index)
            throws SQLException
    {
        if (value == null) {
            statement.setNull(index, type.getVendorTypeNumber());
            return;
        }

        switch (type) {
            case BOOLEAN:
                statement.setBoolean(index, (boolean) value);
                return;
            case BIGINT:
                statement.setLong(index, ((Number) value).longValue());
                return;
            case INTEGER:
                statement.setInt(index, ((Number) value).intValue());
                return;
            case SMALLINT:
                statement.setShort(index, ((Number) value).shortValue());
                return;
            case TINYINT:
                statement.setByte(index, ((Number) value).byteValue());
                return;
            case VARBINARY:
                statement.setBytes(index, truncateIndexValue((byte[]) value));
                return;
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled type: " + type);
    }

    private static String sqlColumnType(Database.Type databaseType, Type columnType)
    {
        JDBCType jdbcType = jdbcType(columnType);
        if (jdbcType == null) {
            return null;
        }

        switch (jdbcType) {
            case BOOLEAN:
                return "boolean";
            case BIGINT:
                return "bigint";
            case INTEGER:
                return "integer";
            case SMALLINT:
                return "smallint";
            case TINYINT:
                if (databaseType == Database.Type.POSTGRESQL) {
                    return "smallint";
                }
                return "tinyint";
            case VARBINARY:
                if (databaseType == Database.Type.POSTGRESQL) {
                    return "bytea";
                }
                return format("varbinary(%s)", MAX_BINARY_INDEX_SIZE);
        }
        return null;
    }
}
