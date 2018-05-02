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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.joda.time.DateTimeZone;

import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.phoenix.PhoenixClient.ROWKEY;
import static com.facebook.presto.plugin.phoenix.PhoenixClient.getFullTableName;
import static com.facebook.presto.plugin.phoenix.PhoenixClient.toSqlType;
import static com.facebook.presto.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static com.facebook.presto.plugin.phoenix.PhoenixErrorCode.PHOENIX_NON_TRANSIENT_ERROR;
import static com.facebook.presto.plugin.phoenix.TypeUtils.isArrayType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.readBigDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public class PhoenixPageSink
        implements ConnectorPageSink
{
    private final PhoenixConnection connection;
    private final PreparedStatement statement;

    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<String> dupKeyColumns;
    private int batchSize;
    private boolean hasRowkey;

    public PhoenixPageSink(PhoenixOutputTableHandle handle, ConnectorSession session, PhoenixClient phoenixClient)
    {
        columnTypes = handle.getColumnTypes();
        columnNames = handle.getColumnNames();

        hasRowkey = ROWKEY.equalsIgnoreCase(columnNames.get(0));
        List<String> duplicateKeyUpdateColumns = PhoenixSessionProperties.getDuplicateKeyUpdateColumns(session);

        dupKeyColumns = columnNames.stream().filter(column -> duplicateKeyUpdateColumns.contains(column)).collect(Collectors.toList());

        try {
            connection = phoenixClient.getConnection();
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }

        try {
            statement = connection.prepareStatement(buildInsertSql(handle));
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public String buildInsertSql(PhoenixOutputTableHandle handle)
    {
        String columns = Joiner.on(',').join(handle.getColumnNames());
        String vars = Joiner.on(',').join(nCopies(handle.getColumnNames().size(), "?"));
        // ON DUPLICATE KEY UPDATE counter1 = counter1 + 1, counter2 = counter2 + 1;
        StringBuilder sql = new StringBuilder()
                .append("UPSERT INTO ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append("(").append(columns).append(")")
                .append(" VALUES (")
                .append(vars).append(")");

        if (!dupKeyColumns.isEmpty()) {
            sql.append(" ON DUPLICATE KEY UPDATE ");
            sql.append(dupKeyColumns.stream().map(column -> column + " = " + column + " + ?").collect(Collectors.joining(",")));
        }

        return sql.toString();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        int rowkeyParameter = 0;
        if (hasRowkey) {
            rowkeyParameter++;
        }
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (hasRowkey) {
                    statement.setString(1, UUID.randomUUID().toString());
                }
                Object[] dupKeyValues = new Object[dupKeyColumns.size()];
                int channel = 0;
                for (; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    int columnPos = channel + rowkeyParameter;
                    int parameter = columnPos + 1;
                    Type type = columnTypes.get(columnPos);

                    Object value = getObjectValue(type, block, position);

                    if (value instanceof Array) {
                        statement.setArray(parameter, (Array) value);
                    }
                    else {
                        statement.setObject(parameter, value);

                        int dupKeyPos = dupKeyColumns.indexOf(columnNames.get(columnPos));
                        if (dupKeyPos > -1) {
                            dupKeyValues[dupKeyPos] = value;
                        }
                    }
                }
                for (int i = 0; i < dupKeyValues.length; i++) {
                    int parameter = channel + i + rowkeyParameter + 1;
                    statement.setObject(parameter, dupKeyValues[i]);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position) throws SQLException
    {
        if (block.isNull(position)) {
            return null;
        }

        if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        else if (BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        else if (INTEGER.equals(type)) {
            return toIntExact(type.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        else if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        else if (REAL.equals(type)) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        else if (type instanceof DecimalType) {
            return readBigDecimal((DecimalType) type, block, position);
        }
        else if (isVarcharType(type) || isCharType(type)) {
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        else if (DATE.equals(type)) {
            // convert to midnight in default time zone
            long utcMillis = DAYS.toMillis(type.getLong(block, position));
            long localMillis = getInstanceUTC().getZone().getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
            return new Date(localMillis);
        }
        else if (TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Timestamp(millisUtc);
        }
        else if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getObject(position, Block.class);

            Object[] elements = new Object[arrayBlock.getPositionCount()];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getObjectValue(elementType, arrayBlock, i);
                elements[i] = element;
            }
            String sqlType = toSqlType(elementType);
            int boundedPos = sqlType.indexOf('(');
            if (boundedPos > -1) {
                sqlType = sqlType.substring(0, boundedPos).trim();
            }
            return connection.createArrayOf(sqlType, elements);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // commit and close
        try (PhoenixConnection connection = this.connection;
                PreparedStatement statement = this.statement) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
        catch (SQLNonTransientException e) {
            throw new PrestoException(PHOENIX_NON_TRANSIENT_ERROR, e);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        // rollback and close
        try (PhoenixConnection connection = this.connection;
                PreparedStatement statement = this.statement) {
            connection.rollback();
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }
}
