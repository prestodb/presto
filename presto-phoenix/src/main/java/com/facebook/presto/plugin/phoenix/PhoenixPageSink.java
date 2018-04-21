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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
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
import java.util.concurrent.CompletableFuture;

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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public class PhoenixPageSink
        implements ConnectorPageSink
{
    private final PhoenixConnection connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private int batchSize;

    public PhoenixPageSink(PhoenixOutputTableHandle handle, PhoenixClient phoenixClient)
    {
        try {
            connection = phoenixClient.getConnection();
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }

        try {
            statement = connection.prepareStatement(phoenixClient.buildInsertSql(handle));
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }

        columnTypes = handle.getColumnTypes();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    int parameter = channel + 1;
                    Type type = columnTypes.get(channel);
                    Object value = getObjectValue(type, block, position);
                    if (value instanceof Array) {
                        statement.setArray(parameter, (Array) value);
                    }
                    else {
                        statement.setObject(parameter, value);
                    }
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

            return connection.createArrayOf(toSqlType(elementType, true), elements);
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
