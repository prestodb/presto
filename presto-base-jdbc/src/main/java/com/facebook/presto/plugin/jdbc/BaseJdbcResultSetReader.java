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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static org.joda.time.DateTimeZone.UTC;

public class BaseJdbcResultSetReader
        implements JdbcResultSetReader
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);

    @Override
    public boolean getBoolean(ResultSet resultSet, int column, Type type) throws Exception
    {
        return resultSet.getBoolean(column);
    }

    @Override
    public long getLong(ResultSet resultSet, int column, Type type) throws Exception
    {
        if (type.equals(TinyintType.TINYINT)) {
            return (long) resultSet.getByte(column);
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return (long) resultSet.getShort(column);
        }
        if (type.equals(IntegerType.INTEGER)) {
            return (long) resultSet.getInt(column);
        }
        if (type.equals(RealType.REAL)) {
            return (long) floatToRawIntBits(resultSet.getFloat(column));
        }
        if (type.equals(BigintType.BIGINT)) {
            return resultSet.getLong(column);
        }
        if (type.equals(DateType.DATE)) {
            // JDBC returns a date using a timestamp at midnight in the JVM timezone
            long localMillis = resultSet.getDate(column).getTime();
            // Convert it to a midnight in UTC
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
            // convert to days
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        }
        if (type.equals(TimeType.TIME)) {
            Time time = resultSet.getTime(column);
            return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            Timestamp timestamp = resultSet.getTimestamp(column);
            return timestamp.getTime();
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
    }

    @Override
    public double getDouble(ResultSet resultSet, int column, Type type) throws Exception
    {
        return resultSet.getDouble(column);
    }

    @Override
    public Slice getSlice(ResultSet resultSet, int column, Type type) throws Exception
    {
        if (type instanceof VarcharType) {
            return utf8Slice(resultSet.getString(column));
        }
        if (type instanceof CharType) {
            return utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(column)));
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return wrappedBuffer(resultSet.getBytes(column));
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for slice: " + type.getTypeSignature());
    }

    private static final int VALUE_COLUMN = 2;
    private static final int DEFAULT_EXPECTED_ENTRIES = 4;

    @Override
    public Object getObject(ResultSet resultSet, int column, Type type) throws Exception
    {
        if (!(type instanceof ArrayType)) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for object: " + type.getTypeSignature());
        }

        Type elementType = ((ArrayType) type).getElementType();
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), DEFAULT_EXPECTED_ENTRIES);
        ResultSet arrayResultSet = resultSet.getArray(column).getResultSet();

        if (elementType.getJavaType() == boolean.class) {
            while (arrayResultSet.next()) {
                if (this.isNull(arrayResultSet, VALUE_COLUMN, elementType)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeBoolean(
                            blockBuilder,
                            this.getBoolean(arrayResultSet, VALUE_COLUMN, elementType));
                }
            }
        }
        else if (elementType.getJavaType() == long.class) {
            while (arrayResultSet.next()) {
                if (this.isNull(arrayResultSet, VALUE_COLUMN, elementType)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeLong(
                            blockBuilder,
                            this.getLong(arrayResultSet, VALUE_COLUMN, elementType));
                }
            }
        }
        else if (elementType.getJavaType() == double.class) {
            while (arrayResultSet.next()) {
                if (this.isNull(arrayResultSet, VALUE_COLUMN, elementType)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeDouble(
                            blockBuilder,
                            this.getDouble(arrayResultSet, VALUE_COLUMN, elementType));
                }
            }
        }
        else if (elementType.getJavaType() == Slice.class) {
            while (arrayResultSet.next()) {
                if (this.isNull(arrayResultSet, VALUE_COLUMN, elementType)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeSlice(
                            blockBuilder,
                            this.getSlice(arrayResultSet, VALUE_COLUMN, elementType));
                }
            }
        }
        else if (elementType.getJavaType() == Block.class) {
            while (arrayResultSet.next()) {
                if (this.isNull(arrayResultSet, VALUE_COLUMN, elementType)) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeObject(
                            blockBuilder,
                            this.getObject(arrayResultSet, VALUE_COLUMN, elementType));
                }
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    "Unknown java class " + elementType.getJavaType() +
                    " from type: " + type.getTypeSignature());
        }
        return blockBuilder.build();
    }

    @Override
    public boolean isNull(ResultSet resultSet, int column, Type type) throws Exception
    {
        // JDBC is kind of dumb: we need to read the field and then ask
        // if it was null, which means we are wasting effort here.
        // We could save the result of the field access if it matters.
        resultSet.getObject(column);

        return resultSet.wasNull();
    }
}
