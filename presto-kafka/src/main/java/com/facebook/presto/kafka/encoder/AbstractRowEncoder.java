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
package com.facebook.presto.kafka.encoder;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlTime;
import com.facebook.presto.common.type.SqlTimeWithTimeZone;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;

import java.nio.ByteBuffer;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.isVarbinaryType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRowEncoder
        implements RowEncoder
{
    protected final ConnectorSession session;
    protected final List<EncoderColumnHandle> columnHandles;

    /**
     * The current column index for appending values to the row encoder.
     * Gets incremented by appendColumnValue and set back to zero when the encoder is reset.
     */
    protected int currentColumnIndex;

    protected AbstractRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(columnHandles, "columnHandles is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.currentColumnIndex = 0;
    }

    @Override
    public void appendColumnValue(Block block, int position)
    {
        checkArgument(currentColumnIndex < columnHandles.size(), format("currentColumnIndex '%d' is greater than number of columns '%d'", currentColumnIndex, columnHandles.size()));
        Type type = columnHandles.get(currentColumnIndex).getType();
        if (block.isNull(position)) {
            appendNullValue();
        }
        else if (type == BOOLEAN) {
            appendBoolean(type.getBoolean(block, position));
        }
        else if (type == BIGINT) {
            appendLong(type.getLong(block, position));
        }
        else if (type == INTEGER) {
            appendInt(toIntExact(type.getLong(block, position)));
        }
        else if (type == SMALLINT) {
            appendShort(Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (type == TINYINT) {
            appendByte(SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (type == DOUBLE) {
            appendDouble(type.getDouble(block, position));
        }
        else if (type == REAL) {
            appendFloat(intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (isVarcharType(type)) {
            appendString(type.getSlice(block, position).toStringUtf8());
        }
        else if (isVarbinaryType(type)) {
            appendByteBuffer(type.getSlice(block, position).toByteBuffer());
        }
        else if (type == DATE) {
            appendSqlDate((SqlDate) type.getObjectValue(session.getSqlFunctionProperties(), block, position));
        }
        else if (type == TIME) {
            appendSqlTime((SqlTime) type.getObjectValue(session.getSqlFunctionProperties(), block, position));
        }
        else if (type == TIME_WITH_TIME_ZONE) {
            appendSqlTimeWithTimeZone((SqlTimeWithTimeZone) type.getObjectValue(session.getSqlFunctionProperties(), block, position));
        }
        else if (type instanceof TimestampType) {
            appendSqlTimestamp((SqlTimestamp) type.getObjectValue(session.getSqlFunctionProperties(), block, position));
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            appendSqlTimestampWithTimeZone((SqlTimestampWithTimeZone) type.getObjectValue(session.getSqlFunctionProperties(), block, position));
        }
        else {
            throw new UnsupportedOperationException(format("Column '%s' does not support 'null' value", columnHandles.get(currentColumnIndex).getName()));
        }
        currentColumnIndex++;
    }

    // these append value methods should be overridden for each row encoder
    // only the methods with types supported by the data format should be overridden
    protected void appendNullValue()
    {
        throw new UnsupportedOperationException(format("Column '%s' does not support 'null' value", columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendLong(long value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", long.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendInt(int value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", int.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendShort(short value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", short.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendByte(byte value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", byte.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendDouble(double value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", double.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendFloat(float value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", float.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendBoolean(boolean value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", boolean.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendString(String value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendByteBuffer(ByteBuffer value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlDate(SqlDate value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTime(SqlTime value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimeWithTimeZone(SqlTimeWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimestamp(SqlTimestamp value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimestampWithTimeZone(SqlTimestampWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void resetColumnIndex()
    {
        currentColumnIndex = 0;
    }

    @Override
    public void close()
    {
    }
}
