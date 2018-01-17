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

import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.CharMatcher;
import org.joda.time.chrono.ISOChronology;

import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.ReadMapping.longReadMapping;
import static com.facebook.presto.plugin.jdbc.ReadMapping.sliceReadMapping;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public final class StandardReadMappings
{
    private StandardReadMappings() {}

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    public static ReadMapping booleanReadMapping()
    {
        return ReadMapping.booleanReadMapping(BOOLEAN, ResultSet::getBoolean);
    }

    public static ReadMapping tinyintReadMapping()
    {
        return longReadMapping(TINYINT, ResultSet::getByte);
    }

    public static ReadMapping smallintReadMapping()
    {
        return longReadMapping(SMALLINT, ResultSet::getShort);
    }

    public static ReadMapping integerReadMapping()
    {
        return longReadMapping(INTEGER, ResultSet::getInt);
    }

    public static ReadMapping bigintReadMapping()
    {
        return longReadMapping(BIGINT, ResultSet::getLong);
    }

    public static ReadMapping realReadMapping()
    {
        return longReadMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)));
    }

    public static ReadMapping doubleReadMapping()
    {
        return ReadMapping.doubleReadMapping(DOUBLE, ResultSet::getDouble);
    }

    public static ReadMapping decimalReadMapping(DecimalType decimalType)
    {
        if (decimalType.isShort()) {
            return longReadMapping(decimalType, (resultSet, columnIndex) -> resultSet.getBigDecimal(columnIndex).unscaledValue().longValueExact());
        }
        return sliceReadMapping(decimalType, (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex)));
    }

    public static ReadMapping charReadMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return sliceReadMapping(charType, (resultSet, columnIndex) -> utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex))));
    }

    public static ReadMapping varcharReadMapping(VarcharType varcharType)
    {
        return sliceReadMapping(varcharType, (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)));
    }

    public static ReadMapping varbinaryReadMapping()
    {
        return sliceReadMapping(VARBINARY, (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)));
    }

    public static ReadMapping dateReadMapping()
    {
        return longReadMapping(DATE, (resultSet, columnIndex) -> {
            // JDBC returns a date using a timestamp at midnight in the JVM timezone
            long localMillis = resultSet.getDate(columnIndex).getTime();
            // Convert it to a midnight in UTC
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        });
    }

    public static ReadMapping timeReadMapping()
    {
        return longReadMapping(TIME, (resultSet, columnIndex) -> {
            Time time = resultSet.getTime(columnIndex);
            return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
        });
    }

    public static ReadMapping timestampReadMapping()
    {
        return longReadMapping(TIMESTAMP, (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return timestamp.getTime();
        });
    }

    public static Optional<ReadMapping> jdbcTypeToPrestoType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanReadMapping());

            case Types.TINYINT:
                return Optional.of(tinyintReadMapping());

            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());

            case Types.INTEGER:
                return Optional.of(integerReadMapping());

            case Types.BIGINT:
                return Optional.of(bigintReadMapping());

            case Types.REAL:
                return Optional.of(realReadMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleReadMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = type.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryReadMapping());

            case Types.DATE:
                return Optional.of(dateReadMapping());

            case Types.TIME:
                return Optional.of(timeReadMapping());

            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
        }
        return Optional.empty();
    }
}
