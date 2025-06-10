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
package com.facebook.presto.plugin.jdbc.mapping;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.google.common.base.CharMatcher;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.Decimals.encodeShortScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.UuidType.javaUuidToPrestoUuid;
import static com.facebook.presto.common.type.UuidType.prestoUuidToJavaUuid;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.mapping.ColumnMapping.booleanMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ColumnMapping.doubleMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ColumnMapping.longMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ColumnMapping.sliceMapping;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public final class StandardColumnMappings
{
    private StandardColumnMappings() {}

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    public static ColumnMapping booleanColumnMapping()
    {
        return booleanMapping(BOOLEAN, ResultSet::getBoolean, PreparedStatement::setBoolean);
    }

    public static ColumnMapping tinyintColumnMapping()
    {
        return longMapping(TINYINT, ResultSet::getByte,
                ((statement, index, value) -> statement.setByte(index, SignedBytes.checkedCast(value))));
    }

    public static ColumnMapping smallintColumnMapping()
    {
        return longMapping(SMALLINT, ResultSet::getShort,
                ((statement, index, value) -> statement.setShort(index, Shorts.checkedCast(value))));
    }

    public static ColumnMapping integerColumnMapping()
    {
        return longMapping(INTEGER, ResultSet::getInt,
                (((statement, index, value) -> statement.setInt(index, toIntExact(value)))));
    }

    public static ColumnMapping bigintColumnMapping()
    {
        return longMapping(BIGINT, ResultSet::getLong, PreparedStatement::setLong);
    }

    public static ColumnMapping realColumnMapping()
    {
        return longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                ((statement, index, value) -> statement.setFloat(index, intBitsToFloat(toIntExact(value)))));
    }

    public static ColumnMapping doubleColumnMapping()
    {
        return doubleMapping(DOUBLE, ResultSet::getDouble, PreparedStatement::setDouble);
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return longMapping(decimalType, (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), scale),
                    ((statement, index, value) -> {
                        BigInteger unscaledValue = BigInteger.valueOf(value);
                        BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                        statement.setBigDecimal(index, bigDecimal);
                    }));
        }
        return sliceMapping(decimalType, (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex), scale),
                ((statement, index, value) -> {
                    BigInteger unscaledValue = decodeUnscaledValue(value);
                    BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                    statement.setBigDecimal(index, bigDecimal);
                }));
    }

    public static ColumnMapping charColumnMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return sliceMapping(charType, (resultSet, columnIndex) -> utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex))),
                ((statement, index, value) -> statement.setString(index, value.toStringUtf8())));
    }

    public static ColumnMapping varcharColumnMapping(VarcharType varcharType)
    {
        return sliceMapping(varcharType, (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                ((statement, index, value) -> statement.setString(index, value.toStringUtf8())));
    }

    public static ColumnMapping varbinaryColumnMapping()
    {
        return sliceMapping(VARBINARY, (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                ((statement, index, value) -> statement.setBytes(index, value.getBytes())));
    }

    public static ColumnMapping dateColumnMapping()
    {
        return longMapping(DATE, (resultSet, columnIndex) -> {
            /*
             * JDBC returns a date using a timestamp at midnight in the JVM timezone, or earliest time after that if there was no midnight.
             * This works correctly for all dates and zones except when the missing local times 'gap' is 24h. I.e. this fails when JVM time
             * zone is Pacific/Apia and date to be returned is 2011-12-30.
             *
             * `return resultSet.getObject(columnIndex, LocalDate.class).toEpochDay()` avoids these problems but
             * is currently known not to work with Redshift (old Postgres connector) and SQL Server.
             */
            long localMillis = resultSet.getDate(columnIndex).getTime();
            // Convert it to a ~midnight in UTC.
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        },
                ((statement, index, value) -> statement.setDate(index, new Date(UTC.getMillisKeepLocal
                        (DateTimeZone.getDefault(), DAYS.toMillis(value))))));
    }

    public static ColumnMapping timeColumnMapping()
    {
        return longMapping(TIME, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTime(columnIndex)` returns wrong value if JVM's zone had forward offset change during 1970-01-01
             * and the time value being retrieved was not present in local time (a 'gap'), e.g. time retrieved is 00:10:00 and JVM zone is America/Hermosillo
             * The problem can be averted by using `resultSet.getObject(columnIndex, LocalTime.class)` -- but this is not universally supported by JDBC drivers.
             */
            Time time = resultSet.getTime(columnIndex);
            return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
        },
                ((statement, index, value) -> statement.setTime(index, new Time(value))));
    }

    public static ColumnMapping timestampColumnMapping(TimestampType type)
    {
        return longMapping(TIMESTAMP, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTimestamp(columnIndex)` returns wrong value if JVM's zone had forward offset change and the local time
             * corresponding to timestamp value being retrieved was not present (a 'gap'), this includes regular DST changes (e.g. Europe/Warsaw)
             * and one-time policy changes (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00).
             * The problem can be averted by using `resultSet.getObject(columnIndex, LocalDateTime.class)` -- but this is not universally supported by JDBC drivers.
             */
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return timestamp.getTime();
        }, (statement, index, value) -> statement.setTimestamp(index, Timestamp.from(Instant.ofEpochSecond(
                type.getEpochSecond(value),
                type.getNanos(value)))));
    }

    public static ColumnMapping uuidColumnMapping()
    {
        return sliceMapping(
                UUID,
                (resultSet, columnIndex) -> javaUuidToPrestoUuid((java.util.UUID) resultSet.getObject(columnIndex)),
                ((statement, index, value) -> statement.setObject(index, prestoUuidToJavaUuid(value))));
    }

    public static ColumnMapping timeWithTimeZoneColumnMapping()
    {
        return longMapping(
          TIME_WITH_TIME_ZONE,
                ((resultSet, columnIndex) -> resultSet.getTime(columnIndex).getTime()),
                (((statement, index, value) -> statement.setTime(index, new Time(unpackMillisUtc(value))))));
    }

    public static ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        return longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                ((resultSet, columnIndex) -> resultSet.getTimestamp(columnIndex).getTime()),
                ((statement, index, value) -> statement.setTimestamp(index, new Timestamp(unpackMillisUtc(value)))));
    }

    public static Optional<ColumnMapping> jdbcTypeToPrestoType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = type.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charColumnMapping(createCharType(charLength)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMapping());
            case Types.TIME_WITH_TIMEZONE:
                return Optional.of(timeWithTimeZoneColumnMapping());
            case Types.TIME:
                return Optional.of(timeColumnMapping());
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampWithTimeZoneColumnMapping());
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(TIMESTAMP));
        }
        return Optional.empty();
    }

    public static Optional<ColumnMapping> getColumnMappingFromPrestoType(Type type)
    {
        if (type instanceof BooleanType) {
            return Optional.of(booleanColumnMapping());
        }
        else if (type instanceof TinyintType) {
            return Optional.of(tinyintColumnMapping());
        }
        else if (type instanceof SmallintType) {
            return Optional.of(smallintColumnMapping());
        }
        else if (type instanceof IntegerType) {
            return Optional.of(integerColumnMapping());
        }
        else if (type instanceof BigintType) {
            return Optional.of(bigintColumnMapping());
        }
        else if (type instanceof RealType) {
            return Optional.of(realColumnMapping());
        }
        else if (type instanceof DoubleType) {
            return Optional.of(doubleColumnMapping());
        }
        else if (type instanceof DecimalType) {
            int scale = ((DecimalType) type).getScale();
            int precision = ((DecimalType) type).getPrecision();
            if (precision > Decimals.MAX_PRECISION) {
                return Optional.empty();
            }
            return Optional.of(decimalColumnMapping(createDecimalType(precision, max(scale, 0))));
        }
        else if (type instanceof CharType) {
            //TODO: check if there is any way of getting the column size here
            return Optional.of(charColumnMapping(createCharType(CharType.MAX_LENGTH)));
        }
        else if (type instanceof VarcharType) {
            //TODO: check if there is any way of getting the actual column size here
            return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
        }
        else if (type instanceof VarbinaryType) {
            return Optional.of(varbinaryColumnMapping());
        }
        else if (type instanceof DateType) {
            return Optional.of(dateColumnMapping());
        }
        else if (type instanceof TimeType) {
            return Optional.of(timeColumnMapping());
        }
        else if (type instanceof TimestampType) {
            return Optional.of(timestampColumnMapping((TimestampType) type));
        }
        else if (type instanceof TimeWithTimeZoneType) {
            return Optional.of(timeWithTimeZoneColumnMapping());
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of(timestampWithTimeZoneColumnMapping());
        }
        else if (type instanceof UuidType) {
            return Optional.of(uuidColumnMapping());
        }
        return Optional.empty();
    }
}
