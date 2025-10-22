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

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
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
import static com.facebook.presto.common.type.UuidType.prestoUuidToJavaUuid;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.GeometryUtils.getAsText;
import static com.facebook.presto.plugin.jdbc.GeometryUtils.stGeomFromBinary;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.createBooleanReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.createDoubleReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.createLongReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.createSliceReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.WriteMapping.createBooleanWriteMapping;
import static com.facebook.presto.plugin.jdbc.mapping.WriteMapping.createDoubleWriteMapping;
import static com.facebook.presto.plugin.jdbc.mapping.WriteMapping.createLongWriteMapping;
import static com.facebook.presto.plugin.jdbc.mapping.WriteMapping.createSliceWriteMapping;
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

    public static ReadMapping booleanReadMapping()
    {
        return createBooleanReadMapping(BOOLEAN, ResultSet::getBoolean);
    }

    public static WriteMapping booleanWriteMapping()
    {
        return createBooleanWriteMapping(PreparedStatement::setBoolean);
    }

    public static ReadMapping tinyintReadMapping()
    {
        return createLongReadMapping(TINYINT, ResultSet::getByte);
    }

    public static WriteMapping tinyintWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> statement.setByte(index, SignedBytes.checkedCast(value))));
    }

    public static ReadMapping smallintReadMapping()
    {
        return createLongReadMapping(SMALLINT, ResultSet::getShort);
    }

    public static WriteMapping smallintWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> statement.setShort(index, Shorts.checkedCast(value))));
    }

    public static ReadMapping integerReadMapping()
    {
        return createLongReadMapping(INTEGER, ResultSet::getInt);
    }

    public static WriteMapping integerWriteMapping()
    {
        return createLongWriteMapping((((statement, index, value) -> statement.setInt(index, toIntExact(value)))));
    }

    public static ReadMapping bigintReadMapping()
    {
        return createLongReadMapping(BIGINT, ResultSet::getLong);
    }

    public static WriteMapping bigintWriteMapping()
    {
        return createLongWriteMapping(PreparedStatement::setLong);
    }

    public static ReadMapping realReadMapping()
    {
        return createLongReadMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)));
    }
    public static WriteMapping realWriteMapping()
    {
        return createLongWriteMapping((statement, index, value) -> statement.setFloat(index, intBitsToFloat(toIntExact(value))));
    }

    public static ReadMapping doubleReadMapping()
    {
        return createDoubleReadMapping(DOUBLE, ResultSet::getDouble);
    }

    public static WriteMapping doubleWriteMapping()
    {
        return createDoubleWriteMapping(PreparedStatement::setDouble);
    }

    public static ReadMapping decimalReadMapping(DecimalType decimalType)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return createLongReadMapping(decimalType, (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), scale));
        }
        return createSliceReadMapping(decimalType, (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex), scale));
    }

    public static WriteMapping decimalWriteMapping(DecimalType decimalType)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return createLongWriteMapping(((statement, index, value) -> {
                BigInteger unscaledValue = BigInteger.valueOf(value);
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                statement.setBigDecimal(index, bigDecimal);
            }));
        }
        return createSliceWriteMapping(((statement, index, value) -> {
            BigInteger unscaledValue = decodeUnscaledValue(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            statement.setBigDecimal(index, bigDecimal);
        }));
    }

    public static ReadMapping charReadMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return createSliceReadMapping(charType, (resultSet, columnIndex) -> utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex))));
    }

    public static WriteMapping charWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> statement.setString(index, value.toStringUtf8())));
    }

    public static ReadMapping varcharReadMapping(VarcharType varcharType)
    {
        return createSliceReadMapping(varcharType, (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)));
    }

    public static ReadMapping varbinaryReadMapping()
    {
        return createSliceReadMapping(VARBINARY, (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)));
    }

    public static WriteMapping varbinaryWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> statement.setBytes(index, value.getBytes())));
    }

    public static ReadMapping dateReadMapping()
    {
        return createLongReadMapping(DATE, (resultSet, columnIndex) -> {
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
        });
    }
    public static WriteMapping dateWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> statement.setDate(index, new Date(UTC.getMillisKeepLocal
                        (DateTimeZone.getDefault(), DAYS.toMillis(value))))));
    }

    public static ReadMapping timeReadMapping()
    {
        return createLongReadMapping(TIME, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTime(columnIndex)` returns wrong value if JVM's zone had forward offset change during 1970-01-01
             * and the time value being retrieved was not present in local time (a 'gap'), e.g. time retrieved is 00:10:00 and JVM zone is America/Hermosillo
             * The problem can be averted by using `resultSet.getObject(columnIndex, LocalTime.class)` -- but this is not universally supported by JDBC drivers.
             */
            Time time = resultSet.getTime(columnIndex);
            return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
        });
    }

    public static WriteMapping timeWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> statement.setTime(index, new Time(value))));
    }

    public static ReadMapping timestampReadMapping()
    {
        return createLongReadMapping(TIMESTAMP, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTimestamp(columnIndex)` returns wrong value if JVM's zone had forward offset change and the local time
             * corresponding to timestamp value being retrieved was not present (a 'gap'), this includes regular DST changes (e.g. Europe/Warsaw)
             * and one-time policy changes (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00).
             * The problem can be averted by using `resultSet.getObject(columnIndex, LocalDateTime.class)` -- but this is not universally supported by JDBC drivers.
             */
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return timestamp.getTime();
        });
    }

    public static WriteMapping timestampWriteMapping(TimestampType timestampType)
    {
        return createLongWriteMapping((statement, index, value) -> statement.setTimestamp(index, Timestamp.from(Instant.ofEpochSecond(
                timestampType.getEpochSecond(value),
                timestampType.getNanos(value)))));
    }
    public static WriteMapping uuidWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> statement.setObject(index, prestoUuidToJavaUuid(value))));
    }

    public static WriteMapping timeWithTimeZoneWriteMapping()
    {
        return createLongWriteMapping((((statement, index, value) -> statement.setTime(index, new Time(unpackMillisUtc(value))))));
    }

    public static WriteMapping timestampWithTimeZoneWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> statement.setTimestamp(index, new Timestamp(unpackMillisUtc(value)))));
    }

    public static Optional<ReadMapping> jdbcTypeToReadMapping(JdbcTypeHandle type)
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

    public static Optional<WriteMapping> prestoTypeToWriteMapping(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of(booleanWriteMapping());
        }
        else if (type.equals(TINYINT)) {
            return Optional.of(tinyintWriteMapping());
        }
        else if (type.equals(SMALLINT)) {
            return Optional.of(smallintWriteMapping());
        }
        else if (type.equals(BIGINT)) {
            return Optional.of(bigintWriteMapping());
        }
        else if (type.equals(DOUBLE)) {
            return Optional.of(doubleWriteMapping());
        }
        else if (type.equals(INTEGER)) {
            return Optional.of(integerWriteMapping());
        }
        else if (type.equals(REAL)) {
            return Optional.of(realWriteMapping());
        }
        else if (type instanceof DecimalType) {
            return Optional.of(decimalWriteMapping((DecimalType) type));
        }
        else if (type instanceof CharType || type instanceof VarcharType) {
            return Optional.of(charWriteMapping());
        }
        else if (type.equals(VARBINARY)) {
            return Optional.of(varbinaryWriteMapping());
        }
        else if (type instanceof DateType) {
            return Optional.of(dateWriteMapping());
        }
        else if (type instanceof TimestampType) {
            return Optional.of(timestampWriteMapping((TimestampType) type));
        }
        else if (type.equals(TIME)) {
            return Optional.of(timeWriteMapping());
        }
        else if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Optional.of(timeWithTimeZoneWriteMapping());
        }
        else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Optional.of(timestampWithTimeZoneWriteMapping());
        }
        else if (type.equals(UUID)) {
            return Optional.of(uuidWriteMapping());
        }
        return Optional.empty();
    }

    public static Optional<WriteMapping> getWriteMappingForAccumulators(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of(booleanWriteMapping());
        }
        else if (type.equals(TINYINT)) {
            return Optional.of(tinyintWriteMapping());
        }
        else if (type.equals(SMALLINT)) {
            return Optional.of(smallintWriteMapping());
        }
        else if (type.equals(INTEGER)) {
            return Optional.of(integerWriteMapping());
        }
        else if (type.equals(BIGINT)) {
            return Optional.of(bigintWriteMapping());
        }
        else if (type.equals(REAL)) {
            return Optional.of(realWriteMapping());
        }
        else if (type.equals(DOUBLE)) {
            return Optional.of(doubleWriteMapping());
        }
        else if (type instanceof CharType || type instanceof VarcharType) {
            return Optional.of(charWriteMapping());
        }
        else if (type instanceof DecimalType) {
            return Optional.of(decimalWriteMapping((DecimalType) type));
        }
        else if (type.equals(DateType.DATE)) {
            return Optional.of(dateWriteMapping());
        }
        else if (type.equals(TIME)) {
            return Optional.of(timeWriteMapping());
        }
        else if (type.equals(TIMESTAMP)) {
            return Optional.of(timestampWriteMapping((TimestampType) type));
        }
        else if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Optional.of(timeWithTimeZoneWriteMapping());
        }
        else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Optional.of(timestampWithTimeZoneWriteMapping());
        }
        else if (type instanceof UuidType) {
            return Optional.of(uuidWriteMapping());
        }
        return Optional.empty();
    }
    public static ReadMapping geometryReadMapping()
    {
        return createSliceReadMapping(VARCHAR,
                (resultSet, columnIndex) -> getAsText(stGeomFromBinary(wrappedBuffer(resultSet.getBytes(columnIndex)))));
    }
}
