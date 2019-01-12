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
package io.prestosql.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static java.lang.Byte.parseByte;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

final class ObjectCasts
{
    private ObjectCasts() {}

    public static boolean castToBoolean(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (Boolean) x;
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).intValue() != 0;
            }
            if (x instanceof String) {
                switch (((String) x).toLowerCase(ENGLISH)) {
                    case "0":
                    case "false":
                        return false;
                    case "1":
                    case "true":
                        return true;
                }
                throw new IllegalArgumentException("Invalid boolean value: " + x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static byte castToByte(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (byte) (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).byteValue();
            }
            if (x instanceof String) {
                return parseByte((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static short castToShort(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (short) (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).shortValue();
            }
            if (x instanceof String) {
                return parseShort((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static int castToInt(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).intValue();
            }
            if (x instanceof String) {
                return parseInt((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static long castToLong(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).longValue();
            }
            if (x instanceof String) {
                return parseLong((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static float castToFloat(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).floatValue();
            }
            if (x instanceof String) {
                return parseFloat((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static double castToDouble(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return (((Boolean) x) ? 1 : 0);
        }
        try {
            if (x instanceof Number) {
                return ((Number) x).doubleValue();
            }
            if (x instanceof String) {
                return parseDouble((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static BigDecimal castToBigDecimal(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Boolean) {
            return BigDecimal.valueOf(((Boolean) x) ? 1 : 0);
        }
        if (x instanceof BigInteger) {
            return new BigDecimal((BigInteger) x);
        }
        if (x instanceof BigDecimal) {
            return (BigDecimal) x;
        }
        try {
            if ((x instanceof Byte) || (x instanceof Short) || (x instanceof Integer) || (x instanceof Long)) {
                return BigDecimal.valueOf(((Number) x).longValue());
            }
            if ((x instanceof Float) || (x instanceof Double)) {
                return BigDecimal.valueOf(((Number) x).doubleValue());
            }
            if (x instanceof String) {
                return new BigDecimal((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static byte[] castToBinary(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof byte[]) {
            return (byte[]) x;
        }
        if (x instanceof String) {
            return ((String) x).getBytes(UTF_8);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static Date castToDate(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Date) {
            return (Date) x;
        }
        if (x instanceof java.util.Date) {
            return new Date(((java.util.Date) x).getTime());
        }
        if (x instanceof LocalDate) {
            return Date.valueOf((LocalDate) x);
        }
        if (x instanceof LocalDateTime) {
            return Date.valueOf(((LocalDateTime) x).toLocalDate());
        }
        try {
            if (x instanceof String) {
                return Date.valueOf((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static Time castToTime(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Time) {
            return (Time) x;
        }
        if (x instanceof java.util.Date) {
            return new Time(((java.util.Date) x).getTime());
        }
        if (x instanceof LocalTime) {
            return Time.valueOf((LocalTime) x);
        }
        if (x instanceof LocalDateTime) {
            return Time.valueOf(((LocalDateTime) x).toLocalTime());
        }
        try {
            if (x instanceof String) {
                return Time.valueOf((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    public static Timestamp castToTimestamp(Object x, int targetSqlType)
            throws SQLException
    {
        if (x instanceof Timestamp) {
            return (Timestamp) x;
        }
        if (x instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) x).getTime());
        }
        if (x instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) x);
        }
        try {
            if (x instanceof String) {
                return Timestamp.valueOf((String) x);
            }
        }
        catch (RuntimeException e) {
            throw invalidConversion(x, targetSqlType, e);
        }
        throw invalidConversion(x, targetSqlType);
    }

    private static SQLException invalidConversion(Object x, int sqlType)
    {
        return invalidConversion(x, sqlType, null);
    }

    private static SQLException invalidConversion(Object x, int sqlType, Exception e)
    {
        return new SQLException(format("Cannot convert instance of %s to SQL type %s", x.getClass().getName(), sqlType), e);
    }
}
