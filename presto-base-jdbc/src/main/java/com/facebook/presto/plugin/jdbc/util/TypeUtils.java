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

package com.facebook.presto.plugin.jdbc.util;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.DateTimeZone.UTC;

public final class TypeUtils
{
    private TypeUtils()
    {}

    // PostgreSQL jdbc array element names understood by org.postgresql.jdbc2.TypeInfoCache#getPGArrayType
    // for multidimensional arrays, this should return the base type (e.g. array(array(integer)) returns 'integer')
    public static String getArrayElementPgTypeName(ConnectorSession session, JdbcClient client, Type elementType)
    {
        if (DOUBLE.equals(elementType)) {
            return "float8";
        }

        if (REAL.equals(elementType)) {
            return "float4";
        }

        // TypeInfoCache#getPGArrayType apparently doesn't allow for specifying variable-length limits.
        // Since PostgreSQL char[] can only hold single byte elements, map all CharType to varchar
        if (elementType instanceof VarcharType || elementType instanceof CharType) {
            return "varchar";
        }

        if (elementType instanceof DecimalType) {
            return "decimal";
        }

        if (elementType instanceof ArrayType) {
            return getArrayElementPgTypeName(session, client, ((ArrayType) elementType).getElementType());
        }

        return client.toWriteMapping(session, elementType).getDataType();
    }

    public static Object[] getJdbcObjectArray(ConnectorSession session, Type elementType, Block block)
            throws SQLException
    {
        int positionCount = block.getPositionCount();
        Object[] valuesArray = new Object[positionCount];
        int subArrayLength = 1;
        for (int i = 0; i < positionCount; i++) {
            Object objectValue = prestoNativeToJdbcObject(session, elementType, readNativeValue(elementType, block, i));
            valuesArray[i] = objectValue;
            if (objectValue != null && objectValue.getClass().isArray()) {
                subArrayLength = Math.max(subArrayLength, Array.getLength(objectValue));
            }
        }
        // PostgreSql requires subarrays with matching dimensions, including arrays of null
        if (elementType instanceof ArrayType) {
            handleArrayNulls(valuesArray, subArrayLength);
        }
        return valuesArray;
    }

    public static int arrayDepth(Object jdbcArray)
    {
        checkArgument(jdbcArray.getClass().isArray(), "jdbcArray is not an array");
        int depth = 0;
        while (jdbcArray != null && jdbcArray.getClass().isArray()) {
            depth++;
            if (Array.getLength(jdbcArray) == 0) {
                return depth;
            }
            jdbcArray = Array.get(jdbcArray, 0);
        }
        return depth;
    }

    private static void handleArrayNulls(Object[] valuesArray, int length)
    {
        for (int i = 0; i < valuesArray.length; i++) {
            if (valuesArray[i] == null) {
                valuesArray[i] = new Object[length];
            }
        }
    }

    private static Object prestoNativeToJdbcObject(ConnectorSession session, Type prestoType, Object prestoNative)
            throws SQLException
    {
        if (prestoNative == null) {
            return null;
        }

        if (DOUBLE.equals(prestoType) || BOOLEAN.equals(prestoType) || BIGINT.equals(prestoType)) {
            return prestoNative;
        }

        if (prestoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) prestoType;
            if (decimalType.isShort()) {
                BigInteger unscaledValue = BigInteger.valueOf((long) prestoNative);
                return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            BigInteger unscaledValue = decodeUnscaledValue((Slice) prestoNative);
            return new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }

        if (REAL.equals(prestoType)) {
            return intBitsToFloat(toIntExact((long) prestoNative));
        }

        if (TINYINT.equals(prestoType)) {
            return SignedBytes.checkedCast((long) prestoNative);
        }

        if (SMALLINT.equals(prestoType)) {
            return Shorts.checkedCast((long) prestoNative);
        }

        if (INTEGER.equals(prestoType)) {
            return toIntExact((long) prestoNative);
        }

        if (DATE.equals(prestoType)) {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis((long) prestoNative);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis));
        }

        if (prestoType instanceof VarcharType || prestoType instanceof CharType) {
            return ((Slice) prestoNative).toStringUtf8();
        }

        if (prestoType instanceof ArrayType) {
            // process subarray of multi-dimensional array
            return getJdbcObjectArray(session, ((ArrayType) prestoType).getElementType(), (Block) prestoNative);
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported type: " + prestoType);
    }
}
