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
package io.prestosql.plugin.kudu;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;

import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public class TypeHelper
{
    private TypeHelper()
    {
    }

    public static org.apache.kudu.Type toKuduClientType(Type type)
    {
        if (type instanceof VarcharType) {
            return org.apache.kudu.Type.STRING;
        }
        else if (type == TimestampType.TIMESTAMP) {
            return org.apache.kudu.Type.UNIXTIME_MICROS;
        }
        else if (type == BigintType.BIGINT) {
            return org.apache.kudu.Type.INT64;
        }
        else if (type == IntegerType.INTEGER) {
            return org.apache.kudu.Type.INT32;
        }
        else if (type == SmallintType.SMALLINT) {
            return org.apache.kudu.Type.INT16;
        }
        else if (type == TinyintType.TINYINT) {
            return org.apache.kudu.Type.INT8;
        }
        else if (type == RealType.REAL) {
            return org.apache.kudu.Type.FLOAT;
        }
        else if (type == DoubleType.DOUBLE) {
            return org.apache.kudu.Type.DOUBLE;
        }
        else if (type == BooleanType.BOOLEAN) {
            return org.apache.kudu.Type.BOOL;
        }
        else if (type instanceof VarbinaryType) {
            return org.apache.kudu.Type.BINARY;
        }
        else if (type instanceof DecimalType) {
            return org.apache.kudu.Type.DECIMAL;
        }
        else if (type == DateType.DATE) {
            return org.apache.kudu.Type.STRING;
        }
        else if (type instanceof CharType) {
            return org.apache.kudu.Type.STRING;
        }
        else {
            throw new IllegalStateException("Type mapping implemented for Presto type: " + type);
        }
    }

    public static Type fromKuduColumn(ColumnSchema column)
    {
        return fromKuduClientType(column.getType(), column.getTypeAttributes());
    }

    private static Type fromKuduClientType(org.apache.kudu.Type ktype, ColumnTypeAttributes attributes)
    {
        switch (ktype) {
            case STRING:
                return VarcharType.VARCHAR;
            case UNIXTIME_MICROS:
                return TimestampType.TIMESTAMP;
            case INT64:
                return BigintType.BIGINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT16:
                return SmallintType.SMALLINT;
            case INT8:
                return TinyintType.TINYINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOL:
                return BooleanType.BOOLEAN;
            case BINARY:
                return VarbinaryType.VARBINARY;
            case DECIMAL:
                return DecimalType.createDecimalType(attributes.getPrecision(), attributes.getScale());
            default:
                throw new IllegalStateException("Kudu type not implemented for " + ktype);
        }
    }

    public static Object getJavaValue(Type type, Object nativeValue)
    {
        if (type instanceof VarcharType) {
            return ((Slice) nativeValue).toStringUtf8();
        }
        else if (type == TimestampType.TIMESTAMP) {
            return ((Long) nativeValue) * 1000;
        }
        else if (type == BigintType.BIGINT) {
            return nativeValue;
        }
        else if (type == IntegerType.INTEGER) {
            return ((Long) nativeValue).intValue();
        }
        else if (type == SmallintType.SMALLINT) {
            return ((Long) nativeValue).shortValue();
        }
        else if (type == TinyintType.TINYINT) {
            return ((Long) nativeValue).byteValue();
        }
        else if (type == DoubleType.DOUBLE) {
            return nativeValue;
        }
        else if (type == RealType.REAL) {
            // conversion can result in precision lost
            return intBitsToFloat(((Long) nativeValue).intValue());
        }
        else if (type == BooleanType.BOOLEAN) {
            return nativeValue;
        }
        else if (type instanceof VarbinaryType) {
            return ((Slice) nativeValue).toByteBuffer();
        }
        else if (type instanceof DecimalType) {
            return nativeValue;
        }
        else {
            throw new IllegalStateException("Back conversion not implemented for " + type);
        }
    }

    public static Object getObject(Type type, RowResult row, int field)
    {
        if (row.isNull(field)) {
            return null;
        }
        else {
            if (type instanceof VarcharType) {
                return row.getString(field);
            }
            else if (type == TimestampType.TIMESTAMP) {
                return row.getLong(field) / 1000;
            }
            else if (type == BigintType.BIGINT) {
                return row.getLong(field);
            }
            else if (type == IntegerType.INTEGER) {
                return row.getInt(field);
            }
            else if (type == SmallintType.SMALLINT) {
                return row.getShort(field);
            }
            else if (type == TinyintType.TINYINT) {
                return row.getByte(field);
            }
            else if (type == DoubleType.DOUBLE) {
                return row.getDouble(field);
            }
            else if (type == RealType.REAL) {
                return row.getFloat(field);
            }
            else if (type == BooleanType.BOOLEAN) {
                return row.getBoolean(field);
            }
            else if (type instanceof VarbinaryType) {
                return Slices.wrappedBuffer(row.getBinary(field));
            }
            else if (type instanceof DecimalType) {
                return row.getDecimal(field);
            }
            else {
                throw new IllegalStateException("getObject not implemented for " + type);
            }
        }
    }

    public static long getLong(Type type, RowResult row, int field)
    {
        if (type == TimestampType.TIMESTAMP) {
            return row.getLong(field) / 1000;
        }
        else if (type == BigintType.BIGINT) {
            return row.getLong(field);
        }
        else if (type == IntegerType.INTEGER) {
            return row.getInt(field);
        }
        else if (type == SmallintType.SMALLINT) {
            return row.getShort(field);
        }
        else if (type == TinyintType.TINYINT) {
            return row.getByte(field);
        }
        else if (type == RealType.REAL) {
            return floatToRawIntBits(row.getFloat(field));
        }
        else if (type instanceof DecimalType) {
            DecimalType dtype = (DecimalType) type;
            if (dtype.isShort()) {
                return row.getDecimal(field).unscaledValue().longValue();
            }
            else {
                throw new IllegalStateException("getLong not supported for long decimal: " + type);
            }
        }
        else {
            throw new IllegalStateException("getLong not implemented for " + type);
        }
    }

    public static boolean getBoolean(Type type, RowResult row, int field)
    {
        if (type == BooleanType.BOOLEAN) {
            return row.getBoolean(field);
        }
        else {
            throw new IllegalStateException("getBoolean not implemented for " + type);
        }
    }

    public static double getDouble(Type type, RowResult row, int field)
    {
        if (type == DoubleType.DOUBLE) {
            return row.getDouble(field);
        }
        else {
            throw new IllegalStateException("getDouble not implemented for " + type);
        }
    }

    public static Slice getSlice(Type type, RowResult row, int field)
    {
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(row.getString(field));
        }
        else if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer(row.getBinary(field));
        }
        else if (type instanceof DecimalType) {
            BigDecimal dec = row.getDecimal(field);
            return Decimals.encodeScaledValue(dec);
        }
        else {
            throw new IllegalStateException("getSlice not implemented for " + type);
        }
    }
}
