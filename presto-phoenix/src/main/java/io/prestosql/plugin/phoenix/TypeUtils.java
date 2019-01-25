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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.joda.time.DateTimeZone;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.readBigDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public final class TypeUtils
{
    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "BOOLEAN")
            .put(BIGINT, "BIGINT")
            .put(INTEGER, "INTEGER")
            .put(SMALLINT, "SMALLINT")
            .put(TINYINT, "TINYINT")
            .put(DOUBLE, "DOUBLE")
            .put(REAL, "FLOAT")
            .put(VARBINARY, "VARBINARY")
            .put(DATE, "DATE")
            .put(TIME, "TIME")
            .put(TIME_WITH_TIME_ZONE, "TIME")
            .put(TIMESTAMP, "TIMESTAMP")
            .put(TIMESTAMP_WITH_TIME_ZONE, "TIMESTAMP")
            .build();

    private TypeUtils()
    {
    }

    public static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static boolean isDateType(Type type)
    {
        return type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE);
    }

    public static String toSqlType(Type type)
    {
        if (type instanceof VarcharType) {
            if (((VarcharType) type).isUnbounded()) {
                return "VARCHAR";
            }
            return "VARCHAR(" + ((VarcharType) type).getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "CHAR";
            }
            return "CHAR(" + ((CharType) type).getLength() + ")";
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }

        if (type instanceof DecimalType) {
            return type.toString().toUpperCase();
        }

        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);
            sqlType = toSqlType(elementType);
            return sqlType + " ARRAY[]";
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getTypeSignature());
    }

    public static Type toPrestoType(int phoenixType, int columnSize, int decimalDigits, int arraySize, int rawTypeId)
    {
        switch (phoenixType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
                return DOUBLE;
            case Types.DECIMAL:
                return DecimalType.createDecimalType(columnSize, decimalDigits);
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize == 0 || columnSize > VarcharType.MAX_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.ARRAY:
                PDataType<?> baseType = PDataType.fromTypeId(rawTypeId - PDataType.ARRAY_TYPE_BASE);
                Type basePrestoType = toPrestoType(baseType.getSqlType(), columnSize, decimalDigits, arraySize, rawTypeId);
                return new ArrayType(basePrestoType);
        }
        return null;
    }

    public static Object getObjectValue(Type type, Block block, int position)
            throws SQLException
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
            return PArrayDataType.instantiatePhoenixArray(PDataType.fromSqlTypeName(sqlType),
                    elements);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }
}
