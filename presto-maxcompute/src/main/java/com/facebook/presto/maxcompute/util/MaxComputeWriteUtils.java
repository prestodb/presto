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
package com.facebook.presto.maxcompute.util;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.facebook.presto.common.block.Block;
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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.aliyun.odps.OdpsType.DECIMAL;
import static com.aliyun.odps.OdpsType.STRING;
import static com.aliyun.odps.OdpsType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.padEnd;
import static java.lang.Float.intBitsToFloat;
import static org.joda.time.DateTimeZone.UTC;

public class MaxComputeWriteUtils
{
    private MaxComputeWriteUtils() {}

    public static Object deSerializeObject(Type type, TypeInfo typeInfo, Object value)
    {
        String base = type.getTypeSignature().getBase();
        Block block = (Block) value;
        switch (base) {
            case StandardTypes.ARRAY:
                return parseArrayData(type, (ArrayTypeInfo) typeInfo, block);
            case StandardTypes.MAP:
                return parseMapData(type, (MapTypeInfo) typeInfo, block);
            case StandardTypes.ROW:
                return parseStructData(type, (StructTypeInfo) typeInfo, block);
        }
        return null;
    }

    private static List<Object> parseArrayData(Type type, ArrayTypeInfo typeInfo, Block block)
    {
        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "Array must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);

        List<Object> list = new ArrayList<>(block.getPositionCount());

        for (int i = 0; i < block.getPositionCount(); i++) {
            Object element = getField(elementType, typeInfo.getElementTypeInfo(), block, i);
            list.add(element);
        }
        return list;
    }

    private static Map<Object, Object> parseMapData(Type type, MapTypeInfo typeInfo, Block block)
    {
        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "Map must have exactly 2 type parameters");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            Object key = getField(keyType, typeInfo.getKeyTypeInfo(), block, i);
            Object value = getField(valueType, typeInfo.getValueTypeInfo(), block, i + 1);
            map.put(key, value);
        }
        return map;
    }

    private static SimpleStruct parseStructData(Type type, StructTypeInfo typeInfo, Block block)
    {
        List<Type> fieldTypes = type.getTypeParameters();
        checkArgument(fieldTypes.size() == block.getPositionCount(),
                "Expected row value field count does not match type field count");

        List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();

        List<Object> row = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            Object element = getField(fieldTypes.get(i), fieldTypeInfos.get(i), block, i);
            row.add(element);
        }

        return new SimpleStruct(typeInfo, row);
    }

    private static Object getField(Type type, TypeInfo typeInfo, Block block, int position)
    {
        OdpsType odpsType = typeInfo.getOdpsType();
        if (block.isNull(position)) {
            return null;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BigintType.BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return (int) type.getLong(block, position);
        }
        if (SmallintType.SMALLINT.equals(type)) {
            return (short) type.getLong(block, position);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return (byte) type.getLong(block, position);
        }
        if (RealType.REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DoubleType.DOUBLE.equals(type)) {
            double value = type.getDouble(block, position);
            if (odpsType == DECIMAL) {
                return BigDecimal.valueOf(value);
            }

            return value;
        }
        if (type instanceof VarcharType) {
            Slice slice = type.getSlice(block, position);

            if (odpsType == STRING) {
                return new String(slice.getBytes());
            }
            else if (odpsType == VARCHAR) {
                VarcharType varcharType = (VarcharType) type;
                return new Varchar(new String(slice.getBytes()), varcharType.getLengthSafe());
            }
            else { // CHAR
                VarcharType varcharType = (VarcharType) type;
                String str = padEnd(type.getSlice(block, position).toStringUtf8(), varcharType.getLengthSafe(), ' ');
                return new Char(str, varcharType.getLengthSafe());
            }
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String str = padEnd(type.getSlice(block, position).toStringUtf8(), charType.getLength(), ' ');

            return new Char(str, charType.getLength());
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DateType.DATE.equals(type)) {
            long days = type.getLong(block, position);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), TimeUnit.DAYS.toMillis(days)));
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Timestamp(millisUtc);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaledValue;
            if (decimalType.isShort()) {
                unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
            }
            else {
                unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
            }

            return normalize(new BigDecimal(unscaledValue, decimalType.getScale()), true);
        }
        if (isArrayType(type)) {
            Block arrayBlock = block.getBlock(position);
            List<Object> list = parseArrayData(type, (ArrayTypeInfo) typeInfo, arrayBlock);
            return Collections.unmodifiableList(list);
        }
        if (isMapType(type)) {
            Block mapBlock = block.getBlock(position);
            Map<Object, Object> map = parseMapData(type, (MapTypeInfo) typeInfo, mapBlock);
            return Collections.unmodifiableMap(map);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getBlock(position);
            return parseStructData(type, (StructTypeInfo) typeInfo, rowBlock);
        }

        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    private static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    private static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    private static final int MAX_PRECISION = 38;
    private static final int MAX_SCALE = 38;

    public static BigDecimal normalize(BigDecimal bd, boolean allowRounding)
    {
        if (bd == null) {
            return null;
        }

        bd = trim(bd);

        int intDigits = bd.precision() - bd.scale();

        if (intDigits > MAX_PRECISION) {
            return null;
        }

        int maxScale = Math.min(MAX_SCALE, Math.min(MAX_PRECISION - intDigits, bd.scale()));
        if (bd.scale() > maxScale) {
            if (allowRounding) {
                bd = bd.setScale(maxScale, RoundingMode.HALF_UP);
                // Trimming is again necessary, because rounding may introduce new trailing 0's.
                bd = trim(bd);
            }
            else {
                bd = null;
            }
        }

        return bd;
    }

    private static BigDecimal trim(BigDecimal d)
    {
        if (d.compareTo(BigDecimal.ZERO) == 0) {
            // Special case for 0, because java doesn't strip zeros correctly on that number.
            d = BigDecimal.ZERO;
        }
        else {
            d = d.stripTrailingZeros();
            if (d.scale() < 0) {
                // no negative scale decimals
                d = d.setScale(0);
            }
        }
        return d;
    }
}
