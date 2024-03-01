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

package com.facebook.presto.hive.functions.type;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.CHAR;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getDecimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.varcharTypeInfo;

public final class HiveTypes
{
    private HiveTypes() {}

    public static HiveVarchar createHiveVarChar(String s)
    {
        return new HiveVarchar(s, s.length());
    }

    public static HiveVarchar createHiveVarChar(Slice slice)
    {
        String str = slice.toStringUtf8();
        return new HiveVarchar(str, str.length());
    }

    public static HiveChar createHiveChar(String s)
    {
        return new HiveChar(s, s.length());
    }

    public static HiveChar createHiveChar(Slice slice)
    {
        String str = slice.toStringUtf8();
        return new HiveChar(str, str.length());
    }

    public static TypeInfo toTypeInfo(Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        switch (type.getTypeSignature().getBase()) {
            case BIGINT:
                return longTypeInfo;
            case INTEGER:
                return intTypeInfo;
            case SMALLINT:
                return shortTypeInfo;
            case TINYINT:
                return byteTypeInfo;
            case BOOLEAN:
                return booleanTypeInfo;
            case DATE:
                return dateTypeInfo;
            case DECIMAL:
                return toDecimalTypeInfo(type);
            case REAL:
                return floatTypeInfo;
            case DOUBLE:
                return doubleTypeInfo;
            case TIMESTAMP:
                return timestampTypeInfo;
            case VARBINARY:
                return binaryTypeInfo;
            case VARCHAR:
                return toVarcharTypeInfo(type);
            case CHAR:
                return toCharTypeInfo(type);
            case ROW:
                return toStructTypeInfo(type);
            case ARRAY:
                return toListTypeInfo(type);
            case MAP:
                return toMapTypeInfo(type);
        }
        throw unsupportedType(signature);
    }

    private static TypeInfo toDecimalTypeInfo(Type type)
    {
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return getDecimalTypeInfo(decimal.getPrecision(), decimal.getScale());
        }
        throw unsupportedType(type);
    }

    private static TypeInfo toVarcharTypeInfo(Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varchar = (VarcharType) type;
            if (varchar.isUnbounded()) {
                return varcharTypeInfo;
            }
            return getVarcharTypeInfo(varchar.getLengthSafe());
        }
        throw unsupportedType(type);
    }

    private static TypeInfo toCharTypeInfo(Type type)
    {
        if (type instanceof CharType) {
            CharType chars = (CharType) type;
            return getCharTypeInfo(chars.getLength());
        }
        throw unsupportedType(type);
    }

    private static TypeInfo toStructTypeInfo(Type type)
    {
        if (type instanceof RowType) {
            RowType row = (RowType) type;
            List<RowType.Field> fields = row.getFields();
            List<String> fieldNames = new ArrayList<>(fields.size());
            List<TypeInfo> fieldTypes = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                fieldNames.add(field.getName().orElse("col" + i));
                fieldTypes.add(toTypeInfo(field.getType()));
            }
            return getStructTypeInfo(fieldNames, fieldTypes);
        }
        throw unsupportedType(type);
    }

    private static TypeInfo toListTypeInfo(Type type)
    {
        if (type instanceof ArrayType) {
            ArrayType array = (ArrayType) type;
            Type element = array.getElementType();
            return getListTypeInfo(toTypeInfo(element));
        }
        throw unsupportedType(type);
    }

    private static TypeInfo toMapTypeInfo(Type type)
    {
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return getMapTypeInfo(toTypeInfo(map.getKeyType()), toTypeInfo(map.getValueType()));
        }
        throw unsupportedType(type);
    }
}
