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
package com.facebook.presto.hive;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import javax.annotation.Nullable;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRUCT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public enum HiveType
{
    BOOLEAN(BooleanType.BOOLEAN, BOOLEAN_TYPE_NAME, PrimitiveCategory.BOOLEAN),
    BYTE(BigintType.BIGINT, TINYINT_TYPE_NAME, PrimitiveCategory.BYTE),
    SHORT(BigintType.BIGINT, SMALLINT_TYPE_NAME, PrimitiveCategory.SHORT),
    INT(BigintType.BIGINT, INT_TYPE_NAME, PrimitiveCategory.INT),
    LONG(BigintType.BIGINT, BIGINT_TYPE_NAME, PrimitiveCategory.LONG),
    FLOAT(DoubleType.DOUBLE, FLOAT_TYPE_NAME, PrimitiveCategory.FLOAT),
    DOUBLE(DoubleType.DOUBLE, DOUBLE_TYPE_NAME, PrimitiveCategory.DOUBLE),
    STRING(VarcharType.VARCHAR, STRING_TYPE_NAME, PrimitiveCategory.STRING),
    TIMESTAMP(TimestampType.TIMESTAMP, TIMESTAMP_TYPE_NAME, PrimitiveCategory.TIMESTAMP),
    BINARY(VarbinaryType.VARBINARY, BINARY_TYPE_NAME, PrimitiveCategory.BINARY),
    LIST(VarcharType.VARCHAR, LIST_TYPE_NAME, null),
    MAP(VarcharType.VARCHAR, MAP_TYPE_NAME, null),
    STRUCT(VarcharType.VARCHAR, STRUCT_TYPE_NAME, null);

    private final Type nativeType;
    private final String hiveTypeName;
    @Nullable
    private final PrimitiveCategory primitiveCategory;

    HiveType(Type nativeType, String hiveTypeName, @Nullable PrimitiveCategory primitiveCategory)
    {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
        this.hiveTypeName = checkNotNull(hiveTypeName, "hiveTypeName is null");
        this.primitiveCategory = primitiveCategory;
    }

    private static final Map<String, HiveType> HIVE_TYPE_NAMES;
    private static final Map<PrimitiveCategory, HiveType> PRIMITIVE_CATEGORIES;

    static {
        ImmutableBiMap.Builder<String, HiveType> typeMap = ImmutableBiMap.builder();
        ImmutableBiMap.Builder<PrimitiveCategory, HiveType> categoryMap = ImmutableBiMap.builder();
        for (HiveType type : values()) {
            typeMap.put(type.getHiveTypeName(), type);
            if (type.primitiveCategory != null) {
                categoryMap.put(type.primitiveCategory, type);
            }
        }
        HIVE_TYPE_NAMES = typeMap.build();
        PRIMITIVE_CATEGORIES = categoryMap.build();
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public String getHiveTypeName()
    {
        return hiveTypeName;
    }

    public static HiveType getSupportedHiveType(PrimitiveCategory primitiveCategory)
    {
        HiveType hiveType = PRIMITIVE_CATEGORIES.get(primitiveCategory);
        checkArgument(hiveType != null, "Unknown primitive Hive category: " + primitiveCategory);
        return hiveType;
    }

    public static HiveType getSupportedHiveType(String hiveTypeName)
    {
        HiveType hiveType = HIVE_TYPE_NAMES.get(hiveTypeName);
        checkArgument(hiveType != null, "Unknown Hive type: " + hiveTypeName);
        return hiveType;
    }

    public static HiveType getHiveType(String hiveTypeName)
    {
        return HIVE_TYPE_NAMES.get(hiveTypeName);
    }

    public static HiveType getSupportedHiveType(ObjectInspector fieldInspector)
    {
        HiveType hiveType = getHiveType(fieldInspector);
        checkArgument(hiveType != null, "Unknown Hive category: " + fieldInspector.getCategory());
        return hiveType;
    }

    public static HiveType getHiveType(ObjectInspector fieldInspector)
    {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return PRIMITIVE_CATEGORIES.get(((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory());
            case MAP:
                return MAP;
            case LIST:
                return LIST;
            case STRUCT:
                return STRUCT;
            default:
                return null;
        }
    }

    public static HiveType toHiveType(Type type)
    {
        if (BooleanType.BOOLEAN.equals(type)) {
            return BOOLEAN;
        }
        if (BigintType.BIGINT.equals(type)) {
            return LONG;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return DOUBLE;
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return STRING;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static Function<Type, HiveType> columnTypeToHiveType()
    {
        return new Function<Type, HiveType>()
        {
            @Override
            public HiveType apply(Type type)
            {
                return toHiveType(type);
            }
        };
    }

    public static Function<HiveType, String> hiveTypeNameGetter()
    {
        return new Function<HiveType, String>()
        {
            @Override
            public String apply(HiveType type)
            {
                return type.getHiveTypeName();
            }
        };
    }
}
