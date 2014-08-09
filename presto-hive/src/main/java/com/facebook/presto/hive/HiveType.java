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
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

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
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public enum HiveType
{
    BOOLEAN(BooleanType.BOOLEAN),
    BYTE(BigintType.BIGINT),
    SHORT(BigintType.BIGINT),
    INT(BigintType.BIGINT),
    LONG(BigintType.BIGINT),
    FLOAT(DoubleType.DOUBLE),
    DOUBLE(DoubleType.DOUBLE),
    STRING(VarcharType.VARCHAR),
    TIMESTAMP(TimestampType.TIMESTAMP),
    DATE(DateType.DATE),
    BINARY(VarbinaryType.VARBINARY),
    LIST(VarcharType.VARCHAR),
    MAP(VarcharType.VARCHAR),
    STRUCT(VarcharType.VARCHAR);

    private final Type nativeType;

    HiveType(Type nativeType)
    {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public String getHiveTypeName()
    {
        return HIVE_TYPE_NAMES.inverse().get(this);
    }

    public static HiveType getSupportedHiveType(PrimitiveCategory primitiveCategory)
    {
        HiveType hiveType = getHiveType(primitiveCategory);
        checkArgument(hiveType != null, "Unknown primitive Hive category: " + primitiveCategory);
        return hiveType;
    }

    public static HiveType getHiveType(PrimitiveCategory primitiveCategory)
    {
        switch (primitiveCategory) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BYTE;
            case SHORT:
                return SHORT;
            case INT:
                return INT;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return STRING;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return BINARY;
            default:
                return null;
        }
    }

    public static HiveType getSupportedHiveType(String hiveTypeName)
    {
        HiveType hiveType = getHiveType(hiveTypeName);
        checkArgument(hiveType != null, "Unknown Hive type: " + hiveTypeName);
        return hiveType;
    }

    private static final BiMap<String, HiveType> HIVE_TYPE_NAMES = ImmutableBiMap.<String, HiveType>builder()
            .put(BOOLEAN_TYPE_NAME, BOOLEAN)
            .put(TINYINT_TYPE_NAME, BYTE)
            .put(SMALLINT_TYPE_NAME, SHORT)
            .put(INT_TYPE_NAME, INT)
            .put(BIGINT_TYPE_NAME, LONG)
            .put(FLOAT_TYPE_NAME, FLOAT)
            .put(DOUBLE_TYPE_NAME, DOUBLE)
            .put(STRING_TYPE_NAME, STRING)
            .put(DATE_TYPE_NAME, DATE)
            .put(TIMESTAMP_TYPE_NAME, TIMESTAMP)
            .put(BINARY_TYPE_NAME, BINARY)
            .put(LIST_TYPE_NAME, LIST)
            .put(MAP_TYPE_NAME, MAP)
            .put(STRUCT_TYPE_NAME, STRUCT)
            .build();

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
                return getHiveType(((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory());
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
        if (VarbinaryType.VARBINARY.equals(type)) {
            return BINARY;
        }
        if (DateType.DATE.equals(type)) {
            return DATE;
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            return TIMESTAMP;
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
