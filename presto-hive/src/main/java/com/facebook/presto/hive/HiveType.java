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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveUtil.getDecimalType;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public final class HiveType
{
    public static final HiveType HIVE_BOOLEAN = new HiveType(BOOLEAN_TYPE_NAME);
    public static final HiveType HIVE_BYTE = new HiveType(TINYINT_TYPE_NAME);
    public static final HiveType HIVE_SHORT = new HiveType(SMALLINT_TYPE_NAME);
    public static final HiveType HIVE_INT = new HiveType(INT_TYPE_NAME);
    public static final HiveType HIVE_LONG = new HiveType(BIGINT_TYPE_NAME);
    public static final HiveType HIVE_FLOAT = new HiveType(FLOAT_TYPE_NAME);
    public static final HiveType HIVE_DOUBLE = new HiveType(DOUBLE_TYPE_NAME);
    public static final HiveType HIVE_STRING = new HiveType(STRING_TYPE_NAME);
    public static final HiveType HIVE_TIMESTAMP = new HiveType(TIMESTAMP_TYPE_NAME);
    public static final HiveType HIVE_DATE = new HiveType(DATE_TYPE_NAME);
    public static final HiveType HIVE_BINARY = new HiveType(BINARY_TYPE_NAME);

    private static final Set<HiveType> SUPPORTED_HIVE_TYPES = ImmutableSet.of(
            HIVE_BOOLEAN,
            HIVE_BYTE,
            HIVE_SHORT,
            HIVE_INT,
            HIVE_LONG,
            HIVE_FLOAT,
            HIVE_DOUBLE,
            HIVE_STRING,
            HIVE_TIMESTAMP,
            HIVE_DATE,
            HIVE_BINARY);

    private final String hiveTypeName;
    private final Category category;

    private HiveType(String hiveTypeName)
    {
        this.hiveTypeName = checkNotNull(hiveTypeName, "hiveTypeName is null");
        this.category = TypeInfoUtils.getTypeInfoFromTypeString(hiveTypeName).getCategory();
    }

    @JsonValue
    public String getHiveTypeName()
    {
        return hiveTypeName;
    }

    public Category getCategory()
    {
        return category;
    }

    public static HiveType getSupportedHiveType(String hiveTypeName)
    {
        HiveType hiveType = getHiveType(hiveTypeName);
        checkArgument(hiveType != null, "Unknown Hive type: " + hiveTypeName);
        return hiveType;
    }

    @JsonCreator
    @Nullable
    public static HiveType getHiveType(String hiveTypeName)
    {
        HiveType hiveType = new HiveType(hiveTypeName);
        if (!isStructuralType(hiveType) && !SUPPORTED_HIVE_TYPES.contains(hiveType) && !getDecimalType(hiveType).isPresent()) {
            return null;
        }
        return hiveType;
    }

    public static HiveType getSupportedHiveType(ObjectInspector fieldInspector)
    {
        HiveType hiveType = getHiveType(fieldInspector);
        checkArgument(hiveType != null, "Unknown Hive category: " + fieldInspector.getCategory());
        return hiveType;
    }

    public static HiveType getHiveType(ObjectInspector fieldInspector)
    {
        return getHiveType(fieldInspector.getTypeName());
    }

    public static HiveType toHiveType(Type type)
    {
        if (BooleanType.BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN;
        }
        if (BigintType.BIGINT.equals(type)) {
            return HIVE_LONG;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return HIVE_DOUBLE;
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return HIVE_STRING;
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return HIVE_BINARY;
        }
        if (DateType.DATE.equals(type)) {
            return HIVE_DATE;
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            return HIVE_TIMESTAMP;
        }
        if (isArrayType(type)) {
            HiveType hiveElementType = toHiveType(type.getTypeParameters().get(0));
            return new HiveType(format("array<%s>", hiveElementType.getHiveTypeName()));
        }
        if (isMapType(type)) {
            HiveType hiveKeyType = toHiveType(type.getTypeParameters().get(0));
            HiveType hiveValueType = toHiveType(type.getTypeParameters().get(1));
            return new HiveType(format("map<%s,%s>", hiveKeyType.getHiveTypeName(), hiveValueType.getHiveTypeName()));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new HiveType(format("decimal(%s,%s)", decimalType.getPrecision(), decimalType.getScale()));
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveType hiveType = (HiveType) o;

        if (!hiveTypeName.equals(hiveType.hiveTypeName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return hiveTypeName.hashCode();
    }

    @Override
    public String toString()
    {
        return hiveTypeName;
    }

    public static Type getType(String hiveType)
    {
        Optional<DecimalType> decimalType = getDecimalType(hiveType);
        if (decimalType.isPresent()) {
            return decimalType.get();
        }

        switch (hiveType) {
            case BOOLEAN_TYPE_NAME:
                return BOOLEAN;
            case TINYINT_TYPE_NAME:
            case SMALLINT_TYPE_NAME:
            case INT_TYPE_NAME:
            case BIGINT_TYPE_NAME:
                return BIGINT;
            case FLOAT_TYPE_NAME:
            case DOUBLE_TYPE_NAME:
                return DOUBLE;
            case STRING_TYPE_NAME:
                return VARCHAR;
            case DATE_TYPE_NAME:
                return DATE;
            case TIMESTAMP_TYPE_NAME:
                return TIMESTAMP;
            case BINARY_TYPE_NAME:
                return VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported hive type " + hiveType);
        }
    }

    @Nullable
    public static Type getType(ObjectInspector fieldInspector, TypeManager typeManager)
    {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType(((PrimitiveObjectInspector) fieldInspector));
            case MAP:
                MapObjectInspector mapObjectInspector = checkType(fieldInspector, MapObjectInspector.class, "fieldInspector");
                Type keyType = getType(mapObjectInspector.getMapKeyObjectInspector(), typeManager);
                Type valueType = getType(mapObjectInspector.getMapValueObjectInspector(), typeManager);
                if (keyType == null || valueType == null) {
                    return null;
                }
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
            case LIST:
                ListObjectInspector listObjectInspector = checkType(fieldInspector, ListObjectInspector.class, "fieldInspector");
                Type elementType = getType(listObjectInspector.getListElementObjectInspector(), typeManager);
                if (elementType == null) {
                    return null;
                }
                return typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
            case STRUCT:
                StructObjectInspector structObjectInspector = checkType(fieldInspector, StructObjectInspector.class, "fieldInspector");
                List<TypeSignature> fieldTypes = new ArrayList<>();
                List<Object> fieldNames = new ArrayList<>();
                for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                    fieldNames.add(field.getFieldName());
                    Type fieldType = getType(field.getFieldObjectInspector(), typeManager);
                    if (fieldType == null) {
                        return null;
                    }
                    fieldTypes.add(fieldType.getTypeSignature());
                }
                return typeManager.getParameterizedType(StandardTypes.ROW, fieldTypes, fieldNames);
            default:
                throw new IllegalArgumentException("Unsupported hive type " + fieldInspector.getTypeName());
        }
    }

    public static Type getPrimitiveType(PrimitiveObjectInspector primitiveObjectInspector)
    {
        switch (primitiveObjectInspector.getPrimitiveCategory()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BIGINT;
            case SHORT:
                return BIGINT;
            case INT:
                return BIGINT;
            case LONG:
                return BIGINT;
            case FLOAT:
                return DOUBLE;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return VARCHAR;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                return createDecimalType(primitiveObjectInspector.precision(), primitiveObjectInspector.scale());
            default:
                return null;
        }
    }
}
