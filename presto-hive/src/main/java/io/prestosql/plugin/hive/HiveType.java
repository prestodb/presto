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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

public final class HiveType
{
    public static final HiveType HIVE_BOOLEAN = new HiveType(booleanTypeInfo);
    public static final HiveType HIVE_BYTE = new HiveType(byteTypeInfo);
    public static final HiveType HIVE_SHORT = new HiveType(shortTypeInfo);
    public static final HiveType HIVE_INT = new HiveType(intTypeInfo);
    public static final HiveType HIVE_LONG = new HiveType(longTypeInfo);
    public static final HiveType HIVE_FLOAT = new HiveType(floatTypeInfo);
    public static final HiveType HIVE_DOUBLE = new HiveType(doubleTypeInfo);
    public static final HiveType HIVE_STRING = new HiveType(stringTypeInfo);
    public static final HiveType HIVE_TIMESTAMP = new HiveType(timestampTypeInfo);
    public static final HiveType HIVE_DATE = new HiveType(dateTypeInfo);
    public static final HiveType HIVE_BINARY = new HiveType(binaryTypeInfo);

    private final HiveTypeName hiveTypeName;
    private final TypeInfo typeInfo;

    private HiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        this.hiveTypeName = new HiveTypeName(typeInfo.getTypeName());
        this.typeInfo = typeInfo;
    }

    public HiveTypeName getHiveTypeName()
    {
        return hiveTypeName;
    }

    public Category getCategory()
    {
        return typeInfo.getCategory();
    }

    public TypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    public TypeSignature getTypeSignature()
    {
        return getTypeSignature(typeInfo);
    }

    public Type getType(TypeManager typeManager)
    {
        return typeManager.getType(getTypeSignature());
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

    @JsonValue
    @Override
    public String toString()
    {
        return hiveTypeName.toString();
    }

    public boolean isSupportedType()
    {
        return isSupportedType(getTypeInfo());
    }

    public static boolean isSupportedType(TypeInfo typeInfo)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType((PrimitiveTypeInfo) typeInfo) != null;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return isSupportedType(mapTypeInfo.getMapKeyTypeInfo()) && isSupportedType(mapTypeInfo.getMapValueTypeInfo());
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return isSupportedType(listTypeInfo.getListElementTypeInfo());
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                return structTypeInfo.getAllStructFieldTypeInfos().stream()
                        .allMatch(HiveType::isSupportedType);
        }
        return false;
    }

    @JsonCreator
    public static HiveType valueOf(String hiveTypeName)
    {
        requireNonNull(hiveTypeName, "hiveTypeName is null");
        return toHiveType(getTypeInfoFromTypeString(hiveTypeName));
    }

    public static List<HiveType> toHiveTypes(String hiveTypes)
    {
        requireNonNull(hiveTypes, "hiveTypes is null");
        return ImmutableList.copyOf(getTypeInfosFromTypeString(hiveTypes).stream()
                .map(HiveType::toHiveType)
                .collect(toList()));
    }

    private static HiveType toHiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        return new HiveType(typeInfo);
    }

    public static HiveType toHiveType(TypeTranslator typeTranslator, Type type)
    {
        requireNonNull(typeTranslator, "typeTranslator is null");
        requireNonNull(type, "type is null");
        return new HiveType(typeTranslator.translate(type));
    }

    private static TypeSignature getTypeSignature(TypeInfo typeInfo)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                Type primitiveType = getPrimitiveType((PrimitiveTypeInfo) typeInfo);
                if (primitiveType == null) {
                    break;
                }
                return primitiveType.getTypeSignature();
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeSignature keyType = getTypeSignature(mapTypeInfo.getMapKeyTypeInfo());
                TypeSignature valueType = getTypeSignature(mapTypeInfo.getMapValueTypeInfo());
                return new TypeSignature(
                        StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                TypeSignature elementType = getTypeSignature(listTypeInfo.getListElementTypeInfo());
                return new TypeSignature(
                        StandardTypes.ARRAY,
                        ImmutableList.of(TypeSignatureParameter.of(elementType)));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<TypeInfo> structFieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                List<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
                if (structFieldTypeInfos.size() != structFieldNames.size()) {
                    throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, format("Invalid Hive struct type: %s", typeInfo));
                }
                ImmutableList.Builder<TypeSignatureParameter> typeSignatureBuilder = ImmutableList.builder();
                for (int i = 0; i < structFieldTypeInfos.size(); i++) {
                    TypeSignature typeSignature = getTypeSignature(structFieldTypeInfos.get(i));
                    // Lower case the struct field names.
                    // Otherwise, Presto will refuse to write to columns whose struct type has field names containing upper case characters.
                    // Users can't work around this by casting in their queries because Presto parser always lower case types.
                    // TODO: This is a hack. Presto engine should be able to handle identifiers in a case insensitive way where necessary.
                    String rowFieldName = structFieldNames.get(i).toLowerCase(Locale.US);
                    typeSignatureBuilder.add(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(rowFieldName, false)), typeSignature)));
                }
                return new TypeSignature(StandardTypes.ROW, typeSignatureBuilder.build());
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", typeInfo));
    }

    public static Type getPrimitiveType(PrimitiveTypeInfo typeInfo)
    {
        switch (typeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(((VarcharTypeInfo) typeInfo).getLength());
            case CHAR:
                return createCharType(((CharTypeInfo) typeInfo).getLength());
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.scale());
            default:
                return null;
        }
    }
}
