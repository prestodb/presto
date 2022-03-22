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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

public final class TypeConverter
{
    public static final String ORC_ICEBERG_ID_KEY = "iceberg.id";
    public static final String ORC_ICEBERG_REQUIRED_KEY = "iceberg.required";
    public static final String ICEBERG_LONG_TYPE = "iceberg.long-type";

    private TypeConverter() {}

    public static Type toPrestoType(org.apache.iceberg.types.Type type, TypeManager typeManager)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BINARY:
            case FIXED:
                return VarbinaryType.VARBINARY;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return DecimalType.createDecimalType(decimalType.precision(), decimalType.scale());
            case DOUBLE:
                return DoubleType.DOUBLE;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case INTEGER:
                return IntegerType.INTEGER;
            case STRING:
                return VarcharType.createUnboundedVarcharType();
            case LIST:
                Types.ListType listType = (Types.ListType) type;
                return new ArrayType(toPrestoType(listType.elementType(), typeManager));
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                TypeSignature keyType = toPrestoType(mapType.keyType(), typeManager).getTypeSignature();
                TypeSignature valueType = toPrestoType(mapType.valueType(), typeManager).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
            case STRUCT:
                List<Types.NestedField> fields = ((Types.StructType) type).fields();
                return RowType.from(fields.stream()
                        .map(field -> new RowType.Field(Optional.of(field.name()), toPrestoType(field.type(), typeManager)))
                        .collect(toImmutableList()));
            default:
                throw new UnsupportedOperationException(format("Cannot convert from Iceberg type '%s' (%s) to Presto type", type, type.typeId()));
        }
    }

    public static org.apache.iceberg.types.Type toIcebergType(Type type)
    {
        if (type instanceof BooleanType) {
            return Types.BooleanType.get();
        }
        if (type instanceof IntegerType) {
            return Types.IntegerType.get();
        }
        if (type instanceof BigintType) {
            return Types.LongType.get();
        }
        if (type instanceof RealType) {
            return Types.FloatType.get();
        }
        if (type instanceof DoubleType) {
            return Types.DoubleType.get();
        }
        if (type instanceof DecimalType) {
            return fromDecimal((DecimalType) type);
        }
        if (type instanceof VarcharType) {
            return Types.StringType.get();
        }
        if (type instanceof VarbinaryType) {
            return Types.BinaryType.get();
        }
        if (type instanceof DateType) {
            return Types.DateType.get();
        }
        if (type instanceof RowType) {
            return fromRow((RowType) type);
        }
        if (type instanceof ArrayType) {
            return fromArray((ArrayType) type);
        }
        if (type instanceof MapType) {
            return fromMap((MapType) type);
        }
        if (type instanceof TimeType) {
            throw new PrestoException(NOT_SUPPORTED, format("Time not supported for Iceberg."));
        }
        if (type instanceof TimestampType) {
            throw new PrestoException(NOT_SUPPORTED, format("Timestamp not supported for Iceberg."));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            throw new PrestoException(NOT_SUPPORTED, format("Timestamp with timezone not supported for Iceberg."));
        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }

    public static HiveType toHiveType(Type type)
    {
        return HiveType.toHiveType(toHiveTypeInfo(type));
    }

    private static org.apache.iceberg.types.Type fromDecimal(DecimalType type)
    {
        return Types.DecimalType.of(type.getPrecision(), type.getScale());
    }

    private static org.apache.iceberg.types.Type fromRow(RowType type)
    {
        List<Types.NestedField> fields = new ArrayList<>();
        for (RowType.Field field : type.getFields()) {
            String name = field.getName().orElseThrow(() ->
                    new PrestoException(NOT_SUPPORTED, "Row type field does not have a name: " + type.getDisplayName()));
            fields.add(Types.NestedField.optional(fields.size() + 1, name, toIcebergType(field.getType())));
        }
        return Types.StructType.of(fields);
    }

    private static org.apache.iceberg.types.Type fromArray(ArrayType type)
    {
        return Types.ListType.ofOptional(1, toIcebergType(type.getElementType()));
    }

    private static org.apache.iceberg.types.Type fromMap(MapType type)
    {
        return Types.MapType.ofOptional(1, 2, toIcebergType(type.getKeyType()), toIcebergType(type.getValueType()));
    }

    private static TypeInfo toHiveTypeInfo(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN.getTypeInfo();
        }
        if (BIGINT.equals(type)) {
            return HIVE_LONG.getTypeInfo();
        }
        if (INTEGER.equals(type)) {
            return HIVE_INT.getTypeInfo();
        }
        if (SMALLINT.equals(type)) {
            return HIVE_SHORT.getTypeInfo();
        }
        if (TINYINT.equals(type)) {
            return HIVE_BYTE.getTypeInfo();
        }
        if (REAL.equals(type)) {
            return HIVE_FLOAT.getTypeInfo();
        }
        if (DOUBLE.equals(type)) {
            return HIVE_DOUBLE.getTypeInfo();
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return HIVE_STRING.getTypeInfo();
            }
            if (varcharType.getLengthSafe() <= HiveVarchar.MAX_VARCHAR_LENGTH) {
                return getVarcharTypeInfo(varcharType.getLengthSafe());
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported VARCHAR types: VARCHAR(<=%d), VARCHAR.", type, HiveVarchar.MAX_VARCHAR_LENGTH));
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int charLength = charType.getLength();
            if (charLength <= HiveChar.MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported CHAR types: CHAR(<=%d).",
                    type, HiveChar.MAX_CHAR_LENGTH));
        }
        if (VARBINARY.equals(type)) {
            return HIVE_BINARY.getTypeInfo();
        }
        if (DATE.equals(type)) {
            return HIVE_DATE.getTypeInfo();
        }
        if (TIMESTAMP.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            // Hive does not have TIMESTAMP_WITH_TIME_ZONE, this is just a work around for iceberg.
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (isArrayType(type)) {
            TypeInfo elementType = toHiveTypeInfo(type.getTypeParameters().get(0));
            return getListTypeInfo(elementType);
        }
        if (isMapType(type)) {
            TypeInfo keyType = toHiveTypeInfo(type.getTypeParameters().get(0));
            TypeInfo valueType = toHiveTypeInfo(type.getTypeParameters().get(1));
            return getMapTypeInfo(keyType, valueType);
        }
        if (isRowType(type)) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
                if (!parameter.isNamedTypeSignature()) {
                    throw new IllegalArgumentException(format("Expected all parameters to be named type, but got %s", parameter));
                }
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                if (!namedTypeSignature.getName().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, format("Anonymous row type is not supported in Hive. Please give each field a name: %s", type));
                }
                fieldNames.add(namedTypeSignature.getName().get());
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    type.getTypeParameters().stream()
                            .map(TypeConverter::toHiveTypeInfo)
                            .collect(toList()));
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
    }

    public static List<OrcType> toOrcType(Schema schema)
    {
        return toOrcStructType(0, schema.asStruct(), ImmutableMap.of());
    }

    private static List<OrcType> toOrcType(int nextFieldTypeIndex, org.apache.iceberg.types.Type type, Map<String, String> attributes)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BOOLEAN, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case INTEGER:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case LONG:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FLOAT:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.FLOAT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DOUBLE:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DATE:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DATE, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case TIME:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case TIMESTAMP:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.TIMESTAMP, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case STRING:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.STRING, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case UUID:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case FIXED:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case BINARY:
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.BINARY, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), attributes));
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return ImmutableList.of(new OrcType(OrcType.OrcTypeKind.DECIMAL, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.of(decimalType.precision()), Optional.of(decimalType.scale()), attributes));
            case STRUCT:
                return toOrcStructType(nextFieldTypeIndex, (Types.StructType) type, attributes);
            case LIST:
                return toOrcListType(nextFieldTypeIndex, (Types.ListType) type, attributes);
            case MAP:
                return toOrcMapType(nextFieldTypeIndex, (Types.MapType) type, attributes);
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported Iceberg type: " + type);
        }
    }

    private static List<OrcType> toOrcStructType(int nextFieldTypeIndex, Types.StructType structType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        List<Integer> fieldTypeIndexes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<List<OrcType>> fieldTypesList = new ArrayList<>();
        for (Types.NestedField field : structType.fields()) {
            fieldTypeIndexes.add(nextFieldTypeIndex);
            fieldNames.add(field.name());
            Map<String, String> fieldAttributes = ImmutableMap.<String, String>builder()
                    .put(ORC_ICEBERG_ID_KEY, Integer.toString(field.fieldId()))
                    .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(field.isRequired()))
                    .build();
            List<OrcType> fieldOrcTypes = toOrcType(nextFieldTypeIndex, field.type(), fieldAttributes);
            fieldTypesList.add(fieldOrcTypes);
            nextFieldTypeIndex += fieldOrcTypes.size();
        }

        ImmutableList.Builder<OrcType> orcTypes = ImmutableList.builder();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.STRUCT,
                fieldTypeIndexes,
                fieldNames,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));
        fieldTypesList.forEach(orcTypes::addAll);

        return orcTypes.build();
    }

    private static List<OrcType> toOrcListType(int nextFieldTypeIndex, Types.ListType listType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        Map<String, String> elementAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(listType.elementId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(listType.isElementRequired()))
                .build();
        List<OrcType> itemTypes = toOrcType(nextFieldTypeIndex, listType.elementType(), elementAttributes);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.LIST,
                ImmutableList.of(nextFieldTypeIndex),
                ImmutableList.of("item"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));

        orcTypes.addAll(itemTypes);
        return orcTypes;
    }

    private static List<OrcType> toOrcMapType(int nextFieldTypeIndex, Types.MapType mapType, Map<String, String> attributes)
    {
        nextFieldTypeIndex++;
        Map<String, String> keyAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.keyId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(true))
                .build();
        List<OrcType> keyTypes = toOrcType(nextFieldTypeIndex, mapType.keyType(), keyAttributes);
        Map<String, String> valueAttributes = ImmutableMap.<String, String>builder()
                .put(ORC_ICEBERG_ID_KEY, Integer.toString(mapType.valueId()))
                .put(ORC_ICEBERG_REQUIRED_KEY, Boolean.toString(mapType.isValueRequired()))
                .build();
        List<OrcType> valueTypes = toOrcType(nextFieldTypeIndex + keyTypes.size(), mapType.valueType(), valueAttributes);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcType.OrcTypeKind.MAP,
                ImmutableList.of(nextFieldTypeIndex, nextFieldTypeIndex + keyTypes.size()),
                ImmutableList.of("key", "value"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                attributes));

        orcTypes.addAll(keyTypes);
        orcTypes.addAll(valueTypes);
        return orcTypes;
    }
}
