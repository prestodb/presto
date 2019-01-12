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
package com.facebook.presto.hive.parquet.write;

import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;
import parquet.schema.Types;

import java.util.List;
import java.util.Locale;

import static parquet.schema.OriginalType.MAP_KEY_VALUE;

/**
 * This class is copied from org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter
 * and modified to test maps where MAP_KEY_VALUE is incorrectly used in place of MAP
 * Backward-compatibility rules described in spec https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
 */
public class MapKeyValuesSchemaConverter
{
    private MapKeyValuesSchemaConverter()
    {
    }

    public static MessageType convert(final List<String> columnNames, final List<TypeInfo> columnTypes)
    {
        return new MessageType("hive_schema", convertTypes(columnNames, columnTypes));
    }

    private static Type[] convertTypes(final List<String> columnNames, final List<TypeInfo> columnTypes)
    {
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalStateException("Mismatched Hive columns and types. Hive columns names" +
                    " found : " + columnNames + " . And Hive types found : " + columnTypes);
        }
        final Type[] types = new Type[columnNames.size()];
        for (int i = 0; i < columnNames.size(); ++i) {
            types[i] = convertType(columnNames.get(i), columnTypes.get(i));
        }
        return types;
    }

    private static Type convertType(final String name, final TypeInfo typeInfo)
    {
        return convertType(name, typeInfo, Repetition.OPTIONAL);
    }

    private static Type convertType(final String name, final TypeInfo typeInfo,
            final Repetition repetition)
    {
        if (typeInfo.getCategory().equals(Category.PRIMITIVE)) {
            if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8)
                        .named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.intTypeInfo) ||
                    typeInfo.equals(TypeInfoFactory.shortTypeInfo) ||
                    typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.INT32, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.INT64, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.binaryTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.INT96, repetition).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.voidTypeInfo)) {
                throw new UnsupportedOperationException("Void type not implemented");
            }
            else if (typeInfo.getTypeName().toLowerCase(Locale.ENGLISH).startsWith(
                    serdeConstants.CHAR_TYPE_NAME)) {
                return Types.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);
            }
            else if (typeInfo.getTypeName().toLowerCase(Locale.ENGLISH).startsWith(
                    serdeConstants.VARCHAR_TYPE_NAME)) {
                return Types.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);
            }
            else if (typeInfo instanceof DecimalTypeInfo) {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                int prec = decimalTypeInfo.precision();
                int scale = decimalTypeInfo.scale();
                int bytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
                return Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(bytes).as(OriginalType.DECIMAL).scale(scale).precision(prec).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
                return Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.DATE).named(name);
            }
            else if (typeInfo.equals(TypeInfoFactory.unknownTypeInfo)) {
                throw new UnsupportedOperationException("Unknown type not implemented");
            }
            else {
                throw new IllegalArgumentException("Unknown type: " + typeInfo);
            }
        }
        else if (typeInfo.getCategory().equals(Category.LIST)) {
            return convertArrayType(name, (ListTypeInfo) typeInfo);
        }
        else if (typeInfo.getCategory().equals(Category.STRUCT)) {
            return convertStructType(name, (StructTypeInfo) typeInfo);
        }
        else if (typeInfo.getCategory().equals(Category.MAP)) {
            return convertMapType(name, (MapTypeInfo) typeInfo);
        }
        else if (typeInfo.getCategory().equals(Category.UNION)) {
            throw new UnsupportedOperationException("Union type not implemented");
        }
        else {
            throw new IllegalArgumentException("Unknown type: " + typeInfo);
        }
    }

    // An optional group containing a repeated anonymous group "bag", containing
    // 1 anonymous element "array_element"
    private static GroupType convertArrayType(final String name, final ListTypeInfo typeInfo)
    {
        final TypeInfo subType = typeInfo.getListElementTypeInfo();
        return listWrapper(name, OriginalType.LIST, new GroupType(Repetition.REPEATED,
                ParquetHiveSerDe.ARRAY.toString(), convertType("array_element", subType)));
    }

    // An optional group containing multiple elements
    private static GroupType convertStructType(final String name, final StructTypeInfo typeInfo)
    {
        final List<String> columnNames = typeInfo.getAllStructFieldNames();
        final List<TypeInfo> columnTypes = typeInfo.getAllStructFieldTypeInfos();
        return new GroupType(Repetition.OPTIONAL, name, convertTypes(columnNames, columnTypes));
    }

    // An optional group containing a repeated anonymous group "map", containing
    // 2 elements: "key", "value"
    private static GroupType convertMapType(final String name, final MapTypeInfo typeInfo)
    {
        final Type keyType = convertType(ParquetHiveSerDe.MAP_KEY.toString(),
                typeInfo.getMapKeyTypeInfo(), Repetition.REQUIRED);
        final Type valueType = convertType(ParquetHiveSerDe.MAP_VALUE.toString(),
                typeInfo.getMapValueTypeInfo());
        return mapType(Repetition.OPTIONAL, name, "map", keyType, valueType);
    }

    public static GroupType mapType(Repetition repetition, String alias, String mapAlias, Type keyType, Type valueType)
    {
        //support projection only on key of a map
        if (valueType == null) {
            return listWrapper(
                    repetition,
                    alias,
                    MAP_KEY_VALUE,
                    new GroupType(
                            Repetition.REPEATED,
                            mapAlias,
                            keyType));
        }
        else {
            if (!valueType.getName().equals("value")) {
                throw new RuntimeException(valueType.getName() + " should be value");
            }
            return listWrapper(
                    repetition,
                    alias,
                    MAP_KEY_VALUE,
                    new GroupType(
                            Repetition.REPEATED,
                            mapAlias,
                            keyType,
                            valueType));
        }
    }

    private static GroupType listWrapper(Repetition repetition, String alias, OriginalType originalType, Type nested)
    {
        if (!nested.isRepetition(Repetition.REPEATED)) {
            throw new IllegalArgumentException("Nested type should be repeated: " + nested);
        }
        return new GroupType(repetition, alias, originalType, nested);
    }

    private static GroupType listWrapper(final String name, final OriginalType originalType,
            final GroupType groupType)
    {
        return new GroupType(Repetition.OPTIONAL, name, originalType, groupType);
    }
}
