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

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.PrimitiveBuilder;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergGeospatialUtils.isGeospatialType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.apache.parquet.schema.Types.primitive;

/**
 * Converts an Iceberg schema into the Parquet message type used to write a data file,
 * preserving the geospatial logical annotations that the Iceberg library cannot emit.
 *
 * <p>{@link ParquetSchemaUtil#convert(Schema, String)} throws
 * {@code UnsupportedOperationException: Unsupported type for Parquet: geography} for
 * geospatial columns, as of Iceberg 1.11.0 — its Parquet binding has no geometry or
 * geography support in either direction. To still produce a file that conforms to the
 * Iceberg specification (a {@code BYTE_ARRAY} column annotated {@code GEOGRAPHY} holding
 * well-known binary), this class converts a schema in which geospatial columns have been
 * erased to {@code binary} and then restores the annotation on the resulting leaves,
 * matched by field id. Delegating the conversion keeps Iceberg's naming and nesting
 * conventions — {@code makeCompatibleName}, three-level lists, key/value groups — rather
 * than reimplementing them.
 *
 * <p>TODO: Delete this class once the Iceberg library emits geospatial Parquet logical
 * types itself.
 */
public final class IcebergParquetSchemaUtil
{
    private IcebergParquetSchemaUtil() {}

    public static MessageType convert(Schema schema, String name)
    {
        Map<Integer, LogicalTypeAnnotation> annotations = geospatialAnnotations(schema);
        MessageType converted = ParquetSchemaUtil.convert(toBinarySchema(schema), name);
        if (annotations.isEmpty()) {
            return converted;
        }
        return new MessageType(converted.getName(), restoreAnnotations(converted.getFields(), annotations));
    }

    /**
     * Replaces every geospatial type in the schema with {@code binary}, recursing through
     * structs, lists and maps. Field ids, names, nullability and documentation are
     * preserved, so the erased schema converts to exactly the Parquet structure the real
     * schema would have produced.
     */
    public static Schema toBinarySchema(Schema schema)
    {
        return new Schema(schema.columns().stream()
                .map(IcebergParquetSchemaUtil::toBinaryField)
                .collect(toImmutableList()));
    }

    private static Types.NestedField toBinaryField(Types.NestedField field)
    {
        org.apache.iceberg.types.Type type = toBinaryType(field.type());
        if (type == field.type()) {
            return field;
        }
        return Types.NestedField.of(field.fieldId(), field.isOptional(), field.name(), type, field.doc());
    }

    private static org.apache.iceberg.types.Type toBinaryType(org.apache.iceberg.types.Type type)
    {
        if (isGeospatialType(type)) {
            return Types.BinaryType.get();
        }
        if (type instanceof Types.StructType) {
            Types.StructType structType = (Types.StructType) type;
            return Types.StructType.of(structType.fields().stream()
                    .map(IcebergParquetSchemaUtil::toBinaryField)
                    .collect(toImmutableList()));
        }
        if (type instanceof Types.ListType) {
            Types.ListType listType = (Types.ListType) type;
            org.apache.iceberg.types.Type elementType = toBinaryType(listType.elementType());
            if (elementType == listType.elementType()) {
                return type;
            }
            return listType.isElementOptional()
                    ? Types.ListType.ofOptional(listType.elementId(), elementType)
                    : Types.ListType.ofRequired(listType.elementId(), elementType);
        }
        if (type instanceof Types.MapType) {
            Types.MapType mapType = (Types.MapType) type;
            org.apache.iceberg.types.Type keyType = toBinaryType(mapType.keyType());
            org.apache.iceberg.types.Type valueType = toBinaryType(mapType.valueType());
            if (keyType == mapType.keyType() && valueType == mapType.valueType()) {
                return type;
            }
            return mapType.isValueOptional()
                    ? Types.MapType.ofOptional(mapType.keyId(), mapType.valueId(), keyType, valueType)
                    : Types.MapType.ofRequired(mapType.keyId(), mapType.valueId(), keyType, valueType);
        }
        return type;
    }

    private static Map<Integer, LogicalTypeAnnotation> geospatialAnnotations(Schema schema)
    {
        ImmutableMap.Builder<Integer, LogicalTypeAnnotation> annotations = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            collectGeospatialAnnotations(field, annotations);
        }
        return annotations.build();
    }

    private static void collectGeospatialAnnotations(Types.NestedField field, ImmutableMap.Builder<Integer, LogicalTypeAnnotation> annotations)
    {
        org.apache.iceberg.types.Type type = field.type();
        if (isGeospatialType(type)) {
            annotations.put(field.fieldId(), toLogicalTypeAnnotation(type, field.name()));
            return;
        }
        if (type.isNestedType()) {
            for (Types.NestedField child : type.asNestedType().fields()) {
                collectGeospatialAnnotations(child, annotations);
            }
        }
    }

    private static LogicalTypeAnnotation toLogicalTypeAnnotation(org.apache.iceberg.types.Type type, String fieldName)
    {
        if (type.typeId() == TypeID.GEOMETRY) {
            // Presto's GEOMETRY carries no spatial reference, so it cannot round-trip a
            // geometry column's CRS. Writes are rejected rather than silently attributing
            // the column's declared CRS to coordinates Presto never validated.
            throw new PrestoException(NOT_SUPPORTED, format(
                    "Writing to Iceberg geometry column '%s' is not supported. Only geography columns can be written",
                    fieldName));
        }
        Types.GeographyType geographyType = (Types.GeographyType) type;
        String crs = geographyType.crs();
        EdgeAlgorithm algorithm = geographyType.algorithm();
        if (crs == null && algorithm == null) {
            // Both defaults: emit the parameterless annotation so the file records nothing
            // the reader would have to compare against the defaults.
            return LogicalTypeAnnotation.geographyType();
        }
        return LogicalTypeAnnotation.geographyType(
                crs == null ? Types.GeographyType.DEFAULT_CRS : crs,
                algorithm == null ? LogicalTypeAnnotation.DEFAULT_ALGO : EdgeInterpolationAlgorithm.valueOf(algorithm.name()));
    }

    private static List<Type> restoreAnnotations(List<Type> fields, Map<Integer, LogicalTypeAnnotation> annotations)
    {
        ImmutableList.Builder<Type> restored = ImmutableList.builder();
        for (Type field : fields) {
            restored.add(restoreAnnotations(field, annotations));
        }
        return restored.build();
    }

    private static Type restoreAnnotations(Type field, Map<Integer, LogicalTypeAnnotation> annotations)
    {
        if (field.isPrimitive()) {
            LogicalTypeAnnotation annotation = field.getId() == null ? null : annotations.get(field.getId().intValue());
            if (annotation == null) {
                return field;
            }
            PrimitiveType primitiveType = field.asPrimitiveType();
            PrimitiveBuilder<PrimitiveType> builder = primitive(primitiveType.getPrimitiveTypeName(), primitiveType.getRepetition())
                    .as(annotation)
                    .id(field.getId().intValue());
            return builder.named(primitiveType.getName());
        }
        GroupType groupType = field.asGroupType();
        return groupType.withNewFields(restoreAnnotations(groupType.getFields(), annotations));
    }
}
