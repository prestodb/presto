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
package org.apache.parquet.io;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.VariantField;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.PrimitiveType;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Placed in org.apache.parquet.io package to have access to ColumnIO getRepetitionLevel() and getDefinitionLevel() methods.
 */
public class ColumnIOConverter
{
    private ColumnIOConverter()
    {
    }

    /**
     * Builds a {@link Field} tree for the requested {@code type} from the Parquet {@code columnIO}.
     * Returns {@link Optional#empty()} when the column is absent from the file.
     *
     * <p>For ROW types, if none of the requested children are present in the file this returns
     * {@link Optional#empty()}, preserving the original behaviour for connectors such as Hive
     * where an absent struct means the whole column is null.  Use
     * {@link #constructField(Type, ColumnIO, boolean)} with {@code allowEmptyStruct = true} for
     * Iceberg, where an all-absent struct must still produce a non-null struct block with null fields.
     */
    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        return constructField(type, columnIO, false);
    }

    /**
     * Like {@link #constructField(Type, ColumnIO)} but, when {@code allowEmptyStruct} is
     * {@code true}, a ROW type whose children are all absent from the file is returned as a
     * {@link GroupField} rather than {@link Optional#empty()}.
     *
     * <p>Pass {@code true} only for columns whose type contains unknown fields (i.e. when
     * {@code hasUnknownType(type)} is true): those structs need to produce a non-null struct
     * block with null fields rather than a null position.  For all other columns, pass
     * {@code false} so that an all-absent struct is correctly treated as a null position.
     */
    public static Optional<Field> constructField(Type type, ColumnIO columnIO, boolean allowEmptyStruct)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        if (columnIO instanceof GroupColumnIO) {
            if (ROW.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                List<Type> parameters = type.getTypeParameters();
                ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
                List<TypeSignatureParameter> fields = type.getTypeSignature().getParameters();
                boolean structHasParameters = false;
                for (int i = 0; i < fields.size(); i++) {
                    NamedTypeSignature namedTypeSignature = fields.get(i).getNamedTypeSignature();
                    String name = namedTypeSignature.getName().get().toLowerCase(Locale.ENGLISH);
                    Optional<Field> field = constructField(parameters.get(i), lookupColumnByName(groupColumnIO, name), allowEmptyStruct);
                    structHasParameters |= field.isPresent();
                    fieldsBuilder.add(field);
                }
                if (!allowEmptyStruct && !structHasParameters) {
                    return Optional.empty();
                }
                // When no projected child is present in the file, attach the first physical leaf
                // under the struct so that readStruct can read its definition/repetition levels.
                // Those levels reveal which struct positions are null and, for structs nested in
                // collections, how many elements the collection contains.
                Optional<PrimitiveField> levelSource = structHasParameters
                        ? Optional.empty()
                        : findFirstLeafField(groupColumnIO);
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build(), levelSource));
            }
            else if (MAP.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                MapType mapType = (MapType) type;
                GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
                if (keyValueColumnIO.getChildrenCount() != 2) {
                    return Optional.empty();
                }
                Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0), allowEmptyStruct);
                Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1), allowEmptyStruct);
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
            }
            else if (ARRAY.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                List<Type> types = type.getTypeParameters();
                if (groupColumnIO.getChildrenCount() != 1) {
                    return Optional.empty();
                }
                Optional<Field> field = constructField(types.get(0), getArrayElementColumn(groupColumnIO.getChild(0)), allowEmptyStruct);
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
            }
            else if (JSON.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                // Look up by name: the physical column order is not guaranteed
                // (Iceberg / Spark write metadata first, value second)
                ColumnIO valueColumnIO = lookupColumnByName(groupColumnIO, "value");
                ColumnIO metadataColumnIO = lookupColumnByName(groupColumnIO, "metadata");
                // Shredded Variant files (Iceberg spec §4.3) carry a third child
                // "typed_value" alongside "metadata" and "value". We do not yet
                // support reconstructing values from typed_value, so reject them
                // explicitly rather than silently returning null for every row.
                boolean hasTypedValue = lookupColumnByName(groupColumnIO, "typed_value") != null;
                if (hasTypedValue) {
                    throw new IllegalArgumentException(
                            "Shredded Variant columns are not supported. " +
                                    "Column: " + groupColumnIO.getName());
                }
                if (valueColumnIO == null) {
                    throw new IllegalArgumentException("Value field is missing for variant type: " + type);
                }
                if (metadataColumnIO == null) {
                    throw new IllegalArgumentException("Metadata field is missing for variant type: " + type);
                }
                Optional<Field> value = constructField(VarbinaryType.VARBINARY, valueColumnIO, allowEmptyStruct);
                if (!value.isPresent()) {
                    throw new IllegalArgumentException("Value field is missing for variant type: " + type);
                }
                Optional<Field> metadata = constructField(VarbinaryType.VARBINARY, metadataColumnIO, allowEmptyStruct);
                if (!metadata.isPresent()) {
                    throw new IllegalArgumentException("Metadata field is missing for variant type: " + type);
                }
                return Optional.of(new VariantField(type, repetitionLevel, definitionLevel, required, value.get(), metadata.get()));
            }
        }
        else if (columnIO instanceof PrimitiveColumnIO) {
            PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
            RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
        }
        return Optional.empty();
    }

    /**
     * Returns a {@link PrimitiveField} for the first leaf column found under {@code groupColumnIO},
     * regardless of whether that leaf is a requested projection.  The field is used solely to read
     * definition and repetition levels; its Presto type is chosen to be compatible with the
     * Parquet physical type so that the column reader initialises without error.
     */
    private static Optional<PrimitiveField> findFirstLeafField(GroupColumnIO groupColumnIO)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            ColumnIO child = groupColumnIO.getChild(i);
            if (child instanceof PrimitiveColumnIO) {
                PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) child;
                RichColumnDescriptor descriptor = new RichColumnDescriptor(
                        primitiveColumnIO.getColumnDescriptor(),
                        primitiveColumnIO.getType().asPrimitiveType());
                Type prestoType = prestoTypeForParquet(primitiveColumnIO.getType().asPrimitiveType());
                return Optional.of(new PrimitiveField(
                        prestoType,
                        columnRepetitionLevel(primitiveColumnIO),
                        columnDefinitionLevel(primitiveColumnIO),
                        primitiveColumnIO.getType().getRepetition() != OPTIONAL,
                        descriptor,
                        primitiveColumnIO.getId()));
            }
            if (child instanceof GroupColumnIO) {
                Optional<PrimitiveField> found = findFirstLeafField((GroupColumnIO) child);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    private static Type prestoTypeForParquet(PrimitiveType parquetType)
    {
        switch (parquetType.getPrimitiveTypeName()) {
            case INT32: return IntegerType.INTEGER;
            case INT64: return BigintType.BIGINT;
            case FLOAT: return RealType.REAL;
            case DOUBLE: return DoubleType.DOUBLE;
            case BOOLEAN: return BooleanType.BOOLEAN;
            case INT96: return TimestampType.TIMESTAMP;
            default: return VarbinaryType.VARBINARY;
        }
    }

    public static Optional<ColumnIO> findNestedColumnIO(ColumnIO columnIO, List<String> path)
    {
        if (columnIO == null) {
            return Optional.empty();
        }

        for (String pathElement : path) {
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                columnIO = lookupColumnByName(groupColumnIO, pathElement);
                if (columnIO == null) {
                    return Optional.empty();
                }
                continue;
            }
            throw new IllegalArgumentException("Invalid ColumnIO received. Expected a GroupColumnIO, but got " + columnIO.getClass().getSimpleName() +
                    ", path=" + Joiner.on(".").join(path));
        }

        return Optional.of(columnIO);
    }
}
