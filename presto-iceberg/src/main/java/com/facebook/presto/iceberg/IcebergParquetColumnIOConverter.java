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

import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.VariantField;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnById;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Iceberg-specific Parquet column IO converter.
 *
 * Unlike the shared {@link org.apache.parquet.io.ColumnIOConverter}, this class traverses
 * nested ROW (struct) fields by their Iceberg field ID rather than by name. This is
 * required for correct Iceberg schema evolution: when a nested field is renamed, existing
 * Parquet files still store the field under its original name, but Iceberg assigns each
 * field a stable numeric ID. Matching by ID ensures historical data is readable after a rename.
 *
 * MAP and ARRAY children are matched positionally (as in the shared converter), since
 * Iceberg does not expose renameable child identifiers for those container types in the
 * same way, and the shared converter's positional approach works correctly for them.
 *
 * The JSON (variant) branch is kept identical to the shared converter since it is
 * positional, not name-based.
 */
public final class IcebergParquetColumnIOConverter
{
    private IcebergParquetColumnIOConverter() {}

    public static Optional<Field> constructField(ColumnIdentity columnIdentity, Type type, ColumnIO columnIO)
    {
        requireNonNull(columnIdentity, "columnIdentity is null");
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();

        if (columnIO instanceof GroupColumnIO) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            String typeBase = type.getTypeSignature().getBase();

            if (ROW.equals(typeBase)) {
                List<RowType.Field> fields = ((RowType) type).getFields();
                List<ColumnIdentity> childIdentities = columnIdentity.getChildren();
                ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
                boolean structHasParameters = false;
                for (int i = 0; i < fields.size(); i++) {
                    ColumnIdentity childIdentity = childIdentities.get(i);
                    // Look up the child by Iceberg field ID, not by name — this is the key difference
                    // that makes renamed fields readable in historical Parquet files.
                    ColumnIO childColumnIO = lookupColumnById(groupColumnIO, childIdentity.getId());
                    Optional<Field> field = constructField(childIdentity, fields.get(i).getType(), childColumnIO);
                    structHasParameters |= field.isPresent();
                    fieldsBuilder.add(field);
                }
                if (structHasParameters) {
                    return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
                }
                return Optional.empty();
            }
            else if (MAP.equals(typeBase)) {
                MapType mapType = (MapType) type;
                GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
                if (keyValueColumnIO.getChildrenCount() != 2) {
                    return Optional.empty();
                }
                List<ColumnIdentity> keyValueIdentities = columnIdentity.getChildren();
                Optional<Field> keyField = constructField(keyValueIdentities.get(0), mapType.getKeyType(), keyValueColumnIO.getChild(0));
                Optional<Field> valueField = constructField(keyValueIdentities.get(1), mapType.getValueType(), keyValueColumnIO.getChild(1));
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
            }
            else if (ARRAY.equals(typeBase)) {
                List<Type> typeParams = type.getTypeParameters();
                if (groupColumnIO.getChildrenCount() != 1) {
                    return Optional.empty();
                }
                ColumnIdentity elementIdentity = columnIdentity.getChildren().get(0);
                Optional<Field> field = constructField(elementIdentity, typeParams.get(0), getArrayElementColumn(groupColumnIO.getChild(0)));
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
            }
            else if (JSON.equals(typeBase)) {
                // Iceberg variant type is stored in Presto as JSON; use positional child lookup.
                if (groupColumnIO.getChildrenCount() != 2) {
                    return Optional.empty();
                }
                Optional<Field> value = constructField(columnIdentity, VarbinaryType.VARBINARY, groupColumnIO.getChild(0));
                if (!value.isPresent()) {
                    throw new IllegalArgumentException("Value field is missing for variant type: " + type);
                }
                Optional<Field> metadata = constructField(columnIdentity, VarbinaryType.VARBINARY, groupColumnIO.getChild(1));
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
}
