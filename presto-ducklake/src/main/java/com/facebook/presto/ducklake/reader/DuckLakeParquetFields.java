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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.ducklake.DuckLakeColumnIdentity;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.Type.ID;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Builds a {@link Field} tree out of a {@link ColumnIO} tree, mirroring {@code
 * org.apache.parquet.io.ColumnIOConverter#constructField(Type, ColumnIO)}. The difference is how a
 * {@code STRUCT}'s children are matched to the file's group columns: DuckLake column renames keep
 * the column id stable (spec &sect;4.4), so a struct field's Parquet {@code field_id} is matched
 * against the corresponding {@link DuckLakeColumnIdentity} child's id whenever the file's group
 * carries ids at all, falling back to name matching only for a group with no ids (a migrated
 * file). List elements and map key/value fields are still taken structurally, exactly as the
 * original does, since there is exactly one (or two) of them and no renaming is possible.
 */
final class DuckLakeParquetFields
{
    private DuckLakeParquetFields() {}

    public static Optional<Field> constructField(Type type, DuckLakeColumnIdentity identity, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);

        if (columnIO instanceof GroupColumnIO) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            switch (identity.getTypeCategory()) {
                case STRUCT:
                    return constructStructField(type, identity, repetitionLevel, definitionLevel, required, groupColumnIO);
                case MAP:
                    return constructMapField(type, identity, repetitionLevel, definitionLevel, required, groupColumnIO);
                case ARRAY:
                    return constructArrayField(type, identity, repetitionLevel, definitionLevel, required, groupColumnIO);
                default:
                    return Optional.empty();
            }
        }
        else if (columnIO instanceof PrimitiveColumnIO) {
            PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
            RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
        }
        return Optional.empty();
    }

    private static Optional<Field> constructStructField(
            Type type,
            DuckLakeColumnIdentity identity,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            GroupColumnIO groupColumnIO)
    {
        RowType rowType = (RowType) type;
        List<RowType.Field> typeFields = rowType.getFields();
        List<DuckLakeColumnIdentity> childIdentities = identity.getChildren();
        checkArgument(
                typeFields.size() == childIdentities.size(),
                "Row type %s and column identity %s disagree on the number of fields",
                type,
                identity);

        boolean matchById = groupHasIds(groupColumnIO);
        ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
        boolean structHasParameters = false;
        for (int i = 0; i < childIdentities.size(); i++) {
            DuckLakeColumnIdentity childIdentity = childIdentities.get(i);
            ColumnIO childColumnIO = matchById
                    ? lookupChildById(groupColumnIO, childIdentity.getId())
                    : lookupColumnByName(groupColumnIO, childIdentity.getName());
            Optional<Field> field = constructField(typeFields.get(i).getType(), childIdentity, childColumnIO);
            structHasParameters |= field.isPresent();
            fieldsBuilder.add(field);
        }
        if (structHasParameters) {
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
        }
        return Optional.empty();
    }

    private static Optional<Field> constructMapField(
            Type type,
            DuckLakeColumnIdentity identity,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            GroupColumnIO groupColumnIO)
    {
        MapType mapType = (MapType) type;
        GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
        if (keyValueColumnIO.getChildrenCount() != 2) {
            return Optional.empty();
        }
        List<DuckLakeColumnIdentity> children = identity.getChildren();
        Optional<Field> keyField = constructField(mapType.getKeyType(), children.get(0), keyValueColumnIO.getChild(0));
        Optional<Field> valueField = constructField(mapType.getValueType(), children.get(1), keyValueColumnIO.getChild(1));
        return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
    }

    private static Optional<Field> constructArrayField(
            Type type,
            DuckLakeColumnIdentity identity,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            GroupColumnIO groupColumnIO)
    {
        if (groupColumnIO.getChildrenCount() != 1) {
            return Optional.empty();
        }
        DuckLakeColumnIdentity elementIdentity = identity.getChildren().get(0);
        ColumnIO elementColumnIO = getArrayElementColumn(groupColumnIO.getChild(0));
        Optional<Field> elementField = constructField(((ArrayType) type).getElementType(), elementIdentity, elementColumnIO);
        return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(elementField)));
    }

    private static boolean groupHasIds(GroupColumnIO groupColumnIO)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getType().getId() != null) {
                return true;
            }
        }
        return false;
    }

    private static ColumnIO lookupChildById(GroupColumnIO groupColumnIO, long id)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            ColumnIO child = groupColumnIO.getChild(i);
            ID childId = child.getType().getId();
            if (childId != null && childId.intValue() == id) {
                return child;
            }
        }
        return null;
    }
}
