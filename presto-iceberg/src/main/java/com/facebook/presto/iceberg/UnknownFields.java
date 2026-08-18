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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.PrestoException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

/**
 * A field of type {@code unknown} is never stored in a data file: it always reads back as null.
 * Iceberg leaves such a field out of the Parquet and ORC schemas it writes, so Presto has to leave
 * it out of both the schema it writes and the values it writes.
 *
 * @see UnknownFieldTypes for the types such a file is read with
 */
final class UnknownFields
{
    private UnknownFields() {}

    static boolean isUnknownType(Type type)
    {
        return type.typeId() == Type.TypeID.UNKNOWN;
    }

    static boolean containsUnknownType(Type type)
    {
        if (isUnknownType(type)) {
            return true;
        }
        return type.isNestedType() && type.asNestedType().fields().stream()
                .anyMatch(field -> containsUnknownType(field.type()));
    }

    /**
     * The schema of the data file, which is the table schema without its {@code unknown} fields.
     */
    static Schema fileSchema(Schema tableSchema)
    {
        if (!containsUnknownType(tableSchema.asStruct())) {
            return tableSchema;
        }
        return new Schema(prunedFields(tableSchema.columns()));
    }

    /**
     * Transforms a page of the table schema into a page of the file schema, by dropping the
     * {@code unknown} fields of the row, array and map values it holds. Empty when the schema has no
     * such field, in which case a page can be written as is.
     *
     * <p>Columns that are themselves of type {@code unknown} are left alone: those are dropped a
     * whole channel at a time, by the input column indexes the writers are given.
     */
    static Optional<UnaryOperator<Page>> pagePruner(Schema tableSchema, TypeManager typeManager)
    {
        List<Types.NestedField> columns = tableSchema.columns();
        List<UnaryOperator<Block>> columnPruners = new ArrayList<>(columns.size());
        boolean anyColumnPruned = false;
        for (Types.NestedField column : columns) {
            UnaryOperator<Block> columnPruner = null;
            if (!isUnknownType(column.type()) && containsUnknownType(column.type())) {
                columnPruner = blockPruner(column.type(), typeManager);
                anyColumnPruned = true;
            }
            columnPruners.add(columnPruner);
        }

        if (!anyColumnPruned) {
            return Optional.empty();
        }

        return Optional.of(page -> {
            Block[] blocks = new Block[page.getChannelCount()];
            for (int channel = 0; channel < blocks.length; channel++) {
                Block block = page.getBlock(channel);
                UnaryOperator<Block> columnPruner = channel < columnPruners.size() ? columnPruners.get(channel) : null;
                blocks[channel] = columnPruner == null ? block : columnPruner.apply(block);
            }
            return new Page(page.getPositionCount(), blocks);
        });
    }

    /**
     * Rejects the tables that have an {@code unknown} field in a place where a data file cannot
     * leave it out. Iceberg's own writers fail on these as well.
     */
    static void validateWritable(Schema tableSchema)
    {
        for (Types.NestedField column : tableSchema.columns()) {
            validateWritable(column.type(), column.name());
        }
        if (fileSchema(tableSchema).columns().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to an Iceberg table whose columns are all of type unknown is not supported, because such a table has nothing to store in a data file");
        }
    }

    private static void validateWritable(Type type, String path)
    {
        if (!containsUnknownType(type)) {
            return;
        }

        switch (type.typeId()) {
            case STRUCT:
                List<Types.NestedField> fields = type.asStructType().fields();
                if (prunedFields(fields).isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, format("Writing to an Iceberg table with a row whose fields are all of type unknown is not supported: %s", path));
                }
                for (Types.NestedField field : fields) {
                    validateWritable(field.type(), path + "." + field.name());
                }
                return;
            case LIST:
                Types.ListType list = type.asListType();
                if (isUnknownType(list.elementType())) {
                    throw new PrestoException(NOT_SUPPORTED, format("Writing to an Iceberg table with an array of type unknown is not supported: %s", path));
                }
                validateWritable(list.elementType(), path + ".element");
                return;
            case MAP:
                Types.MapType map = type.asMapType();
                if (isUnknownType(map.keyType()) || isUnknownType(map.valueType())) {
                    throw new PrestoException(NOT_SUPPORTED, format("Writing to an Iceberg table with a map of type unknown is not supported: %s", path));
                }
                validateWritable(map.keyType(), path + ".key");
                validateWritable(map.valueType(), path + ".value");
                return;
            default:
                // a column of type unknown is dropped a whole channel at a time
        }
    }

    private static List<Types.NestedField> prunedFields(List<Types.NestedField> fields)
    {
        return fields.stream()
                .filter(field -> !isUnknownType(field.type()))
                .map(UnknownFields::prunedField)
                .collect(toImmutableList());
    }

    private static Types.NestedField prunedField(Types.NestedField field)
    {
        if (!containsUnknownType(field.type())) {
            return field;
        }
        return Types.NestedField.from(field)
                .ofType(prunedType(field.type()))
                .build();
    }

    private static Type prunedType(Type type)
    {
        switch (type.typeId()) {
            case STRUCT:
                return Types.StructType.of(prunedFields(type.asStructType().fields()));
            case LIST:
                Types.ListType list = type.asListType();
                Type elementType = prunedType(list.elementType());
                return list.isElementOptional() ?
                        Types.ListType.ofOptional(list.elementId(), elementType) :
                        Types.ListType.ofRequired(list.elementId(), elementType);
            case MAP:
                Types.MapType map = type.asMapType();
                Type keyType = prunedType(map.keyType());
                Type valueType = prunedType(map.valueType());
                return map.isValueOptional() ?
                        Types.MapType.ofOptional(map.keyId(), map.valueId(), keyType, valueType) :
                        Types.MapType.ofRequired(map.keyId(), map.valueId(), keyType, valueType);
            default:
                return type;
        }
    }

    private static UnaryOperator<Block> blockPruner(Type type, TypeManager typeManager)
    {
        switch (type.typeId()) {
            case STRUCT:
                return rowBlockPruner(type.asStructType(), typeManager);
            case LIST:
                return arrayBlockPruner(type.asListType(), typeManager);
            case MAP:
                return mapBlockPruner(type.asMapType(), typeManager);
            default:
                throw new IllegalArgumentException("Type has no unknown field to prune: " + type);
        }
    }

    private static UnaryOperator<Block> rowBlockPruner(Types.StructType struct, TypeManager typeManager)
    {
        List<Types.NestedField> fields = struct.fields();
        List<Integer> storedFields = new ArrayList<>(fields.size());
        List<UnaryOperator<Block>> fieldPruners = new ArrayList<>(fields.size());
        for (int field = 0; field < fields.size(); field++) {
            Type fieldType = fields.get(field).type();
            if (isUnknownType(fieldType)) {
                continue;
            }
            storedFields.add(field);
            fieldPruners.add(containsUnknownType(fieldType) ? blockPruner(fieldType, typeManager) : null);
        }

        return block -> {
            ColumnarRow row = toColumnarRow(block);
            Block[] fieldBlocks = new Block[storedFields.size()];
            for (int field = 0; field < fieldBlocks.length; field++) {
                Block fieldBlock = row.getField(storedFields.get(field));
                UnaryOperator<Block> fieldPruner = fieldPruners.get(field);
                fieldBlocks[field] = fieldPruner == null ? fieldBlock : fieldPruner.apply(fieldBlock);
            }
            return RowBlock.fromFieldBlocks(block.getPositionCount(), nullPositions(row.getNullCheckBlock()), fieldBlocks);
        };
    }

    private static UnaryOperator<Block> arrayBlockPruner(Types.ListType list, TypeManager typeManager)
    {
        UnaryOperator<Block> elementPruner = blockPruner(list.elementType(), typeManager);

        return block -> {
            ColumnarArray array = toColumnarArray(block);
            int positionCount = array.getPositionCount();
            int[] offsets = new int[positionCount + 1];
            for (int position = 0; position <= positionCount; position++) {
                offsets[position] = array.getOffset(position);
            }
            return ArrayBlock.fromElementBlock(
                    positionCount,
                    nullPositions(array.getNullCheckBlock()),
                    offsets,
                    elementPruner.apply(array.getElementsBlock()));
        };
    }

    private static UnaryOperator<Block> mapBlockPruner(Types.MapType map, TypeManager typeManager)
    {
        MapType prunedMapType = (MapType) toPrestoType(prunedType(map), typeManager);
        UnaryOperator<Block> keyPruner = containsUnknownType(map.keyType()) ? blockPruner(map.keyType(), typeManager) : null;
        UnaryOperator<Block> valuePruner = containsUnknownType(map.valueType()) ? blockPruner(map.valueType(), typeManager) : null;

        return block -> {
            ColumnarMap columnarMap = toColumnarMap(block);
            int positionCount = columnarMap.getPositionCount();
            int[] offsets = new int[positionCount + 1];
            for (int position = 0; position <= positionCount; position++) {
                offsets[position] = columnarMap.getOffset(position);
            }
            Block keyBlock = columnarMap.getKeysBlock();
            Block valueBlock = columnarMap.getValuesBlock();
            return prunedMapType.createBlockFromKeyValue(
                    positionCount,
                    nullPositions(columnarMap.getNullCheckBlock()),
                    offsets,
                    keyPruner == null ? keyBlock : keyPruner.apply(keyBlock),
                    valuePruner == null ? valueBlock : valuePruner.apply(valueBlock));
        };
    }

    /**
     * The null mask the block factory methods take, which is left out when nothing is null.
     */
    private static Optional<boolean[]> nullPositions(Block nullCheckBlock)
    {
        if (!nullCheckBlock.mayHaveNull()) {
            return Optional.empty();
        }

        int positionCount = nullCheckBlock.getPositionCount();
        boolean[] isNull = new boolean[positionCount];
        boolean anyNull = false;
        for (int position = 0; position < positionCount; position++) {
            isNull[position] = nullCheckBlock.isNull(position);
            anyNull |= isNull[position];
        }
        return anyNull ? Optional.of(isNull) : Optional.empty();
    }
}
