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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

/**
 * Helpers for the Iceberg geospatial types, which Iceberg stores as well-known binary
 * while Presto's GEOMETRY and SPHERICAL_GEOGRAPHY hold their own serialization.
 */
public final class IcebergGeospatialUtils
{
    private IcebergGeospatialUtils() {}

    public static boolean isGeospatialType(Type type)
    {
        return GEOMETRY.equals(type) || SPHERICAL_GEOGRAPHY.equals(type);
    }

    public static boolean isGeospatialType(org.apache.iceberg.types.Type type)
    {
        return type.typeId() == TypeID.GEOMETRY || type.typeId() == TypeID.GEOGRAPHY;
    }

    public static boolean containsGeospatialType(Type type)
    {
        if (isGeospatialType(type)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsGeospatialType(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return containsGeospatialType(mapType.getKeyType()) || containsGeospatialType(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .anyMatch(field -> containsGeospatialType(field.getType()));
        }
        return false;
    }

    /**
     * Returns the type as the file writers see it: geospatial types become VARBINARY,
     * because the values handed to a writer are well-known binary by then and no file
     * format knows Presto's geospatial types. The Iceberg schema keeps the real type, so
     * the column reads back as GEOMETRY or SPHERICAL_GEOGRAPHY.
     */
    public static Type toFileType(Type type, TypeManager typeManager)
    {
        if (isGeospatialType(type)) {
            return VARBINARY;
        }
        if (type instanceof ArrayType) {
            return new ArrayType(toFileType(((ArrayType) type).getElementType(), typeManager));
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(
                            TypeSignatureParameter.of(toFileType(mapType.getKeyType(), typeManager).getTypeSignature()),
                            TypeSignatureParameter.of(toFileType(mapType.getValueType(), typeManager).getTypeSignature())));
        }
        if (type instanceof RowType) {
            return RowType.from(((RowType) type).getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), toFileType(field.getType(), typeManager)))
                    .collect(toImmutableList()));
        }
        return type;
    }

    /**
     * Rewrites a block of geospatial values into the well-known binary Iceberg stores,
     * recursing into arrays, maps and rows. The returned blocks hold the same structure
     * with VARBINARY leaves, matching the type the file writer was given by
     * {@link #toFileType}. Nulls are preserved.
     */
    public static Block toWellKnownBinary(Block block, Type type)
    {
        if (isGeospatialType(type)) {
            return toWellKnownBinaryLeaf(block, type);
        }
        if (type instanceof ArrayType) {
            return toWellKnownBinaryArray(block, (ArrayType) type);
        }
        if (type instanceof MapType) {
            return toWellKnownBinaryMap(block, (MapType) type);
        }
        if (type instanceof RowType) {
            return toWellKnownBinaryRow(block, (RowType) type);
        }
        return block;
    }

    private static Block toWellKnownBinaryLeaf(Block block, Type type)
    {
        block = block.getLoadedBlock();
        int positionCount = block.getPositionCount();
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            try {
                OGCGeometry geometry = EsriGeometrySerde.deserialize(type.getSlice(block, position));
                ByteBuffer wellKnownBinary = geometry.asBinary();
                byte[] bytes = new byte[wellKnownBinary.remaining()];
                wellKnownBinary.get(bytes);
                VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
            }
            catch (Exception e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format(
                        "Failed to convert %s value at position %d to well-known binary",
                        type.getDisplayName(),
                        position), e);
            }
        }
        return builder.build();
    }

    private static Block toWellKnownBinaryArray(Block block, ArrayType type)
    {
        block = block.getLoadedBlock();
        ColumnarArray columnarArray = toColumnarArray(block);
        Block elements = toWellKnownBinary(columnarArray.getElementsBlock(), type.getElementType());

        int positionCount = columnarArray.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount];
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            valueIsNull[position] = columnarArray.isNull(position);
        }
        for (int position = 0; position <= positionCount; position++) {
            offsets[position] = columnarArray.getOffset(position);
        }
        return ArrayBlock.fromElementBlock(positionCount, Optional.of(valueIsNull), offsets, elements);
    }

    private static Block toWellKnownBinaryMap(Block block, MapType type)
    {
        block = block.getLoadedBlock();
        ColumnarMap columnarMap = toColumnarMap(block);
        Block keys = toWellKnownBinary(columnarMap.getKeysBlock(), type.getKeyType());
        Block values = toWellKnownBinary(columnarMap.getValuesBlock(), type.getValueType());

        int positionCount = columnarMap.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount];
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            valueIsNull[position] = columnarMap.isNull(position);
        }
        for (int position = 0; position <= positionCount; position++) {
            offsets[position] = columnarMap.getOffset(position);
        }
        return type.createBlockFromKeyValue(positionCount, Optional.of(valueIsNull), offsets, keys, values);
    }

    private static Block toWellKnownBinaryRow(Block block, RowType type)
    {
        block = block.getLoadedBlock();
        ColumnarRow columnarRow = toColumnarRow(block);
        List<RowType.Field> fields = type.getFields();
        Block[] fieldBlocks = new Block[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldBlocks[i] = toWellKnownBinary(columnarRow.getField(i), fields.get(i).getType());
        }

        int positionCount = columnarRow.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < positionCount; position++) {
            valueIsNull[position] = columnarRow.isNull(position);
        }
        return RowBlock.fromFieldBlocks(positionCount, Optional.of(valueIsNull), fieldBlocks);
    }

    /**
     * Field ids of every geospatial column in the schema, including nested ones. Iceberg
     * gives geospatial fields bounds with geospatial meaning rather than byte comparisons,
     * so these fields are excluded from the byte-wise bounds the writers would otherwise
     * report.
     */
    public static Set<Integer> geospatialFieldIds(Schema schema)
    {
        ImmutableSet.Builder<Integer> fieldIds = ImmutableSet.builder();
        for (Types.NestedField field : schema.columns()) {
            collectGeospatialFieldIds(field, fieldIds);
        }
        return fieldIds.build();
    }

    private static void collectGeospatialFieldIds(Types.NestedField field, ImmutableSet.Builder<Integer> fieldIds)
    {
        if (isGeospatialType(field.type())) {
            fieldIds.add(field.fieldId());
            return;
        }
        if (field.type().isNestedType()) {
            for (Types.NestedField child : field.type().asNestedType().fields()) {
                collectGeospatialFieldIds(child, fieldIds);
            }
        }
    }
}
