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
import io.airlift.slice.Slices;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.geospatial.GeometryUtils.getEnvelope;
import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.validateSphericalGeography;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.opsFromTable;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

/**
 * Helpers for the Iceberg geospatial types, which Iceberg stores as well-known binary
 * while Presto's GEOMETRY and SPHERICAL_GEOGRAPHY hold their own serialization.
 */
public final class IcebergGeospatialUtils
{
    private static final int MIN_FORMAT_VERSION_FOR_GEOSPATIAL = 3;

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
                // Presto validates geospatial values when they are constructed, so a value
                // that cannot be converted indicates a bug rather than bad user input
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format(
                        "Failed to convert %s value at position %d to well-known binary",
                        type.getDisplayName(),
                        position), e);
            }
        }
        return builder.build();
    }

    /**
     * Converts a block of well-known binary read from a data file into Presto's geospatial
     * serialization. The inverse of {@link #toWellKnownBinary} for a single leaf. Files may
     * be written by any engine, so SPHERICAL_GEOGRAPHY values are held to the same
     * validation Presto applies when constructing one with {@code to_spherical_geography}.
     * Nulls are preserved.
     */
    public static Block transformGeometryBlock(Block block, Type type)
    {
        block = block.getLoadedBlock();
        int positionCount = block.getPositionCount();
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            try {
                OGCGeometry geometry = OGCGeometry.fromBinary(ByteBuffer.wrap(type.getSlice(block, position).getBytes()));
                geometry.setSpatialReference(null);
                if (SPHERICAL_GEOGRAPHY.equals(type)) {
                    validateSphericalGeography(getEnvelope(geometry), geometry);
                }
                type.writeSlice(builder, EsriGeometrySerde.serialize(geometry));
            }
            catch (PrestoException e) {
                // Only the geography validation throws a PrestoException
                throw new PrestoException(ICEBERG_BAD_DATA, format("Invalid geography value at position %d: %s", position, e.getMessage()), e);
            }
            catch (Exception e) {
                throw new PrestoException(ICEBERG_BAD_DATA, format("Failed to parse WKB geometry at position %d", position), e);
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
     * Rejects a write to a table whose geospatial columns Presto cannot write, so that the
     * query fails before any data file is produced. Presto can only write geography columns,
     * which Iceberg allows only from format version 3, and only to Parquet files. Geometry
     * columns cannot be written because Presto's GEOMETRY carries no spatial reference, so
     * it cannot honour the CRS the column declares.
     */
    public static void validateGeospatialWrite(Table table)
    {
        validateGeospatialWrite(table.schema(), opsFromTable(table).current().formatVersion(), getFileFormat(table));
    }

    public static void validateGeospatialWrite(Schema schema, int formatVersion, FileFormat fileFormat)
    {
        for (Types.NestedField field : schema.columns()) {
            validateGeospatialWrite(field.name(), field.type(), formatVersion, fileFormat);
        }
    }

    public static void validateGeospatialWrite(String columnName, org.apache.iceberg.types.Type type, int formatVersion, FileFormat fileFormat)
    {
        if (type.typeId() == TypeID.GEOMETRY) {
            throw new PrestoException(NOT_SUPPORTED, format(
                    "Writing to Iceberg geometry column '%s' is not supported. Only geography columns can be written",
                    columnName));
        }
        if (type.typeId() == TypeID.GEOGRAPHY) {
            if (formatVersion < MIN_FORMAT_VERSION_FOR_GEOSPATIAL) {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "Iceberg geography column '%s' requires format version %d or higher, but the table is at format version %d",
                        columnName,
                        MIN_FORMAT_VERSION_FOR_GEOSPATIAL,
                        formatVersion));
            }
            if (fileFormat != FileFormat.PARQUET) {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "Writing to Iceberg geography column '%s' is only supported for the %s file format, but the table uses %s",
                        columnName,
                        FileFormat.PARQUET,
                        fileFormat));
            }
            return;
        }
        if (type.isNestedType()) {
            for (Types.NestedField child : type.asNestedType().fields()) {
                validateGeospatialWrite(columnName, child.type(), formatVersion, fileFormat);
            }
        }
    }
}
