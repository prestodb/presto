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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.geospatial.SphericalGeographyType;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.geospatial.type.GeometryType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY_TYPE_NAME;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.iceberg.FileFormat.ORC;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergGeospatialUtils.isGeometryType;
import static com.facebook.presto.iceberg.IcebergGeospatialUtils.isSphericalGeographyType;
import static com.facebook.presto.iceberg.IcebergGeospatialUtils.transformGeometryBlock;
import static com.facebook.presto.iceberg.IcebergGeospatialUtils.validateGeospatialRead;
import static com.facebook.presto.iceberg.IcebergGeospatialUtils.validateGeospatialWrite;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestIcebergGeospatialUtils
{
    /**
     * The plugin bundles its own copy of the geospatial classes, so the types the engine
     * hands it are instances of a different class than the plugin's GEOMETRY and
     * SPHERICAL_GEOGRAPHY, and never equal to them. Subclasses stand in for those types here:
     * they share the signature but not the class, so they must be recognized by signature.
     */
    @Test
    public void testGeospatialTypesAreRecognizedBySignature()
    {
        Type geometry = new GeometryType(new TypeSignature(GEOMETRY_TYPE_NAME)) {};
        Type geography = new SphericalGeographyType(new TypeSignature(SPHERICAL_GEOGRAPHY_TYPE_NAME)) {};
        assertNotEquals(geometry, GEOMETRY);
        assertNotEquals(geography, SPHERICAL_GEOGRAPHY);

        assertTrue(isGeometryType(geometry));
        assertFalse(isSphericalGeographyType(geometry));
        assertTrue(isSphericalGeographyType(geography));
        assertFalse(isGeometryType(geography));
        assertFalse(isSphericalGeographyType(INTEGER));
        assertFalse(isSphericalGeographyType(new ArrayType(geography)));
    }

    @Test
    public void testReadAcceptsGeospatialFromParquet()
    {
        validateGeospatialRead(columns(SPHERICAL_GEOGRAPHY, new ArrayType(SPHERICAL_GEOGRAPHY), GEOMETRY), PARQUET);
        // Only geospatial columns are rejected; other columns can be read from any format
        validateGeospatialRead(columns(INTEGER, VARCHAR), ORC);
    }

    @Test
    public void testReadRejectsGeographyOutsideParquet()
    {
        assertReadFromFileFails(
                columns(INTEGER, SPHERICAL_GEOGRAPHY),
                ORC,
                "Reading Iceberg geography column 'col1' is only supported for the PARQUET file format, but the data file uses ORC");
        assertReadFromFileFails(
                columns(RowType.from(ImmutableList.of(RowType.field("inner", SPHERICAL_GEOGRAPHY)))),
                ORC,
                "Reading Iceberg geography column 'col0' is only supported for the PARQUET file format, but the data file uses ORC");
    }

    @Test
    public void testReadRejectsGeometryOutsideParquet()
    {
        assertReadFromFileFails(
                columns(INTEGER, GEOMETRY),
                ORC,
                "Reading Iceberg geometry column 'col1' is only supported for the PARQUET file format, but the data file uses ORC");
        assertReadFromFileFails(
                columns(new ArrayType(GEOMETRY)),
                ORC,
                "Reading Iceberg geometry column 'col0' is only supported for the PARQUET file format, but the data file uses ORC");
    }

    @Test
    public void testReadConvertsWellKnownBinary()
    {
        Block block = transformGeometryBlock(wellKnownBinaryBlock(SPHERICAL_GEOGRAPHY, "POINT (10 20)", null), SPHERICAL_GEOGRAPHY);
        assertEquals(block.getPositionCount(), 2);
        assertEquals(SPHERICAL_GEOGRAPHY.getSlice(block, 0), EsriGeometrySerde.serialize(OGCGeometry.fromText("POINT (10 20)")));
        assertTrue(block.isNull(1));
    }

    /**
     * Files written by other engines must be held to the same validation as
     * {@code to_spherical_geography}, since a geography Presto would never construct
     * breaks the spherical functions that assume longitude/latitude.
     */
    @Test
    public void testReadRejectsInvalidGeography()
    {
        assertReadFails("POINT (10 100)", "Invalid geography value at position 0: Latitude must be between -90 and 90");
        assertReadFails("POINT (200 10)", "Invalid geography value at position 0: Longitude must be between -180 and 180");
        assertReadFails("LINESTRING (0 0, 10 10, 181 20)", "Invalid geography value at position 0: Longitude must be between -180 and 180");
        assertReadFails("POINT Z (10 20 30)", "Invalid geography value at position 0: Cannot convert 3D geometry to a spherical geography");
    }

    @Test
    public void testReadDoesNotValidateGeometry()
    {
        Block block = transformGeometryBlock(wellKnownBinaryBlock(GEOMETRY, "POINT (200 100)"), GEOMETRY);
        assertEquals(GEOMETRY.getSlice(block, 0), EsriGeometrySerde.serialize(OGCGeometry.fromText("POINT (200 100)")));
    }

    @Test
    public void testReadRejectsMalformedWellKnownBinary()
    {
        BlockBuilder builder = SPHERICAL_GEOGRAPHY.createBlockBuilder(null, 1);
        SPHERICAL_GEOGRAPHY.writeSlice(builder, Slices.wrappedBuffer(new byte[] {1, 2, 3}));
        try {
            transformGeometryBlock(builder.build(), SPHERICAL_GEOGRAPHY);
            fail("Expected malformed well-known binary to be rejected");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), ICEBERG_BAD_DATA.toErrorCode());
            assertEquals(e.getMessage(), "Failed to parse WKB geometry at position 0");
        }
    }

    @Test
    public void testWriteAcceptsGeographyInVersion3Parquet()
    {
        validateGeospatialWrite(schema(Types.GeographyType.crs84()), 3, PARQUET);
        validateGeospatialWrite(schema(Types.StringType.get()), 2, ORC);
    }

    @Test
    public void testWriteRejectsGeographyBeforeVersion3()
    {
        assertWriteFails(
                schema(Types.GeographyType.crs84()),
                2,
                PARQUET,
                "Iceberg geography column 'geo' requires format version 3 or higher, but the table is at format version 2");
    }

    @Test
    public void testWriteRejectsGeographyOutsideParquet()
    {
        assertWriteFails(
                schema(Types.GeographyType.crs84()),
                3,
                ORC,
                "Writing to Iceberg geography column 'geo' is only supported for the PARQUET file format, but the table uses ORC");
    }

    private static void assertReadFails(String wellKnownText, String expectedMessage)
    {
        try {
            transformGeometryBlock(wellKnownBinaryBlock(SPHERICAL_GEOGRAPHY, wellKnownText), SPHERICAL_GEOGRAPHY);
            fail("Expected reading " + wellKnownText + " as a geography to fail");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), ICEBERG_BAD_DATA.toErrorCode());
            assertEquals(e.getMessage(), expectedMessage);
        }
    }

    private static void assertWriteFails(Schema schema, int formatVersion, FileFormat fileFormat, String expectedMessage)
    {
        try {
            validateGeospatialWrite(schema, formatVersion, fileFormat);
            fail("Expected the write to be rejected");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), expectedMessage);
        }
    }

    private static void assertReadFromFileFails(List<IcebergColumnHandle> columns, FileFormat fileFormat, String expectedMessage)
    {
        try {
            validateGeospatialRead(columns, fileFormat);
            fail("Expected the read to be rejected");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), expectedMessage);
        }
    }

    private static List<IcebergColumnHandle> columns(Type... types)
    {
        ImmutableList.Builder<IcebergColumnHandle> columns = ImmutableList.builder();
        for (int i = 0; i < types.length; i++) {
            columns.add(primitiveIcebergColumnHandle(i + 1, "col" + i, types[i], Optional.empty()));
        }
        return columns.build();
    }

    private static Schema schema(org.apache.iceberg.types.Type type)
    {
        return new Schema(ImmutableList.of(optional(1, "geo", type)));
    }

    /**
     * Builds a block of well-known binary as the Parquet reader produces it: the values
     * are well-known binary even though the block is typed as the geospatial type.
     */
    private static Block wellKnownBinaryBlock(Type type, String... wellKnownTexts)
    {
        BlockBuilder builder = type.createBlockBuilder(null, wellKnownTexts.length);
        for (String wellKnownText : wellKnownTexts) {
            if (wellKnownText == null) {
                builder.appendNull();
                continue;
            }
            ByteBuffer wellKnownBinary = OGCGeometry.fromText(wellKnownText).asBinary();
            byte[] bytes = new byte[wellKnownBinary.remaining()];
            wellKnownBinary.get(bytes);
            type.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
        return builder.build();
    }
}
