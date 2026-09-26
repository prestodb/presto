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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestGeographyTypeConverter
{
    private final TypeManager typeManager = createTestFunctionAndTypeManager();

    @Test
    public void testGeographyMapsToSphericalGeography()
    {
        assertEquals(toPrestoType(Types.GeographyType.crs84(), typeManager), SPHERICAL_GEOGRAPHY);
        assertEquals(toPrestoType(Types.GeographyType.of("OGC:CRS84"), typeManager), SPHERICAL_GEOGRAPHY);
        // Iceberg normalizes the default CRS case-insensitively
        assertEquals(toPrestoType(Types.GeographyType.of("ogc:crs84"), typeManager), SPHERICAL_GEOGRAPHY);
        assertEquals(toPrestoType(Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.SPHERICAL), typeManager), SPHERICAL_GEOGRAPHY);
    }

    @Test
    public void testNestedGeographyMapsToSphericalGeography()
    {
        Type arrayType = toPrestoType(
                Types.ListType.ofOptional(2, Types.GeographyType.crs84()),
                typeManager);
        assertEquals(arrayType, new ArrayType(SPHERICAL_GEOGRAPHY));

        Type rowType = toPrestoType(
                Types.StructType.of(optional(2, "geog", Types.GeographyType.crs84())),
                typeManager);
        assertEquals(rowType, RowType.from(ImmutableList.of(RowType.field("geog", SPHERICAL_GEOGRAPHY))));
    }

    /**
     * EPSG:4326 names the same WGS84 coordinates as the default OGC:CRS84 — Iceberg
     * geography is always longitude-latitude regardless of the code's formal axis order —
     * so a column declaring it is readable rather than rejected.
     */
    @Test
    public void testEpsg4326IsAcceptedAsWgs84()
    {
        assertEquals(toPrestoType(Types.GeographyType.of("EPSG:4326"), typeManager), SPHERICAL_GEOGRAPHY);
        assertEquals(toPrestoType(Types.GeographyType.of("epsg:4326", EdgeAlgorithm.SPHERICAL), typeManager), SPHERICAL_GEOGRAPHY);
    }

    /**
     * Presto's SPHERICAL_GEOGRAPHY always means WGS84 longitude-latitude with great-circle
     * edges. Reading a column that declares anything else would silently return distances
     * and areas computed under the wrong model, so the conversion must fail instead.
     */
    @Test
    public void testNonWgs84CrsIsRejected()
    {
        assertNotSupported(
                Types.GeographyType.of("EPSG:3857"),
                "Iceberg geography type with CRS 'EPSG:3857' is not supported. Only WGS84 ('OGC:CRS84' or 'EPSG:4326') is supported");
    }

    /**
     * The write direction must produce the canonical default form: leaving the CRS and
     * algorithm unset serializes as "geography" rather than spelling out the defaults, which
     * is what other writers emit and what readers compare against.
     */
    @Test
    public void testSphericalGeographyMapsToDefaultGeography()
    {
        org.apache.iceberg.types.Type icebergType = TypeConverter.toIcebergType(SPHERICAL_GEOGRAPHY);
        assertEquals(icebergType, Types.GeographyType.crs84());
        assertEquals(icebergType.toString(), "geography");
        Types.GeographyType geographyType = (Types.GeographyType) icebergType;
        assertNull(geographyType.crs());
        assertNull(geographyType.algorithm());
    }

    @Test
    public void testNonSphericalAlgorithmIsRejected()
    {
        assertNotSupported(
                Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.KARNEY),
                "Iceberg geography type with edge interpolation algorithm 'karney' is not supported. Only 'spherical' is supported");
    }

    private void assertNotSupported(Types.GeographyType type, String expectedMessage)
    {
        try {
            toPrestoType(type, typeManager);
            fail("Expected conversion of " + type + " to fail");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), expectedMessage);
        }
    }
}
