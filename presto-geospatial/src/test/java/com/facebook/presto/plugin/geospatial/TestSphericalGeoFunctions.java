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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSphericalGeoFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            functionAssertions.getTypeRegistry().addType(type);
        }
        functionAssertions.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));
    }

    @Test
    public void testGetObjectValue()
    {
        List<String> wktList = ImmutableList.of(
                "POINT EMPTY",
                "MULTIPOINT EMPTY",
                "LINESTRING EMPTY",
                "MULTILINESTRING EMPTY",
                "POLYGON EMPTY",
                "MULTIPOLYGON EMPTY",
                "GEOMETRYCOLLECTION EMPTY",
                "POINT (-40.2 28.9)",
                "MULTIPOINT ((-40.2 28.9), (-40.2 31.9))",
                "LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)",
                "MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))",
                "MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), ((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))",
                "GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))");

        BlockBuilder builder = SPHERICAL_GEOGRAPHY.createBlockBuilder(null, wktList.size());
        for (String wkt : wktList) {
            SPHERICAL_GEOGRAPHY.writeSlice(builder, GeoFunctions.toSphericalGeography(GeoFunctions.stGeometryFromText(utf8Slice(wkt))));
        }
        Block block = builder.build();
        for (int i = 0; i < wktList.size(); i++) {
            assertEquals(wktList.get(i), SPHERICAL_GEOGRAPHY.getObjectValue(null, block, i));
        }
    }

    @Test
    public void testToAndFromSphericalGeography()
    {
        // empty geometries
        assertToAndFromSphericalGeography("POINT EMPTY");
        assertToAndFromSphericalGeography("MULTIPOINT EMPTY");
        assertToAndFromSphericalGeography("LINESTRING EMPTY");
        assertToAndFromSphericalGeography("MULTILINESTRING EMPTY");
        assertToAndFromSphericalGeography("POLYGON EMPTY");
        assertToAndFromSphericalGeography("MULTIPOLYGON EMPTY");
        assertToAndFromSphericalGeography("GEOMETRYCOLLECTION EMPTY");

        // valid nonempty geometries
        assertToAndFromSphericalGeography("POINT (-40.2 28.9)");
        assertToAndFromSphericalGeography("MULTIPOINT ((-40.2 28.9), (-40.2 31.9))");
        assertToAndFromSphericalGeography("LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)");
        assertToAndFromSphericalGeography("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))");
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))");
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), " +
                "(-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))");
        assertToAndFromSphericalGeography("MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), " +
                "((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))");
        assertToAndFromSphericalGeography("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), " +
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))");

        // geometries containing invalid latitude or longitude values
        assertInvalidLongitude("POINT (-340.2 28.9)");
        assertInvalidLatitude("MULTIPOINT ((-40.2 128.9), (-40.2 31.9))");
        assertInvalidLongitude("LINESTRING (-40.2 28.9, -40.2 31.9, 237.2 31.9)");
        assertInvalidLatitude("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 131.9, -37.2 31.9))");
        assertInvalidLongitude("POLYGON ((-40.2 28.9, -40.2 31.9, 237.2 31.9, -37.2 28.9, -40.2 28.9))");
        assertInvalidLatitude("POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 131.9, -37.2 28.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))");
        assertInvalidLongitude("MULTIPOLYGON (((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)), " +
                "((-39.2 29.9, -39.2 30.9, 238.2 30.9, -38.2 29.9, -39.2 29.9)))");
        assertInvalidLatitude("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 131.9, -37.2 31.9), " +
                "POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)))");
    }

    private void assertToAndFromSphericalGeography(String wkt)
    {
        assertFunction(format("ST_AsText(to_geometry(to_spherical_geography(ST_GeometryFromText('%s'))))", wkt), VARCHAR, wkt);
    }

    private void assertInvalidLongitude(String wkt)
    {
        assertInvalidFunction(format("to_spherical_geography(ST_GeometryFromText('%s'))", wkt), "Longitude must be between -180 and 180");
    }

    private void assertInvalidLatitude(String wkt)
    {
        assertInvalidFunction(format("to_spherical_geography(ST_GeometryFromText('%s'))", wkt), "Latitude must be between -90 and 90");
    }

    @Test
    public void testDistance()
    {
        assertDistance("POINT (-86.67 36.12)", "POINT (-118.40 33.94)", 2886448.973436703);
        assertDistance("POINT (-118.40 33.94)", "POINT (-86.67 36.12)", 2886448.973436703);
        assertDistance("POINT (-71.0589 42.3601)", "POINT (-71.2290 42.4430)", 16734.69743457461);
        assertDistance("POINT (-86.67 36.12)", "POINT (-86.67 36.12)", 0.0);

        assertDistance("POINT EMPTY", "POINT (40 30)", null);
        assertDistance("POINT (20 10)", "POINT EMPTY", null);
        assertDistance("POINT EMPTY", "POINT EMPTY", null);
    }

    private void assertDistance(String wkt, String otherWkt, Double expectedDistance)
    {
        assertFunction(format("ST_Distance(to_spherical_geography(ST_GeometryFromText('%s')), to_spherical_geography(ST_GeometryFromText('%s')))", wkt, otherWkt), DOUBLE, expectedDistance);
    }
}
