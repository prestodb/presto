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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCPoint;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.geospatial.SphericalGeoFunctions.toSphericalGeography;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
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
            functionAssertions.getFunctionAndTypeManager().addType(type);
        }
        functionAssertions.getMetadata().registerBuiltInFunctions(extractFunctions(plugin.getFunctions()));
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
            SPHERICAL_GEOGRAPHY.writeSlice(builder, toSphericalGeography(GeoFunctions.stGeometryFromText(utf8Slice(wkt))));
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
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9))");
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9), " +
                "(-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9))");
        assertToAndFromSphericalGeography("MULTIPOLYGON (((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)), " +
                "((-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9)))");
        assertToAndFromSphericalGeography("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), " +
                "POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)))");

        // geometries containing invalid latitude or longitude values
        assertInvalidLongitude("POINT (-340.2 28.9)");
        assertInvalidLatitude("MULTIPOINT ((-40.2 128.9), (-40.2 31.9))");
        assertInvalidLongitude("LINESTRING (-40.2 28.9, -40.2 31.9, 237.2 31.9)");
        assertInvalidLatitude("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 131.9, -37.2 31.9))");
        assertInvalidLongitude("POLYGON ((-40.2 28.9, -40.2 31.9, 237.2 31.9, -37.2 28.9, -40.2 28.9))");
        assertInvalidLatitude("POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 131.9, -37.2 28.9, -40.2 28.9), (-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9))");
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
    public void testGreatCircleDistance()
    {
        assertFunction("great_circle_distance(36.12, -86.67, 33.94, -118.40)", DOUBLE, 2886.448973436703);
        assertFunction("great_circle_distance(33.94, -118.40, 36.12, -86.67)", DOUBLE, 2886.448973436703);
        assertFunction("great_circle_distance(42.3601, -71.0589, 42.4430, -71.2290)", DOUBLE, 16.73469743457461);
        assertFunction("great_circle_distance(36.12, -86.67, 36.12, -86.67)", DOUBLE, 0.0);

        assertInvalidFunction("great_circle_distance(100, 20, 30, 40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(10, 20, 300, 40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(10, 200, 30, 40)", "Longitude must be between -180 and 180");
        assertInvalidFunction("great_circle_distance(10, 20, 30, 400)", "Longitude must be between -180 and 180");

        assertInvalidFunction("great_circle_distance(nan(), -86.67, 33.94, -118.40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(infinity(), -86.67, 33.94, -118.40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(36.12, nan(), 33.94, -118.40)", "Longitude must be between -180 and 180");
        assertInvalidFunction("great_circle_distance(36.12, infinity(), 33.94, -118.40)", "Longitude must be between -180 and 180");
        assertInvalidFunction("great_circle_distance(36.12, -86.67, nan(), -118.40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(36.12, -86.67, infinity(), -118.40)", "Latitude must be between -90 and 90");
        assertInvalidFunction("great_circle_distance(36.12, -86.67, 33.94, nan())", "Longitude must be between -180 and 180");
        assertInvalidFunction("great_circle_distance(36.12, -86.67, 33.94, infinity())", "Longitude must be between -180 and 180");
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

    @Test
    public void testArea()
            throws IOException
    {
        // Empty polygon
        assertFunction("ST_Area(to_spherical_geography(ST_GeometryFromText('POLYGON EMPTY')))", DOUBLE, null);

        // Invalid data type (point)
        assertInvalidFunction("ST_Area(to_spherical_geography(ST_GeometryFromText('POINT (0 1)')))", "When applied to SphericalGeography inputs, ST_Area only supports POLYGON or MULTI_POLYGON. Input type is: POINT");

        //Invalid Polygon (duplicated point)
        assertInvalidFunction("ST_Area(to_spherical_geography(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 1 1, 1 0, 0 0))')))", "Polygon is not valid: it has two identical consecutive vertices");

        // A polygon around the North Pole
        assertArea("POLYGON((-135 85, -45 85, 45 85, 135 85, -135 85))", 619.00E9);

        assertArea("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", 123.64E8);

        assertArea("POLYGON((-122.150124 37.486095, -122.149201 37.486606,  -122.145725 37.486580, -122.145923 37.483961, -122.149324 37.482480,  -122.150837 37.483238,  -122.150901 37.485392, -122.150124 37.486095))", 163290.93943446054);

        double angleOfOneKm = 0.008993201943349;
        assertArea(format("POLYGON((0 0, %.15f 0, %.15f %.15f, 0 %.15f, 0 0))", angleOfOneKm, angleOfOneKm, angleOfOneKm, angleOfOneKm), 1E6);

        // 1/4th of an hemisphere, ie 1/8th of the planet, should be close to 4PiR2/8 = 637.58E11
        assertArea("POLYGON((90 0, 0 0, 0 90, 90 0))", 637.58E11);

        //A Polygon with a large hole
        assertArea("POLYGON((90 0, 0 0, 0 90, 90 0), (89 1, 1 1, 1 89, 89 1))", 348.04E10);

        Path geometryPath = Paths.get(TestSphericalGeoFunctions.class.getClassLoader().getResource("us-states.tsv").getPath());
        Map<String, String> stateGeometries = Files.lines(geometryPath)
                .map(line -> line.split("\t"))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));

        Path areaPath = Paths.get(TestSphericalGeoFunctions.class.getClassLoader().getResource("us-state-areas.tsv").getPath());
        Map<String, Double> stateAreas = Files.lines(areaPath)
                .map(line -> line.split("\t"))
                .filter(parts -> parts.length >= 2)
                .collect(Collectors.toMap(parts -> parts[0], parts -> Double.valueOf(parts[1])));

        for (String state : stateGeometries.keySet()) {
            assertArea(stateGeometries.get(state), stateAreas.get(state));
        }
    }

    private void assertArea(String wkt, double expectedArea)
    {
        assertFunction(format("ABS(ROUND((ST_Area(to_spherical_geography(ST_GeometryFromText('%s'))) / %f - 1 ) * %d, 0))", wkt, expectedArea, 10000), DOUBLE, 0.0);
    }

    @Test
    public void testLength()
    {
        // Empty linestring returns null
        assertLength("LINESTRING EMPTY", null);

        double length = 4350866.6362;

        // ST_Length is equivalent to sums of ST_DISTANCE between points in the LineString
        assertLength("LINESTRING (-71.05 42.36, -87.62 41.87, -122.41 37.77)", length);

        // Linestring has same length as its reverse
        assertLength("LINESTRING (-122.41 37.77, -87.62 41.87, -71.05 42.36)", length);

        // Path north pole -> south pole -> north pole should be roughly the circumference of the Earth
        assertLength("LINESTRING (0.0 90.0, 0.0 -90.0, 0.0 90.0)", 4.003e7);

        // Empty multi-linestring returns null
        assertLength("MULTILINESTRING (EMPTY)", null);

        // Multi-linestring with one path is equivalent to a single linestring
        assertLength("MULTILINESTRING ((-71.05 42.36, -87.62 41.87, -122.41 37.77))", length);

        // Multi-linestring with two disjoint paths has length equal to sum of lengths of lines
        assertLength("MULTILINESTRING ((-71.05 42.36, -87.62 41.87, -122.41 37.77), (-73.05 42.36, -89.62 41.87, -124.41 37.77))", 2 * length);

        // Multi-linestring with adjacent paths is equivalent to a single linestring
        assertLength("MULTILINESTRING ((-71.05 42.36, -87.62 41.87), (-87.62 41.87, -122.41 37.77))", length);
    }

    @Test
    public void testSTSphericalCentroid()
    {
        // Spherical centroid testing
        assertSphericalCentroid("POINT (3 5)", new Point(3, 5));
        assertSphericalCentroid("POINT EMPTY", null);
        assertSphericalCentroid("MULTIPOINT EMPTY", null);
        assertSphericalCentroid("MULTIPOINT (3 5)", new Point(3, 5));
        assertSphericalCentroid("MULTIPOINT (0 -45, 0 45)", new Point(0, 0));
        assertSphericalCentroid("MULTIPOINT (45 0, -45 0)", new Point(0, 0));
        assertSphericalCentroid("MULTIPOINT (0 0, -180 0)", new Point(-90, 45));
        assertSphericalCentroid("MULTIPOINT (0 -45, 0 45, 30 0)", new Point(12.36780515862267, 0));
        assertSphericalCentroid("MULTIPOINT (0 -45, 0 45, 30 0, -30 0)", new Point(0, 0));
    }

    private void assertSphericalCentroid(String wkt, Point centroid)
    {
        String projection = format("ST_Centroid(to_spherical_geography(ST_GeometryFromText('%s')))", wkt);
        if (centroid == null) {
            assertFunction(projection, SPHERICAL_GEOGRAPHY, null);
        }
        else {
            assertFunction(format("ST_AsText(%s)", projection), VARCHAR, new OGCPoint(centroid, null).asText());
        }
    }

    private void assertLength(String lineString, Double expectedLength)
    {
        String function = format("ST_Length(to_spherical_geography(ST_GeometryFromText('%s')))", lineString);

        if (expectedLength == null || expectedLength == 0.0) {
            assertFunction(function, DOUBLE, expectedLength);
        }
        else {
            assertFunction(format("ROUND(ABS((%s / %f) - 1.0) / %f, 0)", function, expectedLength, 1e-4), DOUBLE, 0.0);
        }
    }
}
