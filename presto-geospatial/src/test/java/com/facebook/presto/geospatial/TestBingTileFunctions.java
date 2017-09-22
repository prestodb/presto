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
package com.facebook.presto.geospatial;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestBingTileFunctions
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
    public void testBingTile()
            throws Exception
    {
        assertFunction("bing_tile_quadkey(bing_tile('213'))", VARCHAR, "213");
        assertFunction("bing_tile_quadkey(bing_tile('123030123010121'))", VARCHAR, "123030123010121");

        assertFunction("bing_tile_quadkey(bing_tile(3, 5, 3))", VARCHAR, "213");
        assertFunction("bing_tile_quadkey(bing_tile(21845, 13506, 15))", VARCHAR, "123030123010121");

        // Invalid calls: corrupt quadkeys
        assertInvalidFunction("bing_tile('')", "QuadKey must not be empty string");
        assertInvalidFunction("bing_tile('test')", "Invalid QuadKey digit sequence: test");
        assertInvalidFunction("bing_tile('12345')", "Invalid QuadKey digit sequence: 12345");
        assertInvalidFunction("bing_tile('101010101010101010101010101010100101010101001010')", "QuadKey must be 23 characters or less");

        // Invalid calls: XY out of range
        assertInvalidFunction("bing_tile(10, 2, 3)", "XY coordinates for a Bing tile at zoom level 3 must be within [0, 8) range");
        assertInvalidFunction("bing_tile(2, 10, 3)", "XY coordinates for a Bing tile at zoom level 3 must be within [0, 8) range");

        // Invalid calls: zoom level out of range
        assertInvalidFunction("bing_tile(2, 7, 37)", "Zoom level must be <= 23");
    }

    @Test
    public void testPointToBingTile()
            throws Exception
    {
        assertFunction("bing_tile_quadkey(bing_tile_at(30.12, 60, 15))", VARCHAR, "123030123010121");

        // Invalid calls
        // Longitude out of range
        assertInvalidFunction("bing_tile_at(30.12, 600, 15)", "Longitude must be between -180.0 and 180.0");
        // Latitude out of range
        assertInvalidFunction("bing_tile_at(300.12, 60, 15)", "Latitude must be between -85.05112878 and 85.05112878");
        // Invalid zoom levels
        assertInvalidFunction("bing_tile_at(30.12, 60, 0)", "Zoom level must be > 0");
        assertInvalidFunction("bing_tile_at(30.12, 60, 40)", "Zoom level must be <= 23");
    }

    @Test
    public void testBingTileCoordinates()
            throws Exception
    {
        assertFunction("bing_tile_coordinates(bing_tile('213')).x", INTEGER, 3);
        assertFunction("bing_tile_coordinates(bing_tile('213')).y", INTEGER, 5);
        assertFunction("bing_tile_coordinates(bing_tile('123030123010121')).x", INTEGER, 21845);
        assertFunction("bing_tile_coordinates(bing_tile('123030123010121')).y", INTEGER, 13506);
    }

    @Test
    public void testBingTileZoomLevel()
            throws Exception
    {
        assertFunction("bing_tile_zoom_level(bing_tile('213'))", TINYINT, (byte) 3);
        assertFunction("bing_tile_zoom_level(bing_tile('123030123010121'))", TINYINT, (byte) 15);
    }

    @Test
    public void testBingTilePolygon()
            throws Exception
    {
        assertFunction("ST_AsText(bing_tile_polygon(bing_tile('123030123010121')))", VARCHAR, "POLYGON ((59.996337890625 30.12612436422458, 59.996337890625 30.11662158281937, 60.00732421875 30.11662158281937, 60.00732421875 30.12612436422458, 59.996337890625 30.12612436422458))");
        assertFunction("ST_AsText(ST_Centroid(bing_tile_polygon(bing_tile('123030123010121'))))", VARCHAR, "POINT (60.00183104425149 30.121372968273892)");
    }

    @Test
    public void testGeometryToBingTiles()
            throws Exception
    {
        assertFunction("transform(geometry_to_bing_tiles(ST_Point(60, 30.12), 10), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("1230301230"));
        assertFunction("transform(geometry_to_bing_tiles(ST_Point(60, 30.12), 15), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("123030123010121"));
        assertFunction("transform(geometry_to_bing_tiles(ST_Point(60, 30.12), 16), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("1230301230101212"));

        assertFunction("transform(geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((0 0, 0 10, 10 10, 10 0))'), 6), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("122220", "122222", "300000", "122221", "122223", "300001"));
        assertFunction("transform(geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((0 0, 0 10, 10 10))'), 6), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("122220", "122222", "300000", "122221"));

        assertFunction("transform(geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((10 10, -10 10, -20 -15, 10 10))'), 3), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("033", "211", "122"));
        assertFunction("transform(geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((10 10, -10 10, -20 -15, 10 10))'), 6), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("211102", "211120", "033321", "033323", "211101", "211103", "211121", "033330", "033332", "211110", "211112", "033331", "033333", "211111", "122220", "122222", "122221"));

        // Invalid input
        // Longitude out of range
        assertInvalidFunction("geometry_to_bing_tiles(ST_Point(600, 30.12), 10)", "Longitude span for the geometry must be in [-180.00, 180.00] range");
        assertInvalidFunction("geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((1000 10, -10 10, -20 -15))'), 10)", "Longitude span for the geometry must be in [-180.00, 180.00] range");
        // Latitude out of range
        assertInvalidFunction("geometry_to_bing_tiles(ST_Point(60, 300.12), 10)", "Latitude span for the geometry must be in [-85.05, 85.05] range");
        assertInvalidFunction("geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((10 1000, -10 10, -20 -15))'), 10)", "Latitude span for the geometry must be in [-85.05, 85.05] range");
        // Invalid zoom levels
        assertInvalidFunction("geometry_to_bing_tiles(ST_Point(60, 30.12), 0)", "Zoom level must be > 0");
        assertInvalidFunction("geometry_to_bing_tiles(ST_Point(60, 30.12), 40)", "Zoom level must be <= 23");
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("bing_tile(3, 5, 3) = bing_tile(3, 5, 3)", BOOLEAN, true);
        assertFunction("bing_tile('213') = bing_tile(3, 5, 3)", BOOLEAN, true);
        assertFunction("bing_tile('213') = bing_tile('213')", BOOLEAN, true);

        assertFunction("bing_tile(3, 5, 3) = bing_tile(3, 5, 4)", BOOLEAN, false);
        assertFunction("bing_tile('213') = bing_tile('2131')", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("bing_tile(3, 5, 3) <> bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') <> bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') <> bing_tile('213')", BOOLEAN, false);

        assertFunction("bing_tile(3, 5, 3) <> bing_tile(3, 5, 4)", BOOLEAN, true);
        assertFunction("bing_tile('213') <> bing_tile('2131')", BOOLEAN, true);
    }
}
