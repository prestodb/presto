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
package io.prestosql.plugin.geospatial;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static io.prestosql.metadata.FunctionExtractor.extractFunctions;
import static io.prestosql.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.prestosql.plugin.geospatial.BingTile.fromCoordinates;
import static io.prestosql.plugin.geospatial.BingTileType.BING_TILE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

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
        functionAssertions.getMetadata().addFunctions(ImmutableList.of(APPLY_FUNCTION));
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();
        BingTile tile = fromCoordinates(1, 2, 3);
        String json = objectMapper.writeValueAsString(tile);
        assertEquals("{\"x\":1,\"y\":2,\"zoom\":3}", json);
        assertEquals(tile, objectMapper.readerFor(BingTile.class).readValue(json));
    }

    @Test
    public void testArrayOfBingTiles()
            throws Exception
    {
        assertFunction("array [bing_tile(1, 2, 10), bing_tile(3, 4, 11)]",
                new ArrayType(BING_TILE),
                ImmutableList.of(fromCoordinates(1, 2, 10), fromCoordinates(3, 4, 11)));
    }

    @Test
    public void testBingTile()
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
    {
        assertFunction("bing_tile_at(30.12, 60, 15)", BING_TILE, fromCoordinates(21845, 13506, 15));
        assertFunction("bing_tile_at(0, -0.002, 1)", BING_TILE, fromCoordinates(0, 1, 1));
        assertFunction("bing_tile_at(1e0/512, 0, 1)", BING_TILE, fromCoordinates(1, 0, 1));
        assertFunction("bing_tile_at(1e0/512, 0, 9)", BING_TILE, fromCoordinates(256, 255, 9));

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
    {
        assertFunction("bing_tile_coordinates(bing_tile('213')).x", INTEGER, 3);
        assertFunction("bing_tile_coordinates(bing_tile('213')).y", INTEGER, 5);
        assertFunction("bing_tile_coordinates(bing_tile('123030123010121')).x", INTEGER, 21845);
        assertFunction("bing_tile_coordinates(bing_tile('123030123010121')).y", INTEGER, 13506);

        assertCachedInstanceHasBoundedRetainedSize("bing_tile_coordinates(bing_tile('213'))");
    }

    private void assertBingTilesAroundWithRadius(
            double latitude,
            double longitude,
            int zoomLevel,
            double radius,
            String... expectedQuadKeys)
    {
        assertFunction(
                format("transform(bing_tiles_around(%s, %s, %s, %s), x -> bing_tile_quadkey(x))",
                        latitude, longitude, zoomLevel, radius),
                new ArrayType(VARCHAR),
                ImmutableList.copyOf(expectedQuadKeys));
    }

    @Test
    public void testBingTilesAroundWithRadius()
    {
        assertBingTilesAroundWithRadius(30.12, 60, 1, 1000, "1");

        assertBingTilesAroundWithRadius(30.12, 60, 15, .5,
                "123030123010120", "123030123010121", "123030123010123");

        assertBingTilesAroundWithRadius(30.12, 60, 19, .05,
                "1230301230101212120",
                "1230301230101212121",
                "1230301230101212130",
                "1230301230101212103",
                "1230301230101212123",
                "1230301230101212112",
                "1230301230101212102");
    }

    @Test
    public void testBingTilesAroundCornerWithRadius()
    {
        // Different zoom Level
        assertBingTilesAroundWithRadius(-85.05112878, -180, 1, 500,
                "3", "2");

        assertBingTilesAroundWithRadius(-85.05112878, -180, 5, 200,
                "33332",
                "33333",
                "22222",
                "22223",
                "22220",
                "22221",
                "33330",
                "33331");

        assertBingTilesAroundWithRadius(-85.05112878, -180, 15, .2,
                "333333333333332",
                "333333333333333",
                "222222222222222",
                "222222222222223",
                "222222222222220",
                "222222222222221",
                "333333333333330",
                "333333333333331");

        // Different Corners
        // Starting Corner 0,3
        assertBingTilesAroundWithRadius(-85.05112878, -180, 4, 500,
                "3323", "3332", "3333", "2222", "2223", "2232", "2220", "2221", "3330", "3331");

        assertBingTilesAroundWithRadius(-85.05112878, 180, 4, 500,
                "3323", "3332", "3333", "2222", "2223", "2232", "3331", "2221", "2220", "3330");

        assertBingTilesAroundWithRadius(85.05112878, -180, 4, 500,
                "1101", "1110", "1111", "0000", "0001", "0010", "0002", "0003", "1112", "1113");

        assertBingTilesAroundWithRadius(85.05112878, 180, 4, 500,
                "1101", "1110", "1111", "0000", "0001", "0010", "1113", "0003", "0002", "1112");
    }

    @Test
    public void testBingTilesAroundEdgeWithRadius()
    {
        // Different zoom Level
        assertBingTilesAroundWithRadius(-85.05112878, 0, 3, 300,
                "233", "322");

        assertBingTilesAroundWithRadius(-85.05112878, 0, 12, 1,
                "233333333332",
                "233333333333",
                "322222222222",
                "322222222223",
                "322222222220",
                "233333333331");

        // Different Edges
        // Starting Edge 2,3
        assertBingTilesAroundWithRadius(-85.05112878, 0, 4, 100,
                "2333", "3222");

        assertBingTilesAroundWithRadius(85.05112878, 0, 4, 100,
                "0111", "1000");

        assertBingTilesAroundWithRadius(0, 180, 4, 100,
                "3111", "2000", "1333", "0222");

        assertBingTilesAroundWithRadius(0, -180, 4, 100,
                "3111", "2000", "0222", "1333");
    }

    @Test
    public void testBingTilesWithRadiusBadInput()
    {
        // Invalid radius
        assertInvalidFunction("bing_tiles_around(30.12, 60.0, 1, -1)", "Radius must be >= 0");
        assertInvalidFunction("bing_tiles_around(30.12, 60.0, 1, 2000)",
                "Radius must be <= 1,000 km");

        // Too many tiles
        assertInvalidFunction("bing_tiles_around(30.12, 60.0, 20, 100)",
                "The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: 36699364. Radius: 100.0 km. Zoom level: 20.");
    }

    @Test
    public void testBingTilesAround()
    {
        assertFunction(
                "transform(bing_tiles_around(30.12, 60, 1), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("0", "2", "1", "3"));
        assertFunction(
                "transform(bing_tiles_around(30.12, 60, 15), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of(
                        "123030123010102",
                        "123030123010120",
                        "123030123010122",
                        "123030123010103",
                        "123030123010121",
                        "123030123010123",
                        "123030123010112",
                        "123030123010130",
                        "123030123010132"));
        assertFunction(
                "transform(bing_tiles_around(30.12, 60, 23), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of(
                        "12303012301012121210122",
                        "12303012301012121210300",
                        "12303012301012121210302",
                        "12303012301012121210123",
                        "12303012301012121210301",
                        "12303012301012121210303",
                        "12303012301012121210132",
                        "12303012301012121210310",
                        "12303012301012121210312"));
    }

    @Test
    public void testBingTilesAroundCorner()
    {
        // Different zoom Level
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, -180, 1), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("0", "2", "1", "3"));
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, -180, 3), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("220", "222", "221", "223"));
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, -180, 15), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("222222222222220", "222222222222222", "222222222222221", "222222222222223"));

        // Different Corners
        // Starting Corner 0,3
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, -180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("20", "22", "21", "23"));
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, 180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("30", "32", "31", "33"));
        assertFunction(
                "transform(bing_tiles_around(85.05112878, -180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("00", "02", "01", "03"));
        assertFunction(
                "transform(bing_tiles_around(85.05112878, 180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("10", "12", "11", "13"));
    }

    @Test
    public void testBingTilesAroundEdge()
    {
        // Different zoom Level
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, 0, 1), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("0", "2", "1", "3"));
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, 0, 3), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("231", "233", "320", "322", "321", "323"));
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, 0, 15), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of(
                        "233333333333331",
                        "233333333333333",
                        "322222222222220",
                        "322222222222222",
                        "322222222222221",
                        "322222222222223"));

        // Different Edges
        // Starting Edge 2,3
        assertFunction(
                "transform(bing_tiles_around(-85.05112878, 0, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("21", "23", "30", "32", "31", "33"));
        assertFunction(
                "transform(bing_tiles_around(85.05112878, 0, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("01", "03", "10", "12", "11", "13"));
        assertFunction(
                "transform(bing_tiles_around(0, 180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("12", "30", "32", "13", "31", "33"));
        assertFunction(
                "transform(bing_tiles_around(0, -180, 2), x -> bing_tile_quadkey(x))",
                new ArrayType(VARCHAR),
                ImmutableList.of("02", "20", "22", "03", "21", "23"));
    }

    @Test
    public void testBingTileZoomLevel()
    {
        assertFunction("bing_tile_zoom_level(bing_tile('213'))", TINYINT, (byte) 3);
        assertFunction("bing_tile_zoom_level(bing_tile('123030123010121'))", TINYINT, (byte) 15);
    }

    @Test
    public void testBingTilePolygon()
    {
        assertFunction("ST_AsText(bing_tile_polygon(bing_tile('123030123010121')))", VARCHAR, "POLYGON ((59.996337890625 30.11662158281937, 60.00732421875 30.11662158281937, 60.00732421875 30.12612436422458, 59.996337890625 30.12612436422458, 59.996337890625 30.11662158281937))");
        assertFunction("ST_AsText(ST_Centroid(bing_tile_polygon(bing_tile('123030123010121'))))", VARCHAR, "POINT (60.0018310442288 30.121372968273892)");

        // Check bottom right corner of a stack of tiles at different zoom levels
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(1, 1, 1)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (180 -85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(3, 3, 2)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (180 -85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(7, 7, 3)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (180 -85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(15, 15, 4)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (180 -85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(31, 31, 5)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (180 -85.05112877980659)");

        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 1)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(1, 1, 2)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(3, 3, 3)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(7, 7, 4)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(15, 15, 5)), g -> ST_Point(ST_XMax(g), ST_YMin(g))))", VARCHAR, "POINT (0 0)");

        // Check top left corner of a stack of tiles at different zoom levels
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(1, 1, 1)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(2, 2, 2)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(4, 4, 3)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(8, 8, 4)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (0 0)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(16, 16, 5)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (0 0)");

        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 1)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (-180 85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 2)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (-180 85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 3)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (-180 85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 4)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (-180 85.05112877980659)");
        assertFunction("ST_AsText(apply(bing_tile_polygon(bing_tile(0, 0, 5)), g -> ST_Point(ST_XMin(g), ST_YMax(g))))", VARCHAR, "POINT (-180 85.05112877980659)");
    }

    @Test
    public void testLargeGeometryToBingTiles()
            throws Exception
    {
        Path filePath = Paths.get(this.getClass().getClassLoader().getResource("large_polygon.txt").getPath());
        List<String> lines = Files.readAllLines(filePath);
        for (String line : lines) {
            String[] parts = line.split("\\|");
            String wkt = parts[0];
            int zoomLevel = Integer.parseInt(parts[1]);
            long tileCount = Long.parseLong(parts[2]);
            assertFunction("cardinality(geometry_to_bing_tiles(ST_GeometryFromText('" + wkt + "'), " + zoomLevel + "))", BIGINT, tileCount);
        }
    }

    @Test
    public void testGeometryToBingTiles()
            throws Exception
    {
        assertGeometryToBingTiles("POINT (60 30.12)", 10, ImmutableList.of("1230301230"));
        assertGeometryToBingTiles("POINT (60 30.12)", 15, ImmutableList.of("123030123010121"));
        assertGeometryToBingTiles("POINT (60 30.12)", 16, ImmutableList.of("1230301230101212"));

        assertGeometryToBingTiles("POLYGON ((0 0, 0 10, 10 10, 10 0))", 6, ImmutableList.of("122220", "122222", "122221", "122223"));
        assertGeometryToBingTiles("POLYGON ((0 0, 0 10, 10 10))", 6, ImmutableList.of("122220", "122222", "122221"));

        assertGeometryToBingTiles("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 3, ImmutableList.of("033", "211", "122"));
        assertGeometryToBingTiles("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 6, ImmutableList.of("211102", "211120", "033321", "033323", "211101", "211103", "211121", "033330", "033332", "211110", "211112", "033331", "033333", "211111", "122220", "122222", "122221"));

        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12))", 10, ImmutableList.of("1230301230"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12))", 15, ImmutableList.of("123030123010121"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12), POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122", "123"));
        assertGeometryToBingTiles("GEOMETRYCOLLECTION (POINT (60 30.12), LINESTRING (61 31, 61.01 31.01), POLYGON EMPTY)", 15, ImmutableList.of("123030123010121", "123030112310200", "123030112310202", "123030112310201"));

        assertFunction("transform(geometry_to_bing_tiles(bing_tile_polygon(bing_tile('1230301230')), 10), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("1230301230"));
        assertFunction("transform(geometry_to_bing_tiles(bing_tile_polygon(bing_tile('1230301230')), 11), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("12303012300", "12303012302", "12303012301", "12303012303"));

        assertFunction("transform(geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('LINESTRING (59.765625 29.84064389983442, 60.2 30.14512718337612)')), 10), x -> bing_tile_quadkey(x))", new ArrayType(VARCHAR), ImmutableList.of("1230301230", "1230301231"));

        // Empty geometries
        assertGeometryToBingTiles("POINT EMPTY", 10, emptyList());
        assertGeometryToBingTiles("POLYGON EMPTY", 10, emptyList());
        assertGeometryToBingTiles("GEOMETRYCOLLECTION EMPTY", 10, emptyList());

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

        // Input rectangle too large
        assertInvalidFunction("geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('LINESTRING (0 0, 80 80)')), 16)",
                "The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: 370085804. Rectangle: xMin=0.00, yMin=0.00, xMax=80.00, yMax=80.00. Zoom level: 16.");
        assertFunction("cardinality(geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('LINESTRING (0 0, 80 80)')), 5))", BIGINT, 104L);

        // Input polygon too complex
        String filePath = this.getClass().getClassLoader().getResource("too_large_polygon.txt").getPath();
        String largeWkt = Files.lines(Paths.get(filePath)).findFirst().get();
        assertInvalidFunction("geometry_to_bing_tiles(ST_GeometryFromText('" + largeWkt + "'), 16)", "The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
        assertFunction("cardinality(geometry_to_bing_tiles(ST_Envelope(ST_GeometryFromText('" + largeWkt + "')), 16))", BIGINT, 19939L);

        // Zoom level is too high
        assertInvalidFunction("geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((0 0, 0 20, 20 20, 0 0))'), 20)", "The zoom level is too high to compute a set of covering Bing tiles.");
        assertFunction("cardinality(geometry_to_bing_tiles(ST_GeometryFromText('POLYGON ((0 0, 0 20, 20 20, 0 0))'), 14))", BIGINT, 428787L);
    }

    private void assertGeometryToBingTiles(String wkt, int zoomLevel, List<String> expectedQuadKeys)
    {
        assertFunction(format("transform(geometry_to_bing_tiles(ST_GeometryFromText('%s'), %s), x -> bing_tile_quadkey(x))", wkt, zoomLevel), new ArrayType(VARCHAR), expectedQuadKeys);
    }

    @Test
    public void testEqual()
    {
        assertFunction("bing_tile(3, 5, 3) = bing_tile(3, 5, 3)", BOOLEAN, true);
        assertFunction("bing_tile('213') = bing_tile(3, 5, 3)", BOOLEAN, true);
        assertFunction("bing_tile('213') = bing_tile('213')", BOOLEAN, true);

        assertFunction("bing_tile(3, 5, 3) = bing_tile(3, 5, 4)", BOOLEAN, false);
        assertFunction("bing_tile('213') = bing_tile('2131')", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("bing_tile(3, 5, 3) <> bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') <> bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') <> bing_tile('213')", BOOLEAN, false);

        assertFunction("bing_tile(3, 5, 3) <> bing_tile(3, 5, 4)", BOOLEAN, true);
        assertFunction("bing_tile('213') <> bing_tile('2131')", BOOLEAN, true);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("null IS DISTINCT FROM null", BOOLEAN, false);
        assertFunction("bing_tile(3, 5, 3) IS DISTINCT FROM null", BOOLEAN, true);
        assertFunction("null IS DISTINCT FROM bing_tile(3, 5, 3)", BOOLEAN, true);

        assertFunction("bing_tile(3, 5, 3) IS DISTINCT FROM bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') IS DISTINCT FROM bing_tile(3, 5, 3)", BOOLEAN, false);
        assertFunction("bing_tile('213') IS DISTINCT FROM bing_tile('213')", BOOLEAN, false);

        assertFunction("bing_tile(3, 5, 3) IS DISTINCT FROM bing_tile(3, 5, 4)", BOOLEAN, true);
        assertFunction("bing_tile('213') IS DISTINCT FROM bing_tile('2131')", BOOLEAN, true);
    }
}
