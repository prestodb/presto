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
package com.facebook.presto.nativetests;

import com.facebook.presto.Session;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.nativetests.GeoSpatialTestUtils.createCoordinates;
import static com.facebook.presto.nativetests.GeoSpatialTestUtils.generateRandomTableName;
import static java.lang.String.format;

public class TestPrestoNativeGeospatial
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createCoordinates(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void setSessionNativeUseVeloxGeospatialJoin()
    {
        @Language("SQL") String query = "WITH regions(name, geom) AS ( VALUES" +
                "('A', ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'))," +
                "('B', ST_GeometryFromText('POLYGON ((5 0, 5 5, 10 5, 10 0, 5 0))')))," +
                "points(id, geom) AS ( VALUES ('P1', ST_Point(1.0, 1.0)), ('P2', ST_Point(6.0, 1.0))," +
                "('P3', ST_Point(8.0, 4.0))) SELECT p.id, r.name FROM points p LEFT JOIN regions r ON ST_Within(p.geom, r.geom)";
        // Run the query with the session property native_use_velox_geospatial_join which defaults to true.
        assertQuery(query);
        // Set the session property native_use_velox_geospatial_join to false and run again.
        Session actualSession = Session.builder(getSession())
                .setSystemProperty("native_use_velox_geospatial_join", "false")
                .build();
        assertQuery(actualSession, query);
    }

    @Test
    public void testBingTileAt()
    {
        assertQuery("SELECT bing_tile_at(lat1, lon1, zoom) FROM coordinates WHERE isvalid = true");
        assertQueryFails("SELECT bing_tile_at(lat1, lon1, zoom) FROM coordinates WHERE lat1 = 90.0 AND zoom = 1",
                "(?s).*Latitude.*outside of valid range.*-85.05112878.*85.05112878.*");
        assertQueryFails("SELECT bing_tile_at(lat1, lon1, zoom) FROM coordinates WHERE lon1 = 200.0 AND zoom = 1",
                "(?s).*Longitude.*outside of valid range.*-180.*180.*");
        assertQueryFails("SELECT bing_tile_at(lat1, lon1, zoom) FROM coordinates WHERE zoom = 24",
                "(?s).*zoom.*23.*");
        assertQueryFails("SELECT bing_tile_at(lat1, lon1, zoom) FROM coordinates WHERE zoom = -1",
                "(?s).*zoom.*negative.*");
    }

    @Test
    public void testBingTilePolygon()
    {
        String tmpTableName = generateRandomTableName(getQueryRunner());
        getQueryRunner().execute(format("CREATE TABLE %s " +
                "(x integer, y integer, z integer, isvalid boolean)", tmpTableName));
        getQueryRunner().execute(format("INSERT INTO %s VALUES " +
                "(0, 0, 1, true)," +
                "(3, 3, 3, true)," +
                "(7, 7, 4, true)," +
                "(0, 0, 5, true)," +
                "(31, 31, 5, true)," +
                "(100, 100, 10, true)," +
                "(2, 0, 1, false) ", tmpTableName));
        assertQuery(format("SELECT bing_tile_polygon(BING_TILE(x, y, z)) FROM %s WHERE isvalid = true", tmpTableName));
        assertQueryFails(format("SELECT bing_tile_polygon(BING_TILE(x, y, z)) FROM %s WHERE isvalid = false", tmpTableName),
                "(?s).*X coordinate.*greater than max coordinate.*zoom.*");
    }

    @Test
    public void testBingTilesAround()
    {
        assertQuery("SELECT cardinality(bing_tiles_around(lat1, lon1, zoom)) FROM coordinates WHERE isvalid = true");
        assertQueryFails("SELECT bing_tiles_around(lat1, lon1, zoom) FROM coordinates WHERE lat1 = 90.0 AND zoom = 2",
                "(?s).*Latitude.*outside of valid range.*-85.05112878.*85.05112878.*");
        assertQueryFails("SELECT bing_tiles_around(lat1, lon1, zoom) FROM coordinates WHERE lon1 = -200.0 AND zoom = 2",
                "(?s).*Longitude.*outside of valid range.*-180.*180.*");
        assertQueryFails("SELECT bing_tiles_around(lat1, lon1, zoom) FROM coordinates WHERE zoom = -1",
                "(?s).*zoom.*negative.*");
    }

    @Test
    public void testGreatCircleDistance()
    {
        assertQuery("SELECT great_circle_distance(lat1, lon1, lat2, lon2) FROM coordinates WHERE lat2 IS NOT NULL AND isvalid = true");
        assertQueryFails("SELECT great_circle_distance(lat1, lon1, lat2, lon2) FROM coordinates WHERE lat1 = 100.0",
                "(?s).*Latitude.*range.*-90.*90.*");
        assertQueryFails("SELECT great_circle_distance(lat1, lon1, lat2, lon2) FROM coordinates WHERE lon1 = 200.0",
                "(?s).*longitude.*range.*-180.*180.*");
    }
}
