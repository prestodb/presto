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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.TestNativeSidecarPlugin.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestGooglePolylineFunctions
        extends AbstractTestQueryFramework
{
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();

        // Create test table with coordinate data
        queryRunner.execute("DROP TABLE IF EXISTS polyline_coordinates");
        queryRunner.execute("CREATE TABLE polyline_coordinates (" +
                "id INTEGER, " +
                "route_id INTEGER, " +
                "lat DOUBLE, " +
                "lon DOUBLE, " +
                "seq INTEGER" +
                ")");
        queryRunner.execute("INSERT INTO polyline_coordinates VALUES " +
                "(1, 1, 38.5, -120.2, 1), " +
                "(2, 1, 40.7, -120.95, 2), " +
                "(3, 1, 43.252, -126.453, 3), " +
                "(4, 2, 37.78327, -122.43877, 1), " +
                "(5, 2, 37.75885, -122.43533, 2)");

        // Create test table with encoded polylines
        queryRunner.execute("DROP TABLE IF EXISTS polyline_encoded");
        queryRunner.execute("CREATE TABLE polyline_encoded (" +
                "id INTEGER, " +
                "polyline VARCHAR" +
                ")");
        queryRunner.execute("INSERT INTO polyline_encoded VALUES " +
                "(1, '_p~iF~ps|U'), " +
                "(2, '_ulLnnqC'), " +
                "(3, '_mqNvxq`@')");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
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
    public void testGooglePolylineEncode()
    {
        assertQuery("SELECT id, google_polyline_encode(ARRAY[ST_Point(lat, lon)]) " +
                "FROM polyline_coordinates " +
                "ORDER BY id");
    }

    @Test
    public void testGooglePolylineDecode()
    {
        assertQuery("SELECT id, google_polyline_decode(polyline) " +
                "FROM polyline_encoded " +
                "ORDER BY id");
    }

    @Test
    public void testGooglePolylineRoundTrip()
    {
        assertQuery("SELECT google_polyline_decode(google_polyline_encode(ARRAY[ST_Point(lat, lon)])) " +
                "FROM polyline_coordinates " +
                "WHERE route_id = 1 AND seq <= 2 " +
                "ORDER BY seq");
    }

    @Test
    public void testGooglePolylineEncodeMultiplePoints()
    {
        assertQuery("SELECT route_id, google_polyline_encode(array_agg(ST_Point(lat, lon) ORDER BY seq)) " +
                "FROM polyline_coordinates " +
                "GROUP BY route_id");
    }

    @Test
    public void testGooglePolylineRoundTripFromTable()
    {
        assertQuery("SELECT route_id, " +
                "google_polyline_decode(google_polyline_encode(array_agg(ST_Point(lat, lon) ORDER BY seq))) " +
                "FROM polyline_coordinates " +
                "GROUP BY route_id");
    }

    @Test
    public void testGooglePolylineEncodeWithNullInput()
    {
        assertQuery("SELECT google_polyline_encode(points) FROM (VALUES (CAST(NULL AS ARRAY<Geometry>))) AS t(points)");
    }

    @Test
    public void testGooglePolylineEncodeWithEmptyArray()
    {
        assertQuery("SELECT google_polyline_encode(empty_array) FROM (VALUES (CAST(ARRAY[] AS ARRAY<Geometry>))) AS t(empty_array)");
    }

    @Test
    public void testGooglePolylineEncodeWithNullGeometry()
    {
        assertQueryFails(
                "WITH test_data(geo) AS (VALUES (NULL)) " +
                "SELECT google_polyline_encode(ARRAY[CAST(geo AS Geometry)]) FROM test_data",
                ".*Invalid input.*null at index.*");
    }

    @Test
    public void testGooglePolylineEncodeWithMixedNullAndValidGeometries()
    {
        assertQueryFails(
                "WITH coords(lat1, lon1, lat2, lon2) AS (VALUES (38.5, -120.2, 40.7, -120.95)) " +
                "SELECT google_polyline_encode(ARRAY[ST_Point(lat1, lon1), CAST(NULL AS Geometry), ST_Point(lat2, lon2)]) " +
                "FROM coords",
                ".*Invalid input.*null at index.*");
    }

    @Test
    public void testGooglePolylineEncodeWithEmptyGeometry()
    {
        assertQueryFails(
                "WITH test_data(geo_text) AS (VALUES ('POINT EMPTY')) " +
                "SELECT google_polyline_encode(ARRAY[ST_GeometryFromText(geo_text)]) FROM test_data",
                ".*Empty point.*at index.*");
    }

    @Test
    public void testGooglePolylineDecodeWithNullInput()
    {
        assertQuery("SELECT google_polyline_decode(polyline) FROM (VALUES (CAST(NULL AS VARCHAR))) AS t(polyline)");
    }

    @Test
    public void testGooglePolylineDecodeWithEmptyString()
    {
        assertQuery("SELECT google_polyline_decode(input) FROM (VALUES ('')) AS t(input)");
    }

    @Test
    public void testGooglePolylineRoundTripWithEmptyArray()
    {
        assertQuery("SELECT google_polyline_decode(google_polyline_encode(empty_array)) FROM (VALUES (CAST(ARRAY[] AS ARRAY<Geometry>))) AS t(empty_array)");
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS polyline_coordinates");
        queryRunner.execute("DROP TABLE IF EXISTS polyline_encoded");
    }
}
