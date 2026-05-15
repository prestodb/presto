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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGooglePolylineFunctions
        extends AbstractTestQueryFramework
{
    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        // Create test table with coordinate data
        getQueryRunner().execute("CREATE TABLE IF NOT EXISTS polyline_coordinates (" +
                "id INTEGER, " +
                "route_id INTEGER, " +
                "lat DOUBLE, " +
                "lon DOUBLE, " +
                "seq INTEGER" +
                ")");

        getQueryRunner().execute("INSERT INTO polyline_coordinates VALUES " +
                "(1, 1, 38.5, -120.2, 1), " +
                "(2, 1, 40.7, -120.95, 2), " +
                "(3, 1, 43.252, -126.453, 3), " +
                "(4, 2, 37.78327, -122.43877, 1), " +
                "(5, 2, 37.75885, -122.43533, 2)");

        // Create test table with encoded polylines
        getQueryRunner().execute("CREATE TABLE IF NOT EXISTS polyline_encoded (" +
                "id INTEGER, " +
                "polyline VARCHAR" +
                ")");

        getQueryRunner().execute("INSERT INTO polyline_encoded VALUES " +
                "(1, '_p~iF~ps|U'), " +
                "(2, '_ulLnnqC'), " +
                "(3, '_mqNvxq`@')");
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
    public void testGooglePolylineEncode()
    {
        assertQuery("SELECT google_polyline_encode(ARRAY[ST_Point(38.5, -120.2)])");
    }

    @Test
    public void testGooglePolylineDecode()
    {
        assertQuery("SELECT google_polyline_decode('_p~iF~ps|U')");
    }

    @Test
    public void testGooglePolylineRoundTrip()
    {
        assertQuery("SELECT google_polyline_decode(google_polyline_encode(ARRAY[ST_Point(38.5, -120.2), ST_Point(40.7, -120.95)]))");
    }

    @Test
    public void testGooglePolylineEncodeFromTable()
    {
        assertQuery("SELECT google_polyline_encode(ARRAY[ST_Point(lat, lon)]) " +
                "FROM polyline_coordinates " +
                "WHERE id = 1");
    }

    @Test
    public void testGooglePolylineDecodeFromTable()
    {
        assertQuery("SELECT google_polyline_decode(polyline) " +
                "FROM polyline_encoded " +
                "WHERE id = 1");
    }

    @Test
    public void testGooglePolylineEncodeMultiplePoints()
    {
        assertQuery("SELECT route_id, google_polyline_encode(array_agg(ST_Point(lat, lon) ORDER BY seq)) " +
                "FROM polyline_coordinates " +
                "GROUP BY route_id " +
                "ORDER BY route_id");
    }

    @Test
    public void testGooglePolylineRoundTripFromTable()
    {
        assertQuery("SELECT route_id, " +
                "google_polyline_decode(google_polyline_encode(array_agg(ST_Point(lat, lon) ORDER BY seq))) " +
                "FROM polyline_coordinates " +
                "GROUP BY route_id " +
                "ORDER BY route_id");
    }
}
