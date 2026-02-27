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

import com.facebook.presto.testing.QueryRunner;

import java.util.UUID;

import static java.lang.String.format;

public class GeoSpatialTestUtils
{
    private GeoSpatialTestUtils() {}

    public static String generateRandomTableName(QueryRunner queryRunner)
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        queryRunner.execute(format("DROP TABLE IF EXISTS %s", tableName));
        return tableName;
    }

    public static void createCoordinates(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS coordinates");
        queryRunner.execute("CREATE TABLE coordinates (lat1 double, lon1 double, lat2 double, lon2 double, zoom integer, isvalid boolean)");
        queryRunner.execute("INSERT INTO coordinates VALUES " +
                "(0.0, 0.0, NULL, NULL, 0, true), " +
                "(30.12, 60, NULL, NULL, 1, true), " +
                "(30.12, 60, NULL, NULL, 15, true), " +
                "(30.12, 60, NULL, NULL, 23, true), " +
                "(-85.05112878, -180, NULL, NULL, 1, true), " +
                "(-85.05112878, -180, NULL, NULL, 3, true), " +
                "(-85.05112878, -180, NULL, NULL, 15, true), " +
                "(0.0, 180.0, NULL, NULL, 2, true), " +
                "(0.0, -180.0, NULL, NULL, 2, true), " +
                "(85.0, 0.0, NULL, NULL, 3, true), " +
                "(-85.0, 0.0, NULL, NULL, 3, true), " +
                "(85.0, 180.0, NULL, NULL, 0, true), " +
                "(-85.0, -180.0, NULL, NULL, 0, true), " +
                "(45.0, 90.0, NULL, NULL, 10, true), " +
                "(-45.0, -90.0, NULL, NULL, 10, true), " +
                "(40.7128, -74.0060, NULL, NULL, 15, true), " +
                "(51.5074, -0.1278, NULL, NULL, 20, true), " +
                "(90.0, 0.0, NULL, NULL, 1, false), " +
                "(0.0, 200.0, NULL, NULL, 1, false), " +
                "(0.0, 0.0, NULL, NULL, 24, false), " +
                "(0.0, 0.0, NULL, NULL, -1, false), " +
                "(0.0, 0.0, NULL, NULL, 2, true), " +
                "(40.7128, -74.0060, NULL, NULL, 5, true), " +
                "(90.0, 0.0, NULL, NULL, 2, false), " +
                "(0.0, -200.0, NULL, NULL, 2, false), " +
                "(0.0, 0.0, NULL, NULL, -1, false), " +
                "(90.0, 0.0, -90.0, 0.0, NULL, true), " +
                "(0.0, 179.0, 0.0, -179.0, NULL, true), " +
                "(37.7749, -122.4194, 37.7750, -122.4195, NULL, true), " +
                "(0.0, 0.0, 0.0, 180.0, NULL, true), " +
                "(40.7128, -74.0060, 51.5074, -0.1278, NULL, true), " +
                "(-33.8688, 151.2093, 35.6762, 139.6503, NULL, true), " +
                "(37.7749, -122.4194, 34.0522, -118.2437, NULL, true), " +
                "(10.0, 0.0, -10.0, 0.0, NULL, true), " +
                "(45.0, -5.0, 45.0, 5.0, NULL, true), " +
                "(100.0, 0.0, 0.0, 0.0, NULL, false), " +
                "(0.0, 200.0, 0.0, 0.0, NULL, false)");
    }
}
