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

    public static void createGeometries(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS geometries");
        queryRunner.execute("CREATE TABLE geometries (gid integer, wkt varchar)");
        queryRunner.execute("INSERT INTO geometries VALUES " +
                // Three overlapping triangles: union and convex hull are single polygons.
                "(1, 'POLYGON ((2 2, 3 1, 1 1, 2 2))'), " +
                "(1, 'POLYGON ((3 2, 4 1, 2 1, 3 2))'), " +
                "(1, 'POLYGON ((4 2, 5 1, 3 1, 4 2))'), " +
                // Two disjoint triangles: union is a multipolygon, convex hull is a polygon.
                "(2, 'POLYGON ((1 1, 3 1, 2 2, 1 1))'), " +
                "(2, 'POLYGON ((4 1, 6 1, 5 2, 4 1))'), " +
                // Points at the corners of a unit square: union is a multipoint, convex hull is a polygon.
                "(3, 'POINT (0 0)'), " +
                "(3, 'POINT (1 0)'), " +
                "(3, 'POINT (1 1)'), " +
                "(3, 'POINT (0 1)'), " +
                // Collinear points: union is a multipoint, convex hull collapses to a linestring.
                "(4, 'POINT (1 1)'), " +
                "(4, 'POINT (2 2)'), " +
                "(4, 'POINT (3 3)'), " +
                // Two overlapping rectangles forming a cross.
                "(5, 'POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))'), " +
                "(5, 'POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))'), " +
                // Linestrings: union is a multilinestring, convex hull is a polygon.
                "(6, 'LINESTRING (0 0, 1 1)'), " +
                "(6, 'LINESTRING (1 0, 2 1)'), " +
                // Null mixed with a value: nulls are skipped by the aggregate.
                "(7, NULL), " +
                "(7, 'POINT (5 5)'), " +
                // Single geometry: union and convex hull return the input unchanged.
                "(8, 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'), " +
                // Multipoints on the line y = 2x.
                "(9, 'MULTIPOINT ((1 2), (2 4), (3 6))'), " +
                "(9, 'MULTIPOINT ((4 8), (5 10))'), " +
                // Empty geometry absorbed by a non-empty one: result is the non-empty geometry.
                "(10, 'LINESTRING EMPTY'), " +
                "(10, 'LINESTRING (1 1, 3 3)'), " +
                // Only-empty geometry: result is empty, so ST_Area is 0 and the envelope is null.
                "(11, 'POLYGON EMPTY')");
    }

    public static void createCoordinates(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS coordinates");
        queryRunner.execute("CREATE TABLE coordinates (lat1 double, lon1 double, lat2 double, lon2 double, zoom integer, radiusinkm double, isvalid boolean)");
        queryRunner.execute("INSERT INTO coordinates VALUES " +
                "(0.0, 0.0, NULL, NULL, 0, NULL, true), " +
                "(30.12, 60, NULL, NULL, 1, 0, true), " +
                "(30.12, 60, NULL, NULL, 15, 0, true), " +
                "(30.12, 60, NULL, NULL, 23, 0, true), " +
                "(-85.05112878, -180, NULL, NULL, 1, 0, true), " +
                "(-85.05112878, -180, NULL, NULL, 3, 0, true), " +
                "(-85.05112878, -180, NULL, NULL, 15, 0, true), " +
                "(0.0, 180.0, NULL, NULL, 2, 0.25, true), " +
                "(0.0, -180.0, NULL, NULL, 2, 0.25, true), " +
                "(85.0, 0.0, NULL, NULL, 3, 1, true), " +
                "(-85.0, 0.0, NULL, NULL, 3, 2, true), " +
                "(85.0, 180.0, NULL, NULL, 0, 0.25, true), " +
                "(-85.0, -180.0, NULL, NULL, 0, 1, true), " +
                "(45.0, 90.0, NULL, NULL, 10, 2, true), " +
                "(-45.0, -90.0, NULL, NULL, 10, 10, true), " +
                "(40.7128, -74.0060, NULL, NULL, 15, 2, true), " +
                "(51.5074, -0.1278, NULL, NULL, 20, 0, true), " +
                "(90.0, 0.0, NULL, NULL, 1, 0, false), " +
                "(0.0, 200.0, NULL, NULL, 1, 0, false), " +
                "(0.0, 0.0, NULL, NULL, 24, 0.25, false), " +
                "(0.0, 0.0, NULL, NULL, -1, 250, false), " +
                "(0.0, 0.0, NULL, NULL, 2, 12, true), " +
                "(40.7128, -74.0060, NULL, NULL, 5, 2, true), " +
                "(90.0, 0.0, NULL, NULL, 2, 1, false), " +
                "(0.0, -200.0, NULL, NULL, 2, 0, false), " +
                "(0.0, 0.0, NULL, NULL, -1, 0, false), " +
                "(90.0, 0.0, -90.0, 0.0, NULL, 0, true), " +
                "(0.0, 179.0, 0.0, -179.0, NULL, 0, true), " +
                "(37.7749, -122.4194, 37.7750, -122.4195, NULL, 0, true), " +
                "(0.0, 0.0, 0.0, 180.0, NULL, 0, true), " +
                "(40.7128, -74.0060, 51.5074, -0.1278, NULL, 0, true), " +
                "(-33.8688, 151.2093, 35.6762, 139.6503, NULL, 0, true), " +
                "(37.7749, -122.4194, 34.0522, -118.2437, NULL, 0, true), " +
                "(10.0, 0.0, -10.0, 0.0, NULL, 0, true), " +
                "(45.0, -5.0, 45.0, 5.0, NULL, 0, true), " +
                "(100.0, 0.0, 0.0, 0.0, NULL, 0, false), " +
                "(0.0, 200.0, 0.0, 0.0, NULL, 0, false)");
    }
}
