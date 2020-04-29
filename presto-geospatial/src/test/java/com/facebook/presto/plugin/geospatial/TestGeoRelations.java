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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class TestGeoRelations
        extends AbstractTestFunctions
{
    // A set of geometries such that:
    // 0, 1: Within (1, 0: Contains)
    // 0, 2: Touches
    // 1, 2: Overlaps
    // 0, 3: Touches
    // 1, 3: Crosses
    // 1, 4: Touches
    // 1, 5: Touches
    // 2, 3: Contains
    // 2, 4: Crosses
    // 2, 5: Crosses
    // 3, 4: Crosses
    // 3, 5: Touches
    // 4, 5: Contains
    // 1, 6: Contains
    // 2, 6: Contains
    // 1, 7: Touches
    // 2, 7: Contains
    // 3, 6: Contains
    // 3, 7: Contains
    // 4, 7: Contains
    // 5, 7: Touches
    public static final List<String> RELATION_GEOMETRIES_WKT = ImmutableList.of(
            "'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'", // 0
            "'POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'", // 1
            "'POLYGON ((1 0, 1 1, 3 1, 3 0, 1 0))'", // 2
            "'LINESTRING (1 0.5, 2.5 0.5)'", // 3
            "'LINESTRING (2 0, 2 2)'", // 4
            "'LINESTRING (2 0.5, 2 2)'", // 5
            "'POINT (1.5 0.5)'", // 6
            "'POINT (2 0.5)'"); // 7

    public static final List<Pair<Integer, Integer>> EQUALS_PAIRS = ImmutableList.of(
            Pair.of(0, 0),
            Pair.of(1, 1),
            Pair.of(2, 2),
            Pair.of(3, 3),
            Pair.of(4, 4),
            Pair.of(5, 5),
            Pair.of(6, 6),
            Pair.of(7, 7));

    public static final List<Pair<Integer, Integer>> CONTAINS_PAIRS = ImmutableList.of(
            Pair.of(1, 0),
            Pair.of(2, 3),
            Pair.of(4, 5),
            Pair.of(1, 6),
            Pair.of(2, 6),
            Pair.of(2, 7),
            Pair.of(3, 6),
            Pair.of(3, 7),
            Pair.of(4, 7));

    public static final List<Pair<Integer, Integer>> TOUCHES_PAIRS = ImmutableList.of(
            Pair.of(0, 2),
            Pair.of(0, 3),
            Pair.of(1, 4),
            Pair.of(1, 5),
            Pair.of(3, 5),
            Pair.of(1, 7),
            Pair.of(5, 7));

    public static final List<Pair<Integer, Integer>> OVERLAPS_PAIRS = ImmutableList.of(
            Pair.of(1, 2));

    public static final List<Pair<Integer, Integer>> CROSSES_PAIRS = ImmutableList.of(
            Pair.of(1, 3),
            Pair.of(2, 4),
            Pair.of(2, 5),
            Pair.of(3, 4));

    @BeforeClass
    protected void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        registerTypes(plugin);
        registerFunctions(plugin);
    }

    @Test
    public void testStContains()
    {
        assertRelation("ST_Contains", "null", "'POINT (25 25)'", null);
        assertRelation("ST_Contains", "'POINT (20 20)'", "'POINT (25 25)'", false);
        assertRelation("ST_Contains", "'MULTIPOINT (20 20, 25 25)'", "'POINT (25 25)'", true);
        assertRelation("ST_Contains", "'LINESTRING (20 20, 30 30)'", "'POINT (25 25)'", true);
        assertRelation("ST_Contains", "'LINESTRING (20 20, 30 30)'", "'MULTIPOINT (25 25, 31 31)'", false);
        assertRelation("ST_Contains", "'LINESTRING (20 20, 30 30)'", "'LINESTRING (25 25, 27 27)'", true);
        assertRelation("ST_Contains", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))'", false);
        assertRelation("ST_Contains", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", "'POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))'", true);
        assertRelation("ST_Contains", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", "'POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))'", false);
        assertRelation("ST_Contains", "'MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))'", "'POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))'", true);
        assertRelation("ST_Contains", "'LINESTRING (20 20, 30 30)'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", false);
        assertRelation("ST_Contains", "'LINESTRING EMPTY'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", false);
        assertRelation("ST_Contains", "'LINESTRING (20 20, 30 30)'", "'POLYGON EMPTY'", false);
    }

    @Test
    public void testSTCrosses()
    {
        assertRelation("ST_Crosses", "'POINT (20 20)'", "'POINT (25 25)'", false);
        assertRelation("ST_Crosses", "'LINESTRING (20 20, 30 30)'", "'POINT (25 25)'", false);
        assertRelation("ST_Crosses", "'LINESTRING (20 20, 30 30)'", "'MULTIPOINT (25 25, 31 31)'", true);
        assertRelation("ST_Crosses", "'LINESTRING(0 0, 1 1)'", "'LINESTRING (1 0, 0 1)'", true);
        assertRelation("ST_Crosses", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", "'POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))'", false);
        assertRelation("ST_Crosses", "'MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))'", "'POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))'", false);
        assertRelation("ST_Crosses", "'LINESTRING (-2 -2, 6 6)'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", true);
        assertRelation("ST_Crosses", "'POINT (20 20)'", "'POINT (20 20)'", false);
        assertRelation("ST_Crosses", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", false);
        assertRelation("ST_Crosses", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", "'LINESTRING (0 0, 0 4, 4 4, 4 0)'", false);
    }

    @Test
    public void testSTDisjoint()
    {
        assertRelation("ST_Disjoint", "'POINT (50 100)'", "'POINT (150 150)'", true);
        assertRelation("ST_Disjoint", "'MULTIPOINT (50 100, 50 200)'", "'POINT (50 100)'", false);
        assertRelation("ST_Disjoint", "'LINESTRING (0 0, 0 1)'", "'LINESTRING (1 1, 1 0)'", true);
        assertRelation("ST_Disjoint", "'LINESTRING (2 1, 1 2)'", "'LINESTRING (3 1, 1 3)'", true);
        assertRelation("ST_Disjoint", "'LINESTRING (1 1, 3 3)'", "'LINESTRING (3 1, 1 3)'", false);
        assertRelation("ST_Disjoint", "'LINESTRING (50 100, 50 200)'", "'LINESTRING (20 150, 100 150)'", false);
        assertRelation("ST_Disjoint", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", false);
        assertRelation("ST_Disjoint", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))'", true);
        assertRelation("ST_Disjoint", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", false);
    }

    @Test
    public void testSTEquals()
    {
        assertRelation("ST_Equals", "'POINT (50 100)'", "'POINT (150 150)'", false);
        assertRelation("ST_Equals", "'MULTIPOINT (50 100, 50 200)'", "'POINT (50 100)'", false);
        assertRelation("ST_Equals", "'LINESTRING (0 0, 0 1)'", "'LINESTRING (1 1, 1 0)'", false);
        assertRelation("ST_Equals", "'LINESTRING (0 0, 2 2)'", "'LINESTRING (0 0, 2 2)'", true);
        assertRelation("ST_Equals", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", false);
        assertRelation("ST_Equals", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((3 3, 3 1, 1 1, 1 3, 3 3))'", true);
        assertRelation("ST_Equals", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", false);
    }

    @Test
    public void testSTIntersects()
    {
        assertRelation("ST_Intersects", "'POINT (50 100)'", "'POINT (150 150)'", false);
        assertRelation("ST_Intersects", "'MULTIPOINT (50 100, 50 200)'", "'POINT (50 100)'", true);
        assertRelation("ST_Intersects", "'LINESTRING (0 0, 0 1)'", "'LINESTRING (1 1, 1 0)'", false);
        assertRelation("ST_Intersects", "'LINESTRING (50 100, 50 200)'", "'LINESTRING (20 150, 100 150)'", true);
        assertRelation("ST_Intersects", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", true);
        assertRelation("ST_Intersects", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))'", false);
        assertRelation("ST_Intersects", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", true);
        assertRelation("ST_Intersects", "'POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))'", "'LINESTRING (16.6 53, 16.6 56)'", true);
        assertRelation("ST_Intersects", "'POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))'", "'LINESTRING (16.6667 54.05, 16.8667 54.05)'", false);
        assertRelation("ST_Intersects", "'POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))'", "'LINESTRING (16.6667 54.25, 16.8667 54.25)'", false);
    }

    @Test
    public void testSTOverlaps()
    {
        assertRelation("ST_Overlaps", "'POINT (50 100)'", "'POINT (150 150)'", false);
        assertRelation("ST_Overlaps", "'POINT (50 100)'", "'POINT (50 100)'", false);
        assertRelation("ST_Overlaps", "'MULTIPOINT (50 100, 50 200)'", "'POINT (50 100)'", false);
        assertRelation("ST_Overlaps", "'LINESTRING (0 0, 0 1)'", "'LINESTRING (1 1, 1 0)'", false);
        assertRelation("ST_Overlaps", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", true);
        assertRelation("ST_Overlaps", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", "'POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))'", true);
        assertRelation("ST_Overlaps", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", false);
        assertRelation("ST_Overlaps", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", "'LINESTRING (1 1, 4 4)'", false);
        assertRelation("ST_Overlaps", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))'", false);
        assertRelation("ST_Overlaps", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", true);
    }

    @Test
    public void testSTRelate()
    {
        assertFunction("ST_Relate(ST_GeometryFromText('LINESTRING (0 0, 3 3)'), ST_GeometryFromText('LINESTRING (1 1, 4 1)'), '****T****')", BOOLEAN, false);
        assertFunction("ST_Relate(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'), '****T****')", BOOLEAN, true);
        assertFunction("ST_Relate(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1, 2 0))'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'), 'T********')", BOOLEAN, false);
    }

    @Test
    public void testSTTouches()
    {
        assertRelation("ST_Touches", "'POINT (50 100)'", "'POINT (150 150)'", false);
        assertRelation("ST_Touches", "'MULTIPOINT (50 100, 50 200)'", "'POINT (50 100)'", false);
        assertRelation("ST_Touches", "'LINESTRING (50 100, 50 200)'", "'LINESTRING (20 150, 100 150)'", false);
        assertRelation("ST_Touches", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", false);
        assertRelation("ST_Touches", "'POINT (1 2)'", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", true);
        assertRelation("ST_Touches", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))'", false);
        assertRelation("ST_Touches", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'LINESTRING (0 0, 1 1)'", true);
        assertRelation("ST_Touches", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))'", true);
        assertRelation("ST_Touches", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", false);
    }

    @Test
    public void testSTWithin()
    {
        assertRelation("ST_Within", "'POINT (50 100)'", "'POINT (150 150)'", false);
        assertRelation("ST_Within", "'POINT (50 100)'", "'MULTIPOINT (50 100, 50 200)'", true);
        assertRelation("ST_Within", "'LINESTRING (50 100, 50 200)'", "'LINESTRING (50 50, 50 250)'", true);
        assertRelation("ST_Within", "'MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'", "'MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'", false);
        assertRelation("ST_Within", "'POINT (3 2)'", "'POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'", true);
        assertRelation("ST_Within", "'POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", true);
        assertRelation("ST_Within", "'LINESTRING (1 1, 3 3)'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", true);
        assertRelation("ST_Within", "'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))'", "'POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))'", false);
        assertRelation("ST_Within", "'POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))'", "'POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'", false);
    }

    @Test
    public void testContainsWithin()
    {
        for (int i = 0; i < RELATION_GEOMETRIES_WKT.size(); i++) {
            for (int j = 0; j < RELATION_GEOMETRIES_WKT.size(); j++) {
                boolean ok = (i == j) || CONTAINS_PAIRS.contains(Pair.of(i, j));
                assertRelation("ST_Contains", RELATION_GEOMETRIES_WKT.get(i), RELATION_GEOMETRIES_WKT.get(j), ok);
                // Within is just the inverse of contain
                assertRelation("ST_Within", RELATION_GEOMETRIES_WKT.get(j), RELATION_GEOMETRIES_WKT.get(i), ok);
            }
        }
    }

    @Test
    public void testEquals()
    {
        testSymmetricRelations("ST_Equals", EQUALS_PAIRS);
    }

    @Test
    public void testTouches()
    {
        testSymmetricRelations("ST_Touches", TOUCHES_PAIRS);
    }

    @Test
    public void testOverlaps()
    {
        testSymmetricRelations("ST_Overlaps", OVERLAPS_PAIRS);
    }

    @Test
    public void testCrosses()
    {
        testSymmetricRelations("ST_Crosses", CROSSES_PAIRS);
    }

    private void testSymmetricRelations(String relation, List<Pair<Integer, Integer>> pairs)
    {
        for (int i = 0; i < RELATION_GEOMETRIES_WKT.size(); i++) {
            for (int j = 0; j < RELATION_GEOMETRIES_WKT.size(); j++) {
                boolean ok = pairs.contains(Pair.of(i, j)) || pairs.contains(Pair.of(j, i));
                assertRelation(relation, RELATION_GEOMETRIES_WKT.get(i), RELATION_GEOMETRIES_WKT.get(j), ok);
            }
        }
    }

    private void assertRelation(String relation, String leftWkt, String rightWkt, Boolean expected)
    {
        assertFunction(format("%s(ST_GeometryFromText(%s), ST_GeometryFromText(%s))", relation, leftWkt, rightWkt), BOOLEAN, expected);
    }
}
