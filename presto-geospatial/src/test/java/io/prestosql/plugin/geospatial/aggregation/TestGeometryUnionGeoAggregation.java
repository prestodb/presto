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
package io.prestosql.plugin.geospatial.aggregation;

import com.google.common.base.Joiner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY;
import static java.lang.String.format;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;

public class TestGeometryUnionGeoAggregation
        extends AbstractTestGeoAggregationFunctions
{
    private static final Joiner COMMA_JOINER = Joiner.on(",");

    @DataProvider(name = "point")
    public Object[][] point()
    {
        return new Object[][] {
                {
                        "identity",
                        "POINT (1 2)",
                        new String[] {"POINT (1 2)", "POINT (1 2)", "POINT (1 2)"},
                },
                {
                        "no input yields null",
                        null,
                        new String[] {},
                },
                {
                        "empty with non-empty",
                        "POINT (1 2)",
                        new String[] {"POINT EMPTY", "POINT (1 2)"},
                },
                {
                        "disjoint returns multipoint",
                        "MULTIPOINT ((1 2), (3 4))",
                        new String[] {"POINT (1 2)", "POINT (3 4)"},
                },
        };
    }

    @DataProvider(name = "linestring")
    public Object[][] linestring()
    {
        return new Object[][] {
                {
                        "identity",
                        "LINESTRING (1 1, 2 2)",
                        new String[] {"LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)"},
                },
                {
                        "empty with non-empty",
                        "LINESTRING (1 1, 2 2)",
                        new String[] {"LINESTRING EMPTY", "LINESTRING (1 1, 2 2)"},
                },
                {
                        "overlap",
                        "LINESTRING (1 1, 2 2, 3 3, 4 4)",
                        new String[] {"LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (2 2, 3 3, 4 4)"},
                },
                {
                        "disjoint returns multistring",
                        "MULTILINESTRING ((1 1, 2 2, 3 3), (1 2, 2 3, 3 4))",
                        new String[] {"LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (1 2, 2 3, 3 4)"},
                },
                {
                        "cut through returns multistring",
                        "MULTILINESTRING ((1 1, 2 2), (3 1, 2 2), (2 2, 3 3), (2 2, 1 3))",
                        new String[] {"LINESTRING (1 1, 3 3)", "LINESTRING (3 1, 1 3)"},
                },
        };
    }

    @DataProvider(name = "polygon")
    public Object[][] polygon()
    {
        return new Object[][] {
                {
                        "identity",
                        "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        new String[] {"POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))"},
                },
                {
                        "empty with non-empty",
                        "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        new String[] {"POLYGON EMPTY)", "POLYGON ((2 2, 1 1, 3 1, 2 2))"},
                },
                {
                        "three overlapping triangles",
                        "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((3 2, 4 1, 2 1, 3 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))"},
                },
                {
                        "two triangles touching at 3 1 returns multipolygon",
                        "MULTIPOLYGON (((1 1, 3 1, 2 2, 1 1)), ((3 1, 5 1, 4 2, 3 1)))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))"},
                },
                {
                        "two disjoint triangles returns multipolygon",
                        "MULTIPOLYGON (((1 1, 3 1, 2 2, 1 1)), ((4 1, 6 1, 5 2, 4 1)))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((5 2, 6 1, 4 1, 5 2))"},
                },
                {
                        "polygon with hole that is filled is simplified",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))"},
                },
                {
                        "polygon with hole with shape larger than hole is simplified",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2))"},
                },
                {
                        "polygon with hole with shape smaller than hole becomes multipolygon",
                        "MULTIPOLYGON (((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3)), ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25)))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25))"},
                },
                {
                        "polygon with hole with several smaller pieces which fill hole simplify into polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 3 3.5, 3.5 3.5, 3.5 3, 3 3))",
                                "POLYGON ((3.5 3.5, 3.5 4, 4 4, 4 3.5, 3.5 3.5))", "POLYGON ((3 3.5, 3 4, 3.5 4, 3.5 3.5, 3 3.5))",
                                "POLYGON ((3.5 3, 3.5 3.5, 4 3.5, 4 3, 3.5 3))"},
                },
                {
                        "two overlapping rectangles becomes cross",
                        "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                        new String[] {"POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))", "POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))"},
                },
                {
                        "touching squares become single cross",
                        "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                        new String[] {"POLYGON ((1 3, 1 4, 3 4, 3 3, 1 3))", "POLYGON ((3 3, 3 4, 4 4, 4 3, 3 3))", "POLYGON ((4 3, 4 4, 6 4, 6 3, 4 3))",
                                "POLYGON ((3 1, 4 1, 4 3, 3 3, 3 1))", "POLYGON ((3 4, 3 6, 4 6, 4 4, 3 4))"},
                },
                {
                        "square with touching point becomes simplified polygon",
                        "POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 1))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (3 2)"},
                },
        };
    }

    @DataProvider(name = "multipoint")
    public Object[][] multipoint()
    {
        return new Object[][] {
                {
                        "identity",
                        "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        new String[] {"MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))"},
                },
                {
                        "empty with non-empty",
                        "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        new String[] {"MULTIPOINT EMPTY", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))"},
                },
                {
                        "disjoint",
                        "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        new String[] {"MULTIPOINT ((1 2), (2 4))", "MULTIPOINT ((3 6), (4 8))"},
                },
                {
                        "overlap",
                        "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        new String[] {"MULTIPOINT ((1 2), (2 4))", "MULTIPOINT ((2 4), (3 6))", "MULTIPOINT ((3 6), (4 8))"},
                },
        };
    }

    @DataProvider(name = "multilinestring")
    public Object[][] multilinestring()
    {
        return new Object[][] {
                {
                        "identity",
                        "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))"},
                },
                {
                        "empty with non-empty",
                        "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                        new String[] {"MULTILINESTRING EMPTY", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))"},
                },
                {
                        "disjoint",
                        "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1), (3 5, 6 1), (4 5, 7 1))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))"},
                },
                {
                        "disjoint aggregates with cut through",
                        "MULTILINESTRING ((2.5 3, 4 1), (3.5 3, 5 1), (4.5 3, 6 1), (5.5 3, 7 1), (1 3, 2.5 3), (2.5 3, 3.5 3), (1 5, 2.5 3), (3.5 3, 4.5 3), (2 5, 3.5 3), (4.5 3, 5.5 3), (3 5, 4.5 3), (5.5 3, 8 3), (4 5, 5.5 3))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))", "LINESTRING (1 3, 8 3)"},
                },
        };
    }

    @DataProvider(name = "multipolygon")
    public Object[][] multipolygon()
    {
        return new Object[][] {
                {
                        "identity",
                        "MULTIPOLYGON (((4 2, 3 1, 5 1, 4 2)), ((14 12, 13 11, 15 11, 14 12)))",
                        new String[] {"MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))",
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))"},
                },
                {
                        "empty with non-empty",
                        "MULTIPOLYGON (((4 2, 3 1, 5 1, 4 2)), ((14 12, 13 11, 15 11, 14 12)))",
                        new String[] {"MULTIPOLYGON EMPTY", "MULTIPOLYGON (((4 2, 5 1, 3 1, 4 2)), ((14 12, 15 11, 13 11, 14 12)))"},
                },
                {
                        "disjoint",
                        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)), ((0 3, 2 3, 2 5, 0 5, 0 3)), ((3 3, 5 3, 5 5, 3 5, 3 3)))",
                        new String[] {"MULTIPOLYGON ((( 0 0, 0 2, 2 2, 2 0, 0 0 )), (( 0 3, 0 5, 2 5, 2 3, 0 3 )))",
                                "MULTIPOLYGON ((( 3 0, 3 2, 5 2, 5 0, 3 0 )), (( 3 3, 3 5, 5 5, 5 3, 3 3 )))"},
                },
                {
                        "overlapping multipolygons are simplified",
                        "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                        new String[] {"MULTIPOLYGON (((2 2, 3 1, 1 1, 2 2)), ((3 2, 4 1, 2 1, 3 2)))", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))"},
                },
                {
                        "overlapping multipolygons become single cross",
                        "POLYGON ((3 1, 4 1, 4 3, 6 3, 6 4, 4 4, 4 6, 3 6, 3 4, 1 4, 1 3, 3 3, 3 1))",
                        new String[] {"MULTIPOLYGON (((1 3, 1 4, 3 4, 3 3, 1 3)), ((3 3, 3 4, 4 4, 4 3, 3 3)), ((4 3, 4 4, 6 4, 6 3, 4 3)))",
                                "MULTIPOLYGON (((3 1, 4 1, 4 3, 3 3, 3 1)), ((3 4, 3 6, 4 6, 4 4, 3 4)))"},
                },
        };
    }

    @DataProvider(name = "geometrycollection")
    public Object[][] geometryCollection()
    {
        return new Object[][] {
                {
                        "identity",
                        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                        new String[] {"MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))"},
                },
                {
                        "empty collection with empty collection",
                        "GEOMETRYCOLLECTION EMPTY",
                        new String[] {"GEOMETRYCOLLECTION EMPTY",
                                "GEOMETRYCOLLECTION EMPTY"},
                },
                {
                        "empty with non-empty",
                        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                        new String[] {"GEOMETRYCOLLECTION EMPTY",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))"},
                },
                {
                        "overlapping geometry collections are simplified",
                        "POLYGON ((1 1, 2 1, 3 1, 4 1, 5 1, 4 2, 3.5 1.5, 3 2, 2.5 1.5, 2 2, 1 1))",
                        new String[] {"GEOMETRYCOLLECTION ( POLYGON ((2 2, 3 1, 1 1, 2 2)), POLYGON ((3 2, 4 1, 2 1, 3 2)) )",
                                "GEOMETRYCOLLECTION ( POLYGON ((4 2, 5 1, 3 1, 4 2)) )"},
                },
                {
                        "disjoint geometry collection of polygons becomes multipolygon",
                        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)), ((0 3, 2 3, 2 5, 0 5, 0 3)), ((3 3, 5 3, 5 5, 3 5, 3 3)))",
                        new String[] {"GEOMETRYCOLLECTION ( POLYGON (( 0 0, 0 2, 2 2, 2 0, 0 0 )), POLYGON (( 0 3, 0 5, 2 5, 2 3, 0 3 )) )",
                                "GEOMETRYCOLLECTION ( POLYGON (( 3 0, 3 2, 5 2, 5 0, 3 0 )), POLYGON (( 3 3, 3 5, 5 5, 5 3, 3 3 )) )"},
                },
                {
                        "square with a line crossed becomes geometry collection",
                        "GEOMETRYCOLLECTION (MULTILINESTRING ((0 2, 1 2), (3 2, 5 2)), POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 2, 1 1)))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 2, 5 2)"},
                },
                {
                        "square with adjacent line becomes geometry collection",
                        "GEOMETRYCOLLECTION (LINESTRING (0 5, 5 5), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 5, 5 5)"},
                },
                {
                        "square with adjacent point becomes geometry collection",
                        "GEOMETRYCOLLECTION (POINT (5 2), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (5 2)"},
                },
        };
    }

    @Test(dataProvider = "point")
    public void testPoint(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "linestring")
    public void testLineString(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "polygon")
    public void testPolygon(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "multipoint")
    public void testMultiPoint(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "multilinestring")
    public void testMultiLineString(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "multipolygon")
    public void testMultiPolygon(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "geometrycollection")
    public void testGeometryCollection(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
        assertArrayAggAndGeometryUnion(testDescription, expectedWkt, wkts);
    }

    @Override
    protected String getFunctionName()
    {
        return "geometry_union_agg";
    }

    private void assertArrayAggAndGeometryUnion(String testDescription, String expectedWkt, String[] wkts)
    {
        List<String> wktList = Arrays.stream(wkts).map(wkt -> format("ST_GeometryFromText('%s')", wkt)).collect(toList());
        String wktArray = format("ARRAY[%s]", COMMA_JOINER.join(wktList));
        // ST_Union(ARRAY[ST_GeometryFromText('...'), ...])
        assertFunction(format("geometry_union(%s)", wktArray), GEOMETRY, expectedWkt);

        reverse(wktList);
        wktArray = format("ARRAY[%s]", COMMA_JOINER.join(wktList));
        assertFunction(format("geometry_union(%s)", wktArray), GEOMETRY, expectedWkt);
    }
}
