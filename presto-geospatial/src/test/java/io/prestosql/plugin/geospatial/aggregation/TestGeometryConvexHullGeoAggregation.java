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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestGeometryConvexHullGeoAggregation
        extends AbstractTestGeoAggregationFunctions
{
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
                        "null before value yields the value",
                        "POINT (1 2)",
                        new String[] {null, "POINT (1 2)"},
                },
                {
                        "null after value yields the value",
                        "POINT (1 2)",
                        new String[] {"POINT (1 2)", null},
                },
                {
                        "empty with non-empty",
                        "POINT (1 2)",
                        new String[] {"POINT EMPTY", "POINT (1 2)"},
                },
                {
                        "2 disjoint points return linestring",
                        "LINESTRING (1 2, 3 4)",
                        new String[] {"POINT (1 2)", "POINT (3 4)"},
                },
                {
                        "points lying on the same line return linestring",
                        "LINESTRING (3 3, 1 1)",
                        new String[] {"POINT (1 1)", "POINT (2 2)", "POINT (3 3)"},
                },
                {
                        "points forming a polygon return polygon",
                        "POLYGON ((5 8, 2 3, 1 1, 5 8))",
                        new String[] {"POINT (1 1)", "POINT (2 3)", "POINT (5 8)"},
                }
        };
    }

    @DataProvider(name = "linestring")
    public Object[][] linestring()
    {
        return new Object[][] {
                {
                        "identity",
                        "LINESTRING (1 1, 2 2)",
                        new String[] {"LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)",
                                "LINESTRING (1 1, 2 2)"},
                },
                {
                        "empty with non-empty",
                        "LINESTRING (1 1, 2 2)",
                        new String[] {"LINESTRING EMPTY", "LINESTRING (1 1, 2 2)"},
                },
                {
                        "overlap",
                        "LINESTRING (1 1, 4 4)",
                        new String[] {"LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (2 2, 3 3, 4 4)"},
                },
                {
                        "disjoint returns polygon",
                        "POLYGON ((1 1, 3 3, 3 4, 1 2, 1 1))",
                        new String[] {"LINESTRING (1 1, 2 2, 3 3)", "LINESTRING (1 2, 2 3, 3 4)"},
                },
                {
                        "cut through returns polygon",
                        "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)),",
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
                        new String[] {
                                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        },
                },
                {
                        "empty with non-empty",
                        "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        new String[] {
                                "POLYGON EMPTY",
                                "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        },
                },
                {
                        "three overlapping triangles",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {
                                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                                "POLYGON ((3 2, 4 1, 2 1, 3 2))",
                                "POLYGON ((4 2, 5 1, 3 1, 4 2))",
                        },
                },
                {
                        "two triangles touching at 3 1 returns polygon",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {
                                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                                "POLYGON ((4 2, 5 1, 3 1, 4 2))",
                        },
                },
                {
                        "two disjoint triangles returns polygon",
                        "POLYGON ((1 1, 6 1, 5 2, 2 2, 1 1))",
                        new String[] {
                                "POLYGON ((2 2, 3 1, 1 1, 2 2))",
                                "POLYGON ((5 2, 6 1, 4 1, 5 2))",
                        },
                },
                {
                        "polygon with hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {
                                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                                "POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))",
                        },
                },
                {
                        "polygon with hole with shape larger than hole is simplified",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {
                                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                                "POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2))",
                        },
                },
                {
                        "polygon with hole with shape smaller than hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {
                                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                                "POLYGON ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25))",
                        },
                },
                {
                        "polygon with hole with several smaller pieces which fill hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {
                                "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
                                "POLYGON ((3 3, 3 3.5, 3.5 3.5, 3.5 3, 3 3))",
                                "POLYGON ((3.5 3.5, 3.5 4, 4 4, 4 3.5, 3.5 3.5))",
                                "POLYGON ((3 3.5, 3 4, 3.5 4, 3.5 3.5, 3 3.5))",
                                "POLYGON ((3.5 3, 3.5 3.5, 4 3.5, 4 3, 3.5 3))",
                        },
                },
                {
                        "two overlapping rectangles",
                        "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
                        new String[] {
                                "POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))",
                                "POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))",
                        },
                },
                {
                        "touching squares",
                        "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
                        new String[] {
                                "POLYGON ((1 3, 1 4, 3 4, 3 3, 1 3))",
                                "POLYGON ((3 3, 3 4, 4 4, 4 3, 3 3))",
                                "POLYGON ((4 3, 4 4, 6 4, 6 3, 4 3))",
                                "POLYGON ((3 1, 4 1, 4 3, 3 3, 3 1))",
                                "POLYGON ((3 4, 3 6, 4 6, 4 4, 3 4))",
                        },
                },
                {
                        "square with touching point becomes simplified polygon",
                        "POLYGON ((1 1, 3 1, 3 2, 3 3, 1 3, 1 1))",
                        new String[] {
                                "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
                                "POINT (3 2)",
                        },
                },
        };
    }

    @DataProvider(name = "multipoint")
    public Object[][] multipoint()
    {
        return new Object[][] {
                {
                        "lying on the same line",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {
                                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        },
                },
                {
                        "empty with non-empty",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {
                                "MULTIPOINT EMPTY",
                                "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))",
                        },
                },
                {
                        "disjoint",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {
                                "MULTIPOINT ((1 2), (2 4))",
                                "MULTIPOINT ((3 6), (4 8))",
                        },
                },
                {
                        "overlap",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {
                                "MULTIPOINT ((1 2), (2 4))",
                                "MULTIPOINT ((2 4), (3 6))",
                                "MULTIPOINT ((3 6), (4 8))",
                        },
                },
        };
    }

    @DataProvider(name = "multilinestring")
    public Object[][] multilinestring()
    {
        return new Object[][] {
                {
                        "identity",
                        "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                        new String[] {
                                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                        },
                },
                {
                        "empty with non-empty",
                        "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                        new String[] {
                                "MULTILINESTRING EMPTY",
                                "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))",
                        },
                },
                {
                        "disjoint",
                        "POLYGON ((4 5, 1 5, 4 1, 7 1, 4 5))",
                        new String[] {
                                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))",
                                "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))",
                        },
                },
                {
                        "disjoint aggregates with cut through",
                        "POLYGON ((1 3, 4 1, 6 1, 8 3, 3 5, 1 5, 1 3))",
                        new String[] {
                                "MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))",
                                "LINESTRING (1 3, 8 3)",
                        },
                },
        };
    }

    @DataProvider(name = "multipolygon")
    public Object[][] multipolygon()
    {
        return new Object[][] {
                {
                        "identity",
                        "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        new String[] {
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        },
                },
                {
                        "empty with non-empty",
                        "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        new String[] {
                                "MULTIPOLYGON EMPTY",
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        },
                },
                {
                        "disjoint",
                        "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
                        new String[] {
                                "MULTIPOLYGON ((( 0 0, 0 2, 2 2, 2 0, 0 0 )), (( 0 3, 0 5, 2 5, 2 3, 0 3 )))",
                                "MULTIPOLYGON ((( 3 0, 3 2, 5 2, 5 0, 3 0 )), (( 3 3, 3 5, 5 5, 5 3, 3 3 )))",
                        },
                },
                {
                        "overlapping multipolygons",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {
                                "MULTIPOLYGON (((2 2, 3 1, 1 1, 2 2)), ((3 2, 4 1, 2 1, 3 2)))",
                                "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        },
                },
        };
    }

    @DataProvider(name = "1000points")
    public Object[][] points1000()
            throws IOException
    {
        Path filePath = Paths.get(this.getClass().getClassLoader().getResource("1000_points.txt").getPath());
        List<String> points = Files.readAllLines(filePath);
        return new Object[][] {
                {
                        "1000points",
                        "POLYGON ((0.7642699 0.000490129, 0.92900103 0.005068898, 0.97419316 0.019917727, 0.99918157 0.063635945, 0.9997078 0.10172784, 0.9973114 0.41161585, 0.9909166 0.94222105, 0.9679412 0.9754768, 0.95201814 0.9936909, 0.44082636 0.9999601, 0.18622541 0.998157, 0.07163471 0.98902994, 0.066090584 0.9885783, 0.024429202 0.9685611, 0.0044354796 0.8878008, 0.0025004745 0.81172496, 0.0015820265 0.39900982, 0.001614511 0.00065791607, 0.7642699 0.000490129))",
                        points.toArray(new String[0]),
                },
        };
    }

    @DataProvider(name = "geometryCollection")
    public Object[][] geometryCollection()
    {
        return new Object[][] {
                {
                        "identity",
                        "POLYGON ((0 0, 5 0, 5 2, 0 2, 0 0))",
                        new String[] {"MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))"},
                },
                {
                        "empty with non-empty",
                        "POLYGON ((0 0, 5 0, 5 2, 0 2, 0 0))",
                        new String[] {"GEOMETRYCOLLECTION EMPTY",
                                "GEOMETRYCOLLECTION ( POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0)))"},
                },
                {
                        "overlapping geometry collections",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {"GEOMETRYCOLLECTION ( POLYGON ((2 2, 3 1, 1 1, 2 2)), POLYGON ((3 2, 4 1, 2 1, 3 2)) )",
                                "GEOMETRYCOLLECTION ( POLYGON ((4 2, 5 1, 3 1, 4 2)) )"},
                },
                {
                        "disjoint geometry collection of polygons",
                        "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
                        new String[] {"GEOMETRYCOLLECTION ( POLYGON (( 0 0, 0 2, 2 2, 2 0, 0 0 )), POLYGON (( 0 3, 0 5, 2 5, 2 3, 0 3 )) )",
                                "GEOMETRYCOLLECTION ( POLYGON (( 3 0, 3 2, 5 2, 5 0, 3 0 )), POLYGON (( 3 3, 3 5, 5 5, 5 3, 3 3 )) )"},
                },
                {
                        "square with a line crossed",
                        "POLYGON ((0 2, 1 1, 3 1, 5 2, 3 3, 1 3, 0 2))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 2, 5 2)"},
                },
                {
                        "square with adjacent line",
                        "POLYGON ((0 5, 1 1, 3 1, 5 5, 0 5))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "LINESTRING (0 5, 5 5)"},
                },
                {
                        "square with adjacent point",
                        "POLYGON ((5 2, 3 3, 1 3, 1 1, 3 1, 5 2))",
                        new String[] {"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POINT (5 2)"},
                },
        };
    }

    @Test(dataProvider = "point")
    public void testPoint(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "linestring")
    public void testLineString(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "polygon")
    public void testPolygon(String testDescription, String expectedWkt, String... wkts)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkts);
    }

    @Test(dataProvider = "multipoint")
    public void testMultipoint(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    @Test(dataProvider = "multilinestring")
    public void testMultilinestring(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    @Test(dataProvider = "multipolygon")
    public void testMultipolygon(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    @Test(dataProvider = "1000points")
    public void test1000Points(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    @Test(dataProvider = "geometryCollection")
    public void testGeometryCollection(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    @Override
    protected String getFunctionName()
    {
        return "convex_hull_agg";
    }
}
