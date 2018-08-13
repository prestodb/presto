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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.plugin.geospatial.GeoPlugin;
import com.facebook.presto.plugin.geospatial.GeometryType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;

public class TestConvexHullAgg extends AbstractTestFunctions
{
    @DataProvider(name = "point")
    public Object [][] point()
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
                        new String[] {"LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)", "LINESTRING (1 1, 2 2)"},
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
                        new String[] {"POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))", "POLYGON ((2 2, 1 1, 3 1, 2 2))"},
                },
                {
                        "empty with non-empty",
                        "POLYGON ((2 2, 1 1, 3 1, 2 2))",
                        new String[] {"POLYGON EMPTY)", "POLYGON ((2 2, 1 1, 3 1, 2 2))"},
                },
                {
                        "three overlapping triangles",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((3 2, 4 1, 2 1, 3 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))"},
                },
                {
                        "two triangles touching at 3 1 returns polygon",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((4 2, 5 1, 3 1, 4 2))"},
                },
                {
                        "two disjoint triangles returns polygon",
                        "POLYGON ((1 1, 6 1, 5 2, 2 2, 1 1))",
                        new String[] {"POLYGON ((2 2, 3 1, 1 1, 2 2))", "POLYGON ((5 2, 6 1, 4 1, 5 2))"},
                },
                {
                        "polygon with hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))"},
                },
                {
                        "polygon with hole with shape larger than hole is simplified",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2))"},
                },
                {
                        "polygon with hole with shape smaller than hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3.25 3.25, 3.75 3.25, 3.75 3.75, 3.25 3.75, 3.25 3.25))"},
                },
                {
                        "polygon with hole with several smaller pieces which fill hole returns the exterior polygon",
                        "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))",
                        new String[] {"POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))", "POLYGON ((3 3, 3 3.5, 3.5 3.5, 3.5 3, 3 3))",
                                "POLYGON ((3.5 3.5, 3.5 4, 4 4, 4 3.5, 3.5 3.5))", "POLYGON ((3 3.5, 3 4, 3.5 4, 3.5 3.5, 3 3.5))",
                                "POLYGON ((3.5 3, 3.5 3.5, 4 3.5, 4 3, 3.5 3))"},
                },
                {
                        "two overlapping rectangles",
                        "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
                        new String[] {"POLYGON ((1 3, 1 4, 6 4, 6 3, 1 3))", "POLYGON ((3 1, 4 1, 4 6, 3 6, 3 1))"},
                },
                {
                        "touching squares",
                        "POLYGON ((3 1, 4 1, 6 3, 6 4, 4 6, 3 6, 1 4, 1 3, 3 1))",
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
                        "lying on the same line",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {"MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))"},
                },
                {
                        "empty with non-empty",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {"MULTIPOINT EMPTY", "MULTIPOINT ((1 2), (2 4), (3 6), (4 8))"},
                },
                {
                        "disjoint",
                        "LINESTRING (1 2, 4 8)",
                        new String[] {"MULTIPOINT ((1 2), (2 4))", "MULTIPOINT ((3 6), (4 8))"},
                },
                {
                        "overlap",
                        "LINESTRING (1 2, 4 8)",
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
                        "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))"},
                },
                {
                        "empty with non-empty",
                        "POLYGON ((4 1, 5 1, 2 5, 1 5, 4 1))",
                        new String[] {"MULTILINESTRING EMPTY", "MULTILINESTRING ((1 5, 4 1), (2 5, 5 1))"},
                },
                {
                        "disjoint",
                        "POLYGON ((4 5, 1 5, 4 1, 7 1, 4 5))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "MULTILINESTRING ((2 5, 5 1), (4 5, 7 1))"},
                },
                {
                        "disjoint aggregates with cut through",
                        "POLYGON ((1 3, 4 1, 6 1, 8 3, 3 5, 1 5, 1 3))",
                        new String[] {"MULTILINESTRING ((1 5, 4 1), (3 5, 6 1))", "LINESTRING (1 3, 8 3)"},
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
                        new String[] {"MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))"},
                },
                {
                        "empty with non-empty",
                        "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))",
                        new String[] {"MULTIPOLYGON EMPTY", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))"},
                },
                {
                        "disjoint",
                        "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
                        new String[] {"MULTIPOLYGON ((( 0 0, 0 2, 2 2, 2 0, 0 0 )), (( 0 3, 0 5, 2 5, 2 3, 0 3 )))",
                                "MULTIPOLYGON ((( 3 0, 3 2, 5 2, 5 0, 3 0 )), (( 3 3, 3 5, 5 5, 5 3, 3 3 )))"},
                },
                {
                        "overlapping multipolygons",
                        "POLYGON ((1 1, 5 1, 4 2, 2 2, 1 1))",
                        new String[] {"MULTIPOLYGON (((2 2, 3 1, 1 1, 2 2)), ((3 2, 4 1, 2 1, 3 2)))", "MULTIPOLYGON(((4 2, 5 1, 3 1, 4 2)))"},
                },
        };
    }

    @DataProvider(name = "geometrycollection")
    public Object[][] geometryCollection()
    {
        return new Object[][] {
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

    private InternalAggregationFunction function;

    @BeforeClass
    public void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            functionAssertions.getTypeRegistry().addType(type);
        }
        functionAssertions.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));
        function = functionAssertions.getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(new
                Signature("convex_hull_agg",
                FunctionKind.AGGREGATE,
                TypeSignature.parseTypeSignature(GeometryType.GEOMETRY_TYPE_NAME),
                TypeSignature.parseTypeSignature(GeometryType.GEOMETRY_TYPE_NAME)));
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

    @Test(dataProvider = "geometrycollection")
    public void testGeometrycollection(String testDescription, String expectedWkt, String... wkt)
    {
        assertAggregatedGeometries(testDescription, expectedWkt, wkt);
    }

    private void assertAggregatedGeometries(String testDescription, String expectedWkt, String... wkts)
    {
        List<Slice> geometrySlices = Arrays.stream(wkts)
                .map(text -> text == null ? null : OGCGeometry.fromText(text))
                .map(input -> input == null ? null : GeometrySerde.serialize(input))
                .collect(Collectors.toList());

        // Add a custom equality assertion because the resulting geometry may have its constituent points in a different order
        BiFunction<Object, Object, Boolean> equalityFunction = (left, right) -> {
            if (left == null && right == null) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            OGCGeometry leftGeometry = OGCGeometry.fromText(left.toString());
            OGCGeometry rightGeometry = OGCGeometry.fromText(right.toString());
            // Check for equality by getting the difference
            return leftGeometry.difference(rightGeometry).isEmpty() && rightGeometry.difference(leftGeometry).isEmpty();
        };
        // Test in forward and reverse order to verify that ordering doesn't affect the output
        assertAggregation(function, equalityFunction, testDescription, new Page(BlockAssertions.createSlicesBlock(geometrySlices)), expectedWkt);
        Collections.reverse(geometrySlices);
        assertAggregation(function, equalityFunction, testDescription, new Page(BlockAssertions.createSlicesBlock(geometrySlices)), expectedWkt);
    }
}
