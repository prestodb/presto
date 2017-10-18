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
package com.facebook.presto.geospatial;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestGeoQueries
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
    }

    @Test
    public void testSTPoint()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Point(1, 4))", VARCHAR, "POINT (1 4)");
        assertFunction("ST_AsText(ST_Point(122.3, 10.55))", VARCHAR, "POINT (122.3 10.55)");
    }

    @Test
    public void testSTLineFromText()
            throws Exception
    {
        assertFunction("ST_AsText(ST_LineFromText('LINESTRING EMPTY'))", VARCHAR, "MULTILINESTRING EMPTY");
        assertFunction("ST_AsText(ST_LineFromText('LINESTRING (1 1, 2 2, 1 3)'))", VARCHAR, "LINESTRING (1 1, 2 2, 1 3)");
        assertInvalidFunction("ST_AsText(ST_LineFromText('MULTILINESTRING EMPTY'))", "ST_LineFromText only applies to LINE_STRING. Input type is: MULTI_LINE_STRING");
        assertInvalidFunction("ST_AsText(ST_LineFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", "ST_LineFromText only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTPolygon()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Polygon('POLYGON EMPTY'))", VARCHAR, "MULTIPOLYGON EMPTY");
        assertFunction("ST_AsText(ST_Polygon('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", VARCHAR, "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))");
        assertInvalidFunction("ST_AsText(ST_Polygon('LINESTRING (1 1, 2 2, 1 3)'))", "ST_Polygon only applies to POLYGON. Input type is: LINE_STRING");
    }

    @Test
    public void testSTArea()
            throws Exception
    {
        assertFunction("ST_Area(ST_GeometryFromText('POLYGON ((2 2, 2 6, 6 6, 6 2))'))", DOUBLE, 16.0);
        assertFunction("ST_Area(ST_GeometryFromText('POLYGON EMPTY'))", DOUBLE, 0.0);
        assertInvalidFunction("ST_Area(ST_GeometryFromText('POINT (1 4)'))", "ST_Area only applies to POLYGON or MULTI_POLYGON. Input type is: POINT");
    }

    @Test
    public void testSTCentroid()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('LINESTRING EMPTY')))", VARCHAR, "POINT EMPTY");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('POINT (3 5)')))", VARCHAR, "POINT (3 5)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "POINT (2.5 5)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('LINESTRING (1 1, 2 2, 3 3)')))", VARCHAR, "POINT (2 2)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')))", VARCHAR, "POINT (3 2)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))')))", VARCHAR, "POINT (2.5 2.5)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('POLYGON ((1 1, 5 1, 3 4))')))", VARCHAR, "POINT (3 2)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((2 4, 2 6, 6 6, 6 4)))')))", VARCHAR, "POINT (3.3333333333333335 4)");
        assertFunction("ST_AsText(ST_Centroid(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))')))", VARCHAR, "POINT (2.5416666666666665 2.5416666666666665)");
    }

    @Test
    public void testSTCoordDim()
            throws Exception
    {
        assertFunction("ST_CoordDim(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", TINYINT, (byte) 2);
        assertFunction("ST_CoordDim(ST_GeometryFromText('POLYGON EMPTY'))", TINYINT, (byte) 2);
        assertFunction("ST_CoordDim(ST_GeometryFromText('LINESTRING EMPTY'))", TINYINT, (byte) 2);
        assertFunction("ST_CoordDim(ST_GeometryFromText('POINT (1 4)'))", TINYINT, (byte) 2);
    }

    @Test
    public void testSTDimension()
            throws Exception
    {
        assertFunction("ST_Dimension(ST_GeometryFromText('POLYGON EMPTY'))", TINYINT, (byte) 2);
        assertFunction("ST_Dimension(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", TINYINT, (byte) 2);
        assertFunction("ST_Dimension(ST_GeometryFromText('LINESTRING EMPTY'))", TINYINT, (byte) 1);
        assertFunction("ST_Dimension(ST_GeometryFromText('POINT (1 4)'))", TINYINT, (byte) 0);
    }

    @Test
    public void testSTIsClosed()
            throws Exception
    {
        assertFunction("ST_IsClosed(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3, 1 1)'))", BOOLEAN, true);
        assertFunction("ST_IsClosed(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)'))", BOOLEAN, false);
        assertInvalidFunction("ST_IsClosed(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", "ST_IsClosed only applies to LINE_STRING or MULTI_LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTIsEmpty()
            throws Exception
    {
        assertFunction("ST_IsEmpty(ST_GeometryFromText('POINT (1.5 2.5)'))", BOOLEAN, false);
        assertFunction("ST_IsEmpty(ST_GeometryFromText('POLYGON EMPTY'))", BOOLEAN, true);
    }

    @Test
    public void testSTLength()
            throws Exception
    {
        assertFunction("ST_Length(ST_GeometryFromText('LINESTRING EMPTY'))", DOUBLE, 0.0);
        assertFunction("ST_Length(ST_GeometryFromText('LINESTRING (0 0, 2 2)'))", DOUBLE, 2.8284271247461903);
        assertFunction("ST_Length(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))", DOUBLE, 6.0);
        assertInvalidFunction("ST_Length(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", "ST_Length only applies to LINE_STRING or MULTI_LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTMax()
            throws Exception
    {
        assertFunction("ST_XMax(ST_GeometryFromText('POINT (1.5 2.5)'))", DOUBLE, 1.5);
        assertFunction("ST_YMax(ST_GeometryFromText('POINT (1.5 2.5)'))", DOUBLE, 2.5);
        assertFunction("ST_XMax(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))", DOUBLE, 4.0);
        assertFunction("ST_YMax(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))", DOUBLE, 8.0);
        assertFunction("ST_XMax(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", DOUBLE, 8.0);
        assertFunction("ST_YMax(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", DOUBLE, 7.0);
        assertFunction("ST_XMax(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))", DOUBLE, 5.0);
        assertFunction("ST_YMax(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))", DOUBLE, 4.0);
        assertFunction("ST_XMax(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", DOUBLE, 3.0);
        assertFunction("ST_YMax(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", DOUBLE, 1.0);
        assertFunction("ST_XMax(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((2 4, 2 6, 6 6, 6 4)))'))", DOUBLE, 6.0);
        assertFunction("ST_YMax(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((2 4, 2 6, 6 10, 6 4)))'))", DOUBLE, 10.0);
    }

    @Test
    public void testSTMin()
            throws Exception
    {
        assertFunction("ST_XMin(ST_GeometryFromText('POINT (1.5 2.5)'))", DOUBLE, 1.5);
        assertFunction("ST_YMin(ST_GeometryFromText('POINT (1.5 2.5)'))", DOUBLE, 2.5);
        assertFunction("ST_XMin(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))", DOUBLE, 1.0);
        assertFunction("ST_YMin(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))", DOUBLE, 2.0);
        assertFunction("ST_XMin(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", DOUBLE, 5.0);
        assertFunction("ST_YMin(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", DOUBLE, 4.0);
        assertFunction("ST_XMin(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))", DOUBLE, 1.0);
        assertFunction("ST_YMin(ST_GeometryFromText('MULTILINESTRING ((1 2, 5 3), (2 4, 4 4))'))", DOUBLE, 2.0);
        assertFunction("ST_XMin(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", DOUBLE, 2.0);
        assertFunction("ST_YMin(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", DOUBLE, 0.0);
        assertFunction("ST_XMin(ST_GeometryFromText('MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10)), ((2 4, 2 6, 6 6, 6 4)))'))", DOUBLE, 1.0);
        assertFunction("ST_YMin(ST_GeometryFromText('MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10)), ((2 4, 2 6, 6 10, 6 4)))'))", DOUBLE, 3.0);
    }

    @Test
    public void testSTNumInteriorRing()
            throws Exception
    {
        assertFunction("ST_NumInteriorRing(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'))", BIGINT, 0L);
        assertFunction("ST_NumInteriorRing(ST_GeometryFromText('POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", BIGINT, 1L);
        assertInvalidFunction("ST_NumInteriorRing(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", "ST_NumInteriorRing only applies to POLYGON. Input type is: LINE_STRING");
    }

    @Test
    public void testSTNumPoints()
            throws Exception
    {
        assertFunction("ST_NumPoints(ST_GeometryFromText('POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", BIGINT, 6L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((2 4, 2 6, 6 6, 6 4)))'))", BIGINT, 8L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('LINESTRING (8 4, 5 7)'))", BIGINT, 2L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'))", BIGINT, 4L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('POINT (1 2)'))", BIGINT, 1L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)'))", BIGINT, 4L);
        assertFunction("ST_NumPoints(ST_GeometryFromText('LINESTRING EMPTY'))", BIGINT, 0L);
    }

    @Test
    public void testSTIsRing()
            throws Exception
    {
        assertFunction("ST_IsRing(ST_GeometryFromText('LINESTRING (8 4, 4 8)'))", BOOLEAN, false);
        assertFunction("ST_IsRing(ST_GeometryFromText('LINESTRING (0 0, 1 1, 0 2, 0 0)'))", BOOLEAN, true);
        assertInvalidFunction("ST_IsRing(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", "ST_IsRing only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTStartEndPoint()
            throws Exception
    {
        assertFunction("ST_AsText(ST_StartPoint(ST_GeometryFromText('LINESTRING (8 4, 4 8, 5 6)')))", VARCHAR, "POINT (8 4)");
        assertFunction("ST_AsText(ST_EndPoint(ST_GeometryFromText('LINESTRING (8 4, 4 8, 5 6)')))", VARCHAR, "POINT (5 6)");
        assertInvalidFunction("ST_AsText(ST_StartPoint(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))')))", "ST_StartPoint only applies to LINE_STRING. Input type is: POLYGON");
        assertInvalidFunction("ST_AsText(ST_EndPoint(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))')))", "ST_EndPoint only applies to LINE_STRING. Input type is: POLYGON");
    }

    @Test
    public void testSTXY()
            throws Exception
    {
        assertFunction("ST_Y(ST_GeometryFromText('POINT EMPTY'))", DOUBLE, null);
        assertFunction("ST_X(ST_GeometryFromText('POINT (1 2)'))", DOUBLE, 1.0);
        assertFunction("ST_Y(ST_GeometryFromText('POINT (1 2)'))", DOUBLE, 2.0);
        assertInvalidFunction("ST_Y(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'))", "ST_Y only applies to POINT. Input type is: POLYGON");
    }

    @Test
    public void testSTBoundary()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('POINT (1 2)')))", VARCHAR, "POINT EMPTY");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "POINT EMPTY");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('LINESTRING EMPTY')))", VARCHAR, "POINT EMPTY");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('LINESTRING (8 4, 5 7)')))", VARCHAR, "MULTIPOINT ((8 4), (5 7))");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('LINESTRING (100 150,50 60, 70 80, 160 170)')))", VARCHAR, "MULTIPOINT ((100 150), (160 170))");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')))", VARCHAR, "MULTIPOINT ((1 1), (5 1), (2 4), (4 4))");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('POLYGON ((1 1, 4 1, 1 4))')))", VARCHAR, "LINESTRING (1 1, 4 1, 1 4, 1 1)");
        assertFunction("ST_AsText(ST_Boundary(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))')))", VARCHAR, "MULTILINESTRING ((1 1, 3 1, 3 3, 1 3, 1 1), (0 0, 2 0, 2 2, 0 2, 0 0))");
    }

    @Test
    public void testSTEnvelope()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "POLYGON ((1 2, 4 2, 4 8, 1 8, 1 2))");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('LINESTRING EMPTY')))", VARCHAR, "MULTIPOLYGON EMPTY");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)')))", VARCHAR, "POLYGON ((1 1, 2 1, 2 3, 1 3, 1 1))");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('LINESTRING (8 4, 5 7)')))", VARCHAR, "POLYGON ((5 4, 8 4, 8 7, 5 7, 5 4))");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))')))", VARCHAR, "POLYGON ((1 1, 5 1, 5 4, 1 4, 1 1))");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('POLYGON ((1 1, 4 1, 1 4))')))", VARCHAR, "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))");
        assertFunction("ST_AsText(ST_Envelope(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))')))", VARCHAR, "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))");
    }

    @Test
    public void testSTDifference()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)')))", VARCHAR, "POINT (50 100)");
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)')))", VARCHAR, "POINT (50 200)");
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (50 50, 50 150)')))", VARCHAR, "LINESTRING (50 150, 50 200)");
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((2 1, 4 1), (3 3, 7 3))')))", VARCHAR, "MULTILINESTRING ((1 1, 2 1), (4 1, 5 1), (2 4, 4 4))");
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2))')))", VARCHAR, "POLYGON ((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1))");
        assertFunction("ST_AsText(ST_Difference(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))')))", VARCHAR, "POLYGON ((1 1, 0 1, 0 0, 2 0, 2 1, 1 1))");
    }

    @Test
    public void testSTDistance()
            throws Exception
    {
        assertFunction("ST_Distance(ST_Point(50, 100), ST_Point(150, 150))", DOUBLE, 111.80339887498948);
        assertFunction("ST_Distance(ST_Point(50, 100), ST_GeometryFromText('POINT (150 150)'))", DOUBLE, 111.80339887498948);
        assertFunction("ST_Distance(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", DOUBLE, 111.80339887498948);
        assertFunction("ST_Distance(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('Point (50 100)'))", DOUBLE, 0.0);
        assertFunction("ST_Distance(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (10 10, 20 20)'))", DOUBLE, 85.44003745317531);
        assertFunction("ST_Distance(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('LINESTRING (10 20, 20 50)'))", DOUBLE, 17.08800749063506);
        assertFunction("ST_Distance(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))'))", DOUBLE, 1.4142135623730951);
        assertFunction("ST_Distance(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((10 100, 30 10))'))", DOUBLE, 27.892651361962706);
    }

    @Test
    public void testSTExteriorRing()
            throws Exception
    {
        assertFunction("ST_AsText(ST_ExteriorRing(ST_GeometryFromText('POLYGON EMPTY')))", VARCHAR, null);
        assertFunction("ST_AsText(ST_ExteriorRing(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 1))')))", VARCHAR, "LINESTRING (1 1, 4 1, 1 4, 1 1)");
        assertFunction("ST_AsText(ST_ExteriorRing(ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))')))", VARCHAR, "LINESTRING (0 0, 5 0, 5 5, 0 5, 0 0)");
        assertInvalidFunction("ST_AsText(ST_ExteriorRing(ST_GeometryFromText('LINESTRING (1 1, 2 2, 1 3)')))", "ST_ExteriorRing only applies to POLYGON or MULTI_POLYGON. Input type is: LINE_STRING");
    }

    @Test
    public void testSTIntersection()
            throws Exception
    {
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)')))", VARCHAR, "MULTIPOLYGON EMPTY");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('Point (50 100)')))", VARCHAR, "POINT (50 100)");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (20 150, 100 150)')))", VARCHAR, "POINT (50 150)");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')))", VARCHAR, "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))')))", VARCHAR, "MULTIPOLYGON EMPTY");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))')))", VARCHAR, "GEOMETRYCOLLECTION (LINESTRING (1 1, 2 1), MULTIPOLYGON (((0 1, 1 1, 1 2, 0 2, 0 1)), ((2 1, 3 1, 3 3, 1 3, 1 2, 2 2, 2 1))))");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), ST_GeometryFromText('LINESTRING (2 0, 2 3)')))", VARCHAR, "LINESTRING (2 1, 2 3)");
        assertFunction("ST_AsText(ST_Intersection(ST_GeometryFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeometryFromText('LINESTRING (0 0, 1 -1, 1 2)')))", VARCHAR, "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 0, 1 1))");
    }

    @Test
    public void testSTSymmetricDifference()
            throws Exception
    {
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (50 150)')))", VARCHAR, "MULTIPOINT ((50 100), (50 150))");
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('MULTIPOINT (50 100, 60 200)'), ST_GeometryFromText('MULTIPOINT (60 200, 70 150)')))", VARCHAR, "MULTIPOINT ((50 100), (70 150))");
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (50 50, 50 150)')))", VARCHAR, "MULTILINESTRING ((50 50, 50 100), (50 150, 50 200))");
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))')))", VARCHAR, "MULTILINESTRING ((5 0, 5 1), (1 1, 5 1), (5 1, 5 4), (2 4, 3 4), (4 4, 5 4), (5 4, 6 4))");
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2))')))", VARCHAR, "MULTIPOLYGON (((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1)), ((4 2, 5 2, 5 5, 2 5, 2 4, 4 4, 4 2)))");
        assertFunction("ST_AsText(ST_SymDifference(ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))'), ST_GeometryFromText('POLYGON ((0 0, 0 3, 3 3, 3 0))')))", VARCHAR, "MULTIPOLYGON (((2 0, 3 0, 3 2, 2 2, 2 0)), ((0 2, 2 2, 2 3, 0 3, 0 2)), ((3 2, 4 2, 4 4, 2 4, 2 3, 3 3, 3 2)))");
    }

    @Test
    public void testStContains()
            throws Exception
    {
        assertFunction("ST_Contains(ST_GeometryFromText(null), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, null);
        assertFunction("ST_Contains(ST_GeometryFromText('POINT (20 20)'), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, false);
        assertFunction("ST_Contains(ST_GeometryFromText('MULTIPOINT (20 20, 25 25)'), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, true);
        assertFunction("ST_Contains(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, true);
        assertFunction("ST_Contains(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('MULTIPOINT (25 25, 31 31)'))", BOOLEAN, false);
        assertFunction("ST_Contains(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('LINESTRING (25 25, 27 27)'))", BOOLEAN, true);
        assertFunction("ST_Contains(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))'))", BOOLEAN, false);
        assertFunction("ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'), ST_GeometryFromText('POLYGON ((1 1, 1 2, 2 2, 2 1))'))", BOOLEAN, true);
        assertFunction("ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'), ST_GeometryFromText('POLYGON ((-1 -1, -1 2, 2 2, 2 -1))'))", BOOLEAN, false);
        assertFunction("ST_Contains(ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))'), ST_GeometryFromText('POLYGON ((2 2, 2 3, 3 3, 3 2))'))", BOOLEAN, true);
        assertFunction("ST_Contains(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'))", BOOLEAN, false);
    }

    @Test
    public void testSTCrosses()
            throws Exception
    {
        assertFunction("ST_Crosses(ST_GeometryFromText('POINT (20 20)'), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, false);
        assertFunction("ST_Crosses(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('POINT (25 25)'))", BOOLEAN, false);
        assertFunction("ST_Crosses(ST_GeometryFromText('LINESTRING (20 20, 30 30)'), ST_GeometryFromText('MULTIPOINT (25 25, 31 31)'))", BOOLEAN, true);
        assertFunction("ST_Crosses(ST_GeometryFromText('LINESTRING(0 0, 1 1)'), ST_GeometryFromText('LINESTRING (1 0, 0 1)'))", BOOLEAN, true);
        assertFunction("ST_Crosses(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), ST_GeometryFromText('POLYGON ((2 2, 2 5, 5 5, 5 2))'))", BOOLEAN, false);
        assertFunction("ST_Crosses(ST_GeometryFromText('MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))'), ST_GeometryFromText('POLYGON ((2 2, 2 3, 3 3, 3 2))'))", BOOLEAN, false);
        assertFunction("ST_Crosses(ST_GeometryFromText('LINESTRING (-2 -2, 6 6)'), ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'))", BOOLEAN, true);
    }

    @Test
    public void testSTDisjoint()
            throws Exception
    {
        assertFunction("ST_Disjoint(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, true);
        assertFunction("ST_Disjoint(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))", BOOLEAN, false);
        assertFunction("ST_Disjoint(ST_GeometryFromText('LINESTRING (0 0, 0 1)'), ST_GeometryFromText('LINESTRING (1 1, 1 0)'))", BOOLEAN, true);
        assertFunction("ST_Disjoint(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (20 150, 100 150)'))", BOOLEAN, false);
        assertFunction("ST_Disjoint(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Disjoint(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))'))", BOOLEAN, true);
        assertFunction("ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, false);
    }

    @Test
    public void testSTEquals()
            throws Exception
    {
        assertFunction("ST_Equals(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, false);
        assertFunction("ST_Equals(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))", BOOLEAN, false);
        assertFunction("ST_Equals(ST_GeometryFromText('LINESTRING (0 0, 0 1)'), ST_GeometryFromText('LINESTRING (1 1, 1 0)'))", BOOLEAN, false);
        assertFunction("ST_Equals(ST_GeometryFromText('LINESTRING (0 0, 2 2)'), ST_GeometryFromText('LINESTRING (0 0, 2 2)'))", BOOLEAN, true);
        assertFunction("ST_Equals(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Equals(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((3 3, 3 1, 1 1, 1 3))'))", BOOLEAN, true);
        assertFunction("ST_Equals(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, false);
    }

    @Test
    public void testSTIntersects()
            throws Exception
    {
        assertFunction("ST_Intersects(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, false);
        assertFunction("ST_Intersects(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))", BOOLEAN, true);
        assertFunction("ST_Intersects(ST_GeometryFromText('LINESTRING (0 0, 0 1)'), ST_GeometryFromText('LINESTRING (1 1, 1 0)'))", BOOLEAN, false);
        assertFunction("ST_Intersects(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (20 150, 100 150)'))", BOOLEAN, true);
        assertFunction("ST_Intersects(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, true);
        assertFunction("ST_Intersects(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, true);
    }

    @Test
    public void testSTOverlaps()
            throws Exception
    {
        assertFunction("ST_Overlaps(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, false);
        assertFunction("ST_Overlaps(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))", BOOLEAN, false);
        assertFunction("ST_Overlaps(ST_GeometryFromText('LINESTRING (0 0, 0 1)'), ST_GeometryFromText('LINESTRING (1 1, 1 0)'))", BOOLEAN, false);
        assertFunction("ST_Overlaps(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, true);
        assertFunction("ST_Overlaps(ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), ST_GeometryFromText('POLYGON ((3 3, 3 5, 5 5, 5 3))'))", BOOLEAN, true);
        assertFunction("ST_Overlaps(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Overlaps(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, true);
    }

    @Test
    public void testSTRelate()
            throws Exception
    {
        assertFunction("ST_Relate(ST_GeometryFromText('LINESTRING (0 0, 3 3)'), ST_GeometryFromText('LINESTRING (1 1, 4 1)'), '****T****')", BOOLEAN, false);
        assertFunction("ST_Relate(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), '****T****')", BOOLEAN, true);
        assertFunction("ST_Relate(ST_GeometryFromText('POLYGON ((2 0, 2 1, 3 1))'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'), 'T********')", BOOLEAN, false);
    }

    @Test
    public void testSTTouches()
            throws Exception
    {
        assertFunction("ST_Touches(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, false);
        assertFunction("ST_Touches(ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'), ST_GeometryFromText('POINT (50 100)'))", BOOLEAN, false);
        assertFunction("ST_Touches(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (20 150, 100 150)'))", BOOLEAN, false);
        assertFunction("ST_Touches(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Touches(ST_GeometryFromText('POINT (1 2)'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", BOOLEAN, true);
        assertFunction("ST_Touches(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((4 4, 4 5, 5 5, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Touches(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('LINESTRING (0 0, 1 1)'))", BOOLEAN, true);
        assertFunction("ST_Touches(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((3 3, 3 5, 5 5, 5 3))'))", BOOLEAN, true);
        assertFunction("ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, false);
    }

    @Test
    public void testSTWithin()
            throws Exception
    {
        assertFunction("ST_Within(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('POINT (150 150)'))", BOOLEAN, false);
        assertFunction("ST_Within(ST_GeometryFromText('POINT (50 100)'), ST_GeometryFromText('MULTIPOINT (50 100, 50 200)'))", BOOLEAN, true);
        assertFunction("ST_Within(ST_GeometryFromText('LINESTRING (50 100, 50 200)'), ST_GeometryFromText('LINESTRING (50 50, 50 250)'))", BOOLEAN, true);
        assertFunction("ST_Within(ST_GeometryFromText('MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))'), ST_GeometryFromText('MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))'))", BOOLEAN, false);
        assertFunction("ST_Within(ST_GeometryFromText('POINT (3 2)'), ST_GeometryFromText('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", BOOLEAN, true);
        assertFunction("ST_Within(ST_GeometryFromText('POLYGON ((1 1, 1 3, 3 3, 3 1))'), ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'))", BOOLEAN, true);
        assertFunction("ST_Within(ST_GeometryFromText('LINESTRING (1 1, 3 3)'), ST_GeometryFromText('POLYGON ((0 0, 0 4, 4 4, 4 0))'))", BOOLEAN, true);
        assertFunction("ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1)), ((0 0, 0 2, 2 2, 2 0)))'), ST_GeometryFromText('POLYGON ((0 1, 3 1, 3 3, 0 3))'))", BOOLEAN, false);
    }
}
