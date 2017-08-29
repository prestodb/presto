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

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestGeoQueries
        extends AbstractTestQueryFramework
{
    public TestGeoQueries()
    {
        super(TestGeoQueries::createLocalQueryRunner);
    }

    @Test
    public void testSTPolygon()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 'POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))'");
    }

    @Test
    public void testSTLine()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_line('linestring(1 1, 2 2, 1 3)'))", "select 'LINESTRING (1 1, 2 2, 1 3)'");
    }

    @Test
    public void testSTArea()
            throws Exception
    {
        assertQuery("select st_area(st_geometry_from_wkt('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select 16.0");
    }

    @Test
    public void testSTCentroid()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_centroid(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))')))", "select 'POINT (2.5 2.5)'");
    }

    @Test
    public void testSTCoordinateDimension()
            throws Exception
    {
        assertQuery("select st_coordinate_dimension(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 2");
    }

    @Test
    public void testSTDimension()
            throws Exception
    {
        assertQuery("select st_dimension(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 2");
    }

    @Test
    public void testSTIsClosed()
            throws Exception
    {
        assertQuery("select st_is_closed(st_geometry_from_wkt('linestring(1 1, 2 2, 1 3, 1 1)'))", "select true");
    }

    @Test
    public void testSTIsEmpty()
            throws Exception
    {
        assertQuery("select st_is_empty(st_geometry_from_wkt('POINT (1.5 2.5)'))", "select false");
    }

    @Test
    public void testSTLength()
            throws Exception
    {
        assertQuery("select st_length(st_geometry_from_wkt('linestring(0 0, 2 2)'))", "select 2.8284271247461903");
        assertQuery("select st_length(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 12.0");
    }

    @Test
    public void testSTMax()
            throws Exception
    {
        assertQuery("select st_max_x(st_geometry_from_wkt('linestring(8 4, 4 8)'))", "select 8.0");
        assertQuery("select st_max_y(st_geometry_from_wkt('linestring(8 4, 4 8)'))", "select 8.0");
        assertQuery("select st_max_x(st_geometry_from_wkt('polygon ((2 0, 2 1, 3 1))'))", "select 3.0");
        assertQuery("select st_max_y(st_geometry_from_wkt('polygon ((2 0, 2 1, 3 1))'))", "select 1.0");
    }

    @Test
    public void testSTMin()
            throws Exception
    {
        assertQuery("select st_min_x(st_geometry_from_wkt('linestring(8 4, 4 8)'))", "select 4.0");
        assertQuery("select st_min_y(st_geometry_from_wkt('linestring(8 4, 4 8)'))", "select 4.0");
        assertQuery("select st_min_x(st_geometry_from_wkt('polygon ((2 0, 2 1, 3 1))'))", "select 2.0");
        assertQuery("select st_min_y(st_geometry_from_wkt('polygon ((2 0, 2 1, 3 1))'))", "select 0.0");
    }

    @Test
    public void testSTInteriorRingNumber()
            throws Exception
    {
        assertQuery("select st_interior_ring_number(st_geometry_from_wkt('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", "select 1");
    }

    @Test
    public void testSTPointNumber()
            throws Exception
    {
        assertQuery("select st_point_number(st_geometry_from_wkt('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", "select 8");
    }

    @Test
    public void testSTIsRing()
            throws Exception
    {
        assertQuery("select st_is_ring(st_geometry_from_wkt('linestring(8 4, 4 8)'))", "select false");
    }

    @Test
    public void testSTStartEndPoint()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_start_point(st_geometry_from_wkt('linestring(8 4, 4 8, 5 6)')))", "select 'POINT (8 4)'");
        assertQuery("select st_geometry_to_wkt(st_end_point(st_geometry_from_wkt('linestring(8 4, 4 8, 5 6)')))", "select 'POINT (5 6)'");
    }

    @Test
    public void testSTXY()
            throws Exception
    {
        assertQuery("select st_x(st_geometry_from_wkt('POINT (1 2)'))", "select 1.0");
        assertQuery("select st_y(st_geometry_from_wkt('POINT (1 2)'))", "select 2.0");
    }

    @Test
    public void testSTBoundary()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_boundary(st_geometry_from_wkt('polygon ((1 1, 4 1, 1 4))')))", "select 'LINESTRING (1 1, 4 1, 1 4, 1 1)'");
    }

    @Test
    public void testSTEnvelope()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_envelope(st_geometry_from_wkt('linestring(1 1, 2 2, 1 3)')))", "select 'POLYGON ((1 1, 2 1, 2 3, 1 3, 1 1))'");
    }

    @Test
    public void testSTDifference()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_difference(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'POLYGON ((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1))'");
    }

    @Test
    public void testSTDistance()
            throws Exception
    {
        assertQuery("select st_distance(st_geometry_from_wkt('polygon ((1 1, 1 3, 3 3, 3 1))'), st_geometry_from_wkt('polygon ((4 4, 4 5, 5 5, 5 4))'))", "select 1.4142135623730951");
    }

    @Test
    public void testSTExteriorRing()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_exterior_ring(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 1))')))", "select 'LINESTRING (1 1, 4 1, 1 4, 1 1)'");
    }

    @Test
    public void testSTIntersection()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_intersection(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))'");
        assertQuery("select st_geometry_to_wkt(st_intersection(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('linestring(2 0, 2 3)')))", "select 'LINESTRING (2 1, 2 3)'");
    }

    @Test
    public void testSTSymmetricDifference()
            throws Exception
    {
        assertQuery("select st_geometry_to_wkt(st_symmetric_difference(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'MULTIPOLYGON (((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1)), ((4 2, 5 2, 5 5, 2 5, 2 4, 4 4, 4 2)))'");
    }

    @Test
    public void testStContains()
            throws Exception
    {
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((0 2,1 1,0 -1,0 2))'), st_geometry_from_wkt('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('LINESTRING(20 20,30 30)'), st_geometry_from_wkt('POINT(25 25)'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((-1 2, 0 3, 0 1, -1 2))'), st_geometry_from_wkt('POLYGON((0 3, -1 2, 0 1, 0 3))'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt(null), st_geometry_from_wkt('POINT(25 25)'))", "SELECT null");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((0 2,1 1,0 -1,0 2))'), st_geometry_from_wkt('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('LINESTRING(20 20,30 30)'), st_geometry_from_wkt('POINT(25 25)'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((-1 2, 0 3, 0 1, -1 2))'), st_geometry_from_wkt('POLYGON((0 3, -1 2, 0 1, 0 3))'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt(null), st_geometry_from_wkt('POINT(25 25)'))", "SELECT null");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((0 2,1 1,0 -1,0 2))'), st_geometry_from_wkt('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('POLYGON((0 2,1 1,0 -1,0 2))'), st_geometry_from_wkt('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('LINESTRING(20 20,30 30)'), st_geometry_from_wkt('POINT(25 25)'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_wkt('LINESTRING(20 20,30 30)'), st_geometry_from_wkt('POINT(25 25)'))", "SELECT true");
    }

    @Test
    public void testSTCrosses()
            throws Exception
    {
        assertQuery("select st_crosses(st_geometry_from_wkt('linestring(0 0, 1 1)'), st_geometry_from_wkt('linestring(1 0, 0 1)'))", "select true");
        assertQuery("select st_crosses(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('polygon ((2 2, 2 5, 5 5, 5 2))'))", "select false");
        assertQuery("select st_crosses(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('linestring(2 0, 2 3)'))", "select true");
        assertQuery("select st_crosses(st_geometry_from_wkt('linestring(0 0, 1 1)'), st_geometry_from_wkt('linestring(1 0, 0 1)'))", "select true");
    }

    @Test
    public void testSTDisjoint()
            throws Exception
    {
        assertQuery("select st_disjoint(st_geometry_from_wkt('linestring(0 0, 0 1)'), st_geometry_from_wkt('linestring(1 1, 1 0)'))", "select true");
        assertQuery("select st_disjoint(st_geometry_from_wkt('polygon ((1 1, 1 3, 3 3, 3 1))'), st_geometry_from_wkt('polygon ((4 4, 4 5, 5 5, 5 4))'))", "select true");
        assertQuery("select st_disjoint(st_geometry_from_wkt('polygon ((1 1, 1 3, 3 3, 3 1))'), st_geometry_from_wkt('polygon ((4 4, 4 5, 5 5, 5 4))'))", "select true");
    }

    @Test
    public void testSTEnvelopeIntersect()
            throws Exception
    {
        assertQuery("select st_envelope_intersect(st_geometry_from_wkt('linestring(0 0, 2 2)'), st_geometry_from_wkt('linestring(1 1, 3 3)'))", "select true");
    }

    @Test
    public void testSTEquals()
            throws Exception
    {
        assertQuery("select st_equals(st_geometry_from_wkt('linestring(0 0, 2 2)'), st_geometry_from_wkt('linestring(0 0, 2 2)'))", "select true");
        assertQuery("select st_equals(st_geometry_from_wkt('linestring(0 0, 2 2)'), st_geometry_from_wkt('linestring(0 0, 2 2)'))", "select true");
    }

    @Test
    public void testSTIntersects()
            throws Exception
    {
        assertQuery("select st_intersects(st_geometry_from_wkt('linestring(8 4, 4 8)'), st_geometry_from_wkt('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
        assertQuery("select st_intersects(st_geometry_from_wkt('linestring(8 4, 4 8)'), st_geometry_from_wkt('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
        assertQuery("select st_intersects(st_geometry_from_wkt('linestring(8 4, 4 8)'), st_geometry_from_wkt('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
        assertQuery("select st_intersects(st_geometry_from_wkt('linestring(8 4, 4 8)'), st_geometry_from_wkt('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
    }

    @Test
    public void testSTOverlaps()
            throws Exception
    {
        assertQuery("select st_overlaps(st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), st_geometry_from_wkt('polygon ((3 3, 3 5, 5 5, 5 3))'))", "select true");
    }

    @Test
    public void testSTRelate()
            throws Exception
    {
        assertQuery("select st_relate(st_geometry_from_wkt('polygon ((2 0, 2 1, 3 1))'), st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'), '****T****')", "select true");
    }

    @Test
    public void testSTTouches()
            throws Exception
    {
        assertQuery("select st_touches(st_geometry_from_wkt('POINT(1 2)'), st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select true");
    }

    @Test
    public void testSTWithin()
            throws Exception
    {
        assertQuery("select st_within(st_geometry_from_wkt('POINT(3 2)'), st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select true");
        assertQuery("select st_within(st_geometry_from_wkt('POINT(3 2)'), st_geometry_from_wkt('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select true");
    }

    @Test
    public void testGeoContains()
            throws Exception
    {
        assertQuery("select geo_contains(st_geometry_from_wkt('POINT(25 25)'), model) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains(st_geometry_from_wkt('POINT(25 25)'), model) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT NULL");
    }

    @Test
    public void testGeoIntersects()
            throws Exception
    {
        assertQuery("select geo_intersects(st_geometry_from_wkt('POLYGON((-1 2,0 3,0 1,-1 2))'), model) from (select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,0 1,1 0))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_intersects(st_geometry_from_wkt('POLYGON((-1 2,0 3,0 1,-1 2))'), model) from (select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,2 2,1 0))')) as t(shape_id, shape))", "SELECT NULL ");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(defaultSession.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.<String, String>of());

        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            localQueryRunner.getTypeManager().addType(type);
        }
        localQueryRunner.getMetadata().addFunctions(extractFunctions(new GeoPlugin().getFunctions()));

        return localQueryRunner;
    }
}
