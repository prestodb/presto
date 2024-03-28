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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * Test class that tests for H3HexFunctions
 */
public class TestH3HexFunctions
        extends AbstractTestQueryFramework
{
    private FunctionAssertions funcAssert;

    @BeforeClass
    public void setUpClass()
            throws Exception
    {
        this.funcAssert = new FunctionAssertions(this.getSession());
    }

    @Test
    public void testHexAddr()
            throws Exception
    {
        assertQuery("select get_hexagon_addr(40.730610, -73.935242,2)", "select '822a17fffffffff'");
        assertQuery("select get_hexagon_addr(37.773972,-122.431297,2)", "select '822837fffffffff'");

        assertQuery("select get_hexagon_addr(NULL, NULL, NULL)", "select NULL");
    }

    @Test(enabled = false)
    /*
     * This test is disabled. Locally this test passes but in Jenkins there is a insignificant decimal difference in one
     * of the coordinates. Tried mocking H3 and the H3HexFunctions classes. But due to the design of UDF classes,
     * mocking is extremely difficult. The UDF classes are final classes and are constructed by class loaders. With such
     * a setup mocking is challemging. Hence disabling this test for now until we can figure out someway to make this work.
     */
    public void testHexAddrWkt()
            throws Exception
    {
        assertQuery("select get_hexagon_addr_wkt('822a17fffffffff')", "select 'POLYGON ((-73.07691180312379 42.400492689472884,-75.33345172379178 42.02956371225368,-75.96061877033631 40.48049730850132,-74.44277493761697 39.34056938395393,-72.30787665118663 39.68606179325923,-71.57377195480382 41.195725190458504, -73.07691180312379 42.400492689472884))'");
        assertQuery("select get_hexagon_addr_wkt('822837fffffffff')", "select 'POLYGON ((-121.70715691845142 36.57421829680793,-120.15030815558956 37.77836118370325,-120.62501817993413 39.39386760344102,-122.6990988675928 39.784230841420204,-124.23124622081257 38.56638700335243,-123.71598551689976 36.972296150193095, -121.70715691845142 36.57421829680793))'");
    }

    @Test
    public void testHexAddrWkt_invalid()
            throws Exception
    {
        assertQuery("select get_hexagon_addr_wkt(NULL)", "select NULL");
        assertQuery("select get_hexagon_addr_wkt('')", "select NULL");

        assertQueryFails("select get_hexagon_addr_wkt('1231231231')", "Input is not a valid h3 address \\(1231231231\\).");
    }

    @Test
    public void testHexParent()
            throws Exception
    {
        assertQuery("select get_parent_hexagon_addr('89283475983ffff', 8)", "select '8828347599fffff'");
        assertQuery("select get_parent_hexagon_addr('89283475983ffff', 7)", "select '872834759ffffff'");

        assertQuery("select get_parent_hexagon_addr(NULL, 7)", "select NULL");
        assertQuery("select get_parent_hexagon_addr('89283475983ffff', NULL)", "select NULL");

        assertQueryFails("select get_parent_hexagon_addr('89283475983ffff', 10)", "res \\(10\\) must be between 0 and 9, inclusive");
        assertQueryFails("select get_parent_hexagon_addr('89283475983ffff', -1)", "res \\(-1\\) must be between 0 and 9, inclusive");
    }

    @Test
    public void testHexDistance()
            throws Exception
    {
        assertQuery("select h3_distance(NULL, NULL)", "select NULL");
        assertQuery("select h3_distance('8928308280fffff', NULL)", "select NULL");
        assertQuery("select h3_distance(NULL, '8928308280fffff')", "select NULL");
        assertQuery("select h3_distance('', '')", "select NULL");
        assertQuery("select h3_distance('8928308280fffff', '')", "select NULL");
        assertQuery("select h3_distance('', '8928308280fffff')", "select NULL");

        assertQuery("select h3_distance('8928308280fffff', '892830828d7ffff')", "select 4");
        assertQuery("select h3_distance('89283082993ffff', '89283082827ffff')", "select 5");
        assertQueryFails("select h3_distance('123141411', '8928308280fffff')", "Input is not a valid h3 address \\(123141411\\).");
        assertQueryFails("select h3_distance('8928308280fffff', '123141411')", "Input is not a valid h3 address \\(123141411\\).");
    }

    @Test(enabled = false)
    /*
     * This test is disabled because the test passes locally but does not pass on jenkins due to a decimal difference in one
     * of the resulting coordinates.
     */
    public void testH3ToGeo()
            throws Exception
    {
        funcAssert.assertFunction("h3_to_geo('89283082993ffff')", new ArrayType(DOUBLE), ImmutableList.of(37.77137479807198, -122.44623843414703));
        assertQuery("select h3_to_geo(NULL)", "select NULL");
        assertQuery("select h3_to_geo('')", "select NULL");
        assertQueryFails("select h3_to_geo('123141411')", "Input is not a valid h3 address \\(123141411\\).");
    }

    @Test
    public void testH3ToString()
            throws Exception
    {
        assertQuery("select h3_to_string(617700149764816895)", "select '8928303746fffff'");
        assertQuery("select h3_to_string(NULL)", "select NULL");
    }

    @Test
    public void testEdgeLength()
            throws Exception
    {
        assertQuery("select h3_edge_length(NULL, 'km')", "select NULL");
        assertQuery("select h3_edge_length(1, '')", "select NULL");
        assertQuery("select h3_edge_length(NULL, '')", "select NULL");
        assertQuery("select h3_edge_length(NULL, NULL)", "select NULL");

        assertQuery("select h3_edge_length(1, 'km')", "select 418.6760055");
        assertQuery("select h3_edge_length(1, 'm')", "select 418676.0055");
        assertQuery("select h3_edge_length(5, 'km')", "select 8.544408276");

        assertQueryFails("select h3_edge_length(-1, 'm')", "resolution -1 is out of range \\(must be 0 <= res <= 15\\)");
        assertQueryFails("select h3_edge_length(1, 'illegal_str')", "Length unit must be 'm' or 'km'.");
    }

    @Test
    public void testHexArea()
            throws Exception
    {
        assertQuery("select h3_hex_area(1, '')", "select NULL");
        assertQuery("select h3_hex_area(NULL, 'km2')", "select NULL");
        assertQuery("select h3_hex_area(NULL, '')", "select NULL");
        assertQuery("select h3_hex_area(NULL, NULL)", "select NULL");

        assertQuery("select h3_hex_area(1, 'km2')", "select 609788.4417941332");
        assertQuery("select h3_hex_area(1, 'm2')", "select 609788441794.1332");
        assertQuery("select h3_hex_area(5, 'km2')", "select 252.9033645");

        assertQueryFails("select h3_hex_area(-1, 'm2')", "resolution -1 is out of range \\(must be 0 <= res <= 15\\)");
        assertQueryFails("select h3_hex_area(1, 'illegal_str')", "Area unit must be 'm2' or 'km2'.");
    }

    @Test
    public void testGetResolution()
            throws Exception
    {
        assertQuery("select h3_resolution('')", "select NULL");
        assertQuery("select h3_resolution(NULL)", "select NULL");
        assertQuery("select h3_resolution('8429ab7ffffffff')", "select 4");
        assertQuery("select h3_resolution('8944a18ec03ffff')", "select 9");
        assertQuery("select h3_resolution('8f44a1385a40000')", "select 15");

        assertQueryFails("select h3_resolution('1234112')", "Input is not a valid h3 address \\(1234112\\).");
    }

    @Test
    public void testGetH3Children()
            throws Exception
    {
        assertQuery("select h3_to_children('8928308280fffff', NULL)", "select NULL");
        assertQuery("select h3_to_children('', NULL)", "select NULL");
        assertQuery("select h3_to_children('', 10)", "select NULL");
        assertQuery("select h3_to_children(NULL, 10)", "select NULL");

        assertQuery("select h3_to_children('8928308280fffff', 10)", "select ('8a28308280c7fff', '8a28308280cffff', '8a28308280d7fff', '8a28308280dffff', '8a28308280e7fff', '8a28308280effff', '8a28308280f7fff')");

        assertQuery("select h3_to_children('8928308280fffff', 10)", "select ('8a28308280c7fff', '8a28308280cffff', '8a28308280d7fff', '8a28308280dffff', '8a28308280e7fff', '8a28308280effff', '8a28308280f7fff')");
        assertQuery("select h3_to_children('8928308280fffff', 9)", "select ('8928308280fffff')");
        assertQueryFails("select h3_to_children('1234112', 5)", "Input is not a valid h3 address \\(1234112\\).");
        assertQueryFails("select h3_to_children('8928308280fffff', -1)", "resolution -1 is out of range \\(must be 0 <= res <= 15\\)");
    }

    @Test
    public void testGetHexRange()
            throws Exception
    {
        assertQuery("select h3_hex_range('', 1)", "select NULL");
        assertQuery("select h3_hex_range(NULL, 1)", "select NULL");
        assertQuery("select h3_hex_range('8944a18ec03ffff', NULL)", "select NULL");
        assertQuery("select h3_hex_range(NULL, NULL)", "select NULL");

        funcAssert.assertFunction("h3_hex_range('8944a18ec03ffff', 2)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("8944a18ec03ffff"), ImmutableList.of("8944a18ec1bffff", "8944a18ec0bffff", "8944a18ec0fffff", "8944a18ec07ffff", "8944a18ec17ffff", "8944a18ec13ffff"), ImmutableList.of("8944a18ecc7ffff", "8944a18eccfffff", "8944a18ec57ffff", "8944a18ec47ffff", "8944a18ec73ffff", "8944a18ec77ffff", "8944a18ec3bffff", "8944a18ec33ffff", "8944a18ecabffff", "8944a18ecbbffff", "8944a18ec8fffff", "8944a18ec8bffff")));
        funcAssert.assertFunction("h3_hex_range('8944a18ec03ffff', 1)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("8944a18ec03ffff"), ImmutableList.of("8944a18ec1bffff", "8944a18ec0bffff", "8944a18ec0fffff", "8944a18ec07ffff", "8944a18ec17ffff", "8944a18ec13ffff")));

        assertQueryFails("select h3_hex_range('8944a18ec03ffff', -1)", "Number of rings must be greater than or equal to 0.");
        assertQueryFails("select h3_hex_range('12341134', 2)", "Input is not a valid h3 address \\(12341134\\).");
    }

    @Test
    public void testGetHexRing()
            throws Exception
    {
        assertQuery("select h3_hex_ring('', 1)", "select NULL");
        assertQuery("select h3_hex_ring(NULL, 1)", "select NULL");
        assertQuery("select h3_hex_ring('8944a18ec03ffff', NULL)", "select NULL");
        assertQuery("select h3_hex_ring(NULL, NULL)", "select NULL");

        funcAssert.assertFunction("h3_hex_ring('8944a18ec03ffff', 1)", new ArrayType(VARCHAR), ImmutableList.of("8944a18ec13ffff", "8944a18ec1bffff", "8944a18ec0bffff", "8944a18ec0fffff", "8944a18ec07ffff", "8944a18ec17ffff"));
        funcAssert.assertFunction("h3_hex_ring('8944a18ec03ffff', 0)", new ArrayType(VARCHAR), ImmutableList.of("8944a18ec03ffff"));

        assertQueryFails("select h3_hex_ring('1234235', 1)", "Input is not a valid h3 address \\(1234235\\).");
        assertQueryFails("select h3_hex_ring('8944a18ec03ffff', -1)", "Ring number must be greater than or equal to 0.");
    }

    @Test
    public void testGetKRing()
            throws Exception
    {
        assertQuery("select h3_kring('', 1)", "select NULL");
        assertQuery("select h3_kring(NULL, 1)", "select NULL");
        assertQuery("select h3_kring('8944a18ec03ffff', NULL)", "select NULL");
        assertQuery("select h3_kring(NULL, NULL)", "select NULL");

        funcAssert.assertFunction("h3_kring('8928308280fffff', 2)", new ArrayType(VARCHAR), ImmutableList.of("8928308280fffff", "8928308280bffff", "89283082873ffff", "89283082877ffff", "8928308283bffff", "89283082807ffff", "89283082803ffff", "8928308281bffff", "89283082857ffff", "89283082847ffff", "8928308287bffff", "89283082863ffff", "89283082867ffff", "8928308282bffff", "89283082823ffff", "89283082833ffff", "892830828abffff", "89283082817ffff", "89283082813ffff"));
        funcAssert.assertFunction("h3_kring('8928308280fffff', 0)", new ArrayType(VARCHAR), ImmutableList.of("8928308280fffff"));

        assertQueryFails("select h3_kring('1234235', 1)", "Input is not a valid h3 address \\(1234235\\).");
        assertQueryFails("select h3_kring('8944a18ec03ffff', -1)", "Number of rings must be greater than or equal to 0.");
    }

    @Test
    public void testIsPentagon()
            throws Exception
    {
        assertQuery("select h3_is_pentagon('')", "select NULL");
        assertQuery("select h3_is_pentagon(NULL)", "select NULL");

        assertQuery("select h3_is_pentagon('804dfffffffffff')", "select true");
        assertQuery("select h3_is_pentagon('8f44a1385a40000')", "select false");

        assertQueryFails("select h3_is_pentagon('1234341')", "Input is not a valid h3 address \\(1234341\\).");
    }

    @Test
    public void testUncompact()
            throws Exception
    {
        assertQuery("select h3_uncompact(NULL, 9)", "select NULL");
        assertQuery("select h3_uncompact(array['8844a18ecbfffff'], NULL)", "select NULL");
        assertQuery("select h3_uncompact(NULL, NULL)", "select NULL");

        funcAssert.assertFunction("h3_uncompact(array['8844a18ecbfffff'], 9)", new ArrayType(VARCHAR), ImmutableList.of("8944a18eca3ffff", "8944a18eca7ffff", "8944a18ecabffff", "8944a18ecafffff", "8944a18ecb3ffff", "8944a18ecb7ffff", "8944a18ecbbffff"));
        funcAssert.assertFunction("h3_uncompact(array['8844a18ec9fffff', '8844a18ecbfffff'], 9)", new ArrayType(VARCHAR), ImmutableList.of("8944a18ec83ffff", "8944a18ec87ffff", "8944a18ec8bffff", "8944a18ec8fffff", "8944a18ec93ffff", "8944a18ec97ffff", "8944a18ec9bffff", "8944a18eca3ffff", "8944a18eca7ffff", "8944a18ecabffff", "8944a18ecafffff", "8944a18ecb3ffff", "8944a18ecb7ffff", "8944a18ecbbffff"));
        funcAssert.assertFunction("h3_uncompact(array[], 9)", new ArrayType(VARCHAR), ImmutableList.of());

        assertQueryFails("select h3_uncompact(array['12341111'], 9)", "Input is not a valid h3 address \\(12341111\\).");
        assertQueryFails("select h3_uncompact(array['8844a18ecbfffff'], -1)", "resolution -1 is out of range \\(must be 0 <= res <= 15\\)");
    }

    @Test
    public void testCompact()
            throws Exception
    {
        assertQuery("select h3_compact(NULL)", "select NULL");

        funcAssert.assertFunction("h3_compact(array['8944a18ecb3ffff', '8944a18ecb7ffff'])", new ArrayType(VARCHAR), ImmutableList.of("8944a18ecb3ffff", "8944a18ecb7ffff"));
        funcAssert.assertFunction("h3_compact(array['8944a18ecb3ffff', '8944a18ecb7ffff', '8944a18ecbbffff', '8944a18ecafffff', '8944a18ecabffff', '8944a18eca7ffff', '8944a18eca3ffff', '8a2a1072b587fff', '8a2a1072b5b7fff'])", new ArrayType(VARCHAR), ImmutableList.of("8a2a1072b587fff", "8a2a1072b5b7fff", "8844a18ecbfffff"));
        funcAssert.assertFunction("h3_compact(array['8944a18ecb3ffff', '8944a18ecb7ffff', '8944a18ecbbffff', '8944a18ecafffff', '8944a18ecabffff', '8944a18eca7ffff'])", new ArrayType(VARCHAR), ImmutableList.of("8944a18ecb3ffff", "8944a18ecb7ffff", "8944a18ecbbffff", "8944a18ecafffff", "8944a18ecabffff", "8944a18eca7ffff"));

        funcAssert.assertFunction("h3_compact(array[])", new ArrayType(VARCHAR), ImmutableList.of());
        assertQueryFails("select h3_compact(array['12341111'])", "Input is not a valid h3 address \\(12341111\\).");
    }

    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("UTC");

    @Override
    protected QueryRunner createQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
        return localQueryRunner;
    }
}
