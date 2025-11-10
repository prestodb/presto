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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for ClpUberPinotSplitFilterProvider.
 * Tests Uber-specific TEXT_MATCH transformations in addition to inherited
 * range mapping functionality.
 */
@Test(singleThreaded = true)
public class TestClpUberPinotSplitFilterProvider
{
    private String filterConfigPath;
    private ClpUberPinotSplitFilterProvider filterProvider;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-pinot-split-filter.json");
        if (resource == null) {
            throw new FileNotFoundException("test-pinot-split-filter.json not found in resources");
        }

        filterConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
        ClpConfig config = new ClpConfig();
        config.setSplitFilterConfig(filterConfigPath);
        filterProvider = new ClpUberPinotSplitFilterProvider(config);
    }

    /**
     * Test TEXT_MATCH transformation for simple equality predicates.
     * Verifies that Uber-specific TEXT_MATCH transformations are applied.
     */
    @Test
    public void testTextMatchTransformationSimpleEquality()
    {
        // Test single equality predicate with integer
        String sql1 = "\"status_code\" = 200";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");

        // Test single equality predicate with negative integer
        String sql2 = "\"level\" = -1";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/-1:level/')");

        // Test single equality predicate with decimal
        String sql3 = "\"score\" = 3.14";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/3.14:score/')");

        // Test single equality predicate with scientific notation
        String sql4 = "\"value\" = 1.5e10";
        String result4 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql4);
        assertEquals(result4, "TEXT_MATCH(\"__mergedTextIndex\", '/1.5e10:value/')");
    }

    /**
     * Test TEXT_MATCH transformation for string literal equality predicates.
     */
    @Test
    public void testTextMatchTransformationStringLiterals()
    {
        // Test single equality predicate with string literal
        String sql1 = "\"hostname\" = 'uber-server1'";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "TEXT_MATCH(\"__mergedTextIndex\", '/uber-server1:hostname/')");

        // Test string literal with special characters
        String sql2 = "\"service\" = 'uber.logging.service'";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/uber.logging.service:service/')");

        // Test empty string literal
        String sql3 = "\"tag\" = ''";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/:tag/')");

        // Test string literal with spaces
        String sql4 = "\"message\" = 'Hello Uber World'";
        String result4 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql4);
        assertEquals(result4, "TEXT_MATCH(\"__mergedTextIndex\", '/Hello Uber World:message/')");
    }

    /**
     * Test that range mappings are inherited and work correctly.
     * Columns with range mappings should NOT be transformed to TEXT_MATCH.
     */
    @Test
    public void testRangeMappingInheritance()
    {
        // Test that range-mapped columns don't get TEXT_MATCH transformation
        // msg.timestamp has range mapping in test config
        String sql1 = "\"msg.timestamp\" = 1234";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "(begin_timestamp <= 1234 AND end_timestamp >= 1234)");

        // Test greater than or equal (range mapping)
        String sql2 = "\"msg.timestamp\" >= 5000";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "end_timestamp >= 5000");

        // Test less than or equal (range mapping)
        String sql3 = "\"msg.timestamp\" <= 10000";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "begin_timestamp <= 10000");
    }

    /**
     * Test complex expressions with both TEXT_MATCH and range mappings.
     */
    @Test
    public void testMixedTransformations()
    {
        // Mix of range mapping and TEXT_MATCH
        String sql1 = "(\"msg.timestamp\" >= 1000 AND \"status_code\" = 200)";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "(end_timestamp >= 1000 AND TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/'))");

        // Multiple TEXT_MATCH transformations
        String sql2 = "(\"hostname\" = 'uber1' AND \"service\" = 'logging')";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "(TEXT_MATCH(\"__mergedTextIndex\", '/uber1:hostname/') AND TEXT_MATCH(\"__mergedTextIndex\", '/logging:service/'))");

        // Complex nested expression
        String sql3 = "((\"msg.timestamp\" <= 2000 AND \"hostname\" = 'uber2') OR \"status_code\" = 404)";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "((begin_timestamp <= 2000 AND TEXT_MATCH(\"__mergedTextIndex\", '/uber2:hostname/')) OR TEXT_MATCH(\"__mergedTextIndex\", '/404:status_code/'))");
    }

    /**
     * Test transformations at different scope levels.
     */
    @Test
    public void testDifferentScopes()
    {
        // Table-level scope
        String sql1 = "\"status_code\" = 200";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");

        // Schema-level scope
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default", sql1);
        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");

        // Catalog-level scope
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp", sql1);
        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
    }

    /**
     * Test that the filter provider is correctly instantiated.
     */
    @Test
    public void testConstructor()
    {
        assertNotNull(filterProvider);

        // Verify it's an instance of the parent classes
        assertTrue(filterProvider instanceof ClpPinotSplitFilterProvider);
        assertTrue(filterProvider instanceof ClpMySqlSplitFilterProvider);
        assertTrue(filterProvider instanceof ClpSplitFilterProvider);
    }

    /**
     * Test configuration is loaded correctly.
     */
    @Test
    public void testConfigurationLoaded()
    {
        // Simply verify that the provider was instantiated correctly with the config
        assertTrue(filterConfigPath.endsWith("test-pinot-split-filter.json"));
        assertNotNull(filterProvider);
    }

    /**
     * Test that non-equality expressions are not transformed to TEXT_MATCH.
     */
    @Test
    public void testNonEqualityNotTransformed()
    {
        // Greater than should not be transformed
        String sql1 = "\"status_code\" > 200";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "\"status_code\" > 200");

        // Less than should not be transformed
        String sql2 = "\"level\" < 5";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "\"level\" < 5");

        // Not equal should not be transformed
        String sql3 = "\"hostname\" != 'server1'";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "\"hostname\" != 'server1'");
    }

    /**
     * Test edge cases and special patterns.
     */
    @Test
    public void testEdgeCases()
    {
        // Test expression with no transformable parts
        String sql1 = "1 = 1";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "1 = 1");

        // Test column names with special characters (should still work if quoted properly)
        String sql2 = "\"column.with.dots\" = 'value'";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/value:column.with.dots/')");

        // Test multiple spaces in expression
        String sql3 = "\"status_code\"    =    200";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
    }
}
