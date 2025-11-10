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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpPinotSplitFilterProvider
{
    private String filterConfigPath;
    private ClpPinotSplitFilterProvider filterProvider;

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
        filterProvider = new ClpPinotSplitFilterProvider(config);
    }

    /**
     * Test that Pinot provider correctly inherits MySQL range mapping functionality.
     * Verifies that range comparisons are transformed according to the configuration.
     */
    @Test
    public void testRangeMappingInheritance()
    {
        // Test greater than or equal
        String sql1 = "\"msg.timestamp\" >= 1234";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "end_timestamp >= 1234");

        // Test less than or equal
        String sql2 = "\"msg.timestamp\" <= 5678";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "begin_timestamp <= 5678");

        // Test equality (transforms to range check)
        String sql3 = "\"msg.timestamp\" = 4567";
        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
        assertEquals(result3, "(begin_timestamp <= 4567 AND end_timestamp >= 4567)");
    }

    /**
     * Test that expressions without range mappings pass through unchanged.
     */
    @Test
    public void testNonRangeMappedColumns()
    {
        // Test that non-mapped columns are not transformed
        String sql1 = "\"status_code\" = 200";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "\"status_code\" = 200");

        String sql2 = "\"hostname\" = 'server1'";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "\"hostname\" = 'server1'");
    }

    /**
     * Test complex expressions with multiple predicates.
     */
    @Test
    public void testComplexExpressions()
    {
        // Test AND condition with range mapping
        String sql1 = "(\"msg.timestamp\" >= 1000 AND \"msg.timestamp\" <= 2000)";
        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
        assertEquals(result1, "(end_timestamp >= 1000 AND begin_timestamp <= 2000)");

        // Test mixed conditions
        String sql2 = "(\"msg.timestamp\" = 1500 AND \"status_code\" = 200)";
        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
        assertEquals(result2, "((begin_timestamp <= 1500 AND end_timestamp >= 1500) AND \"status_code\" = 200)");
    }

    /**
     * Test that remapColumnName correctly returns mapped column names.
     */
    @Test
    public void testRemapColumnName()
    {
        // Test range-mapped column
        List<String> mappedColumns = filterProvider.remapColumnName("clp.default.table_1", "msg.timestamp");
        assertEquals(mappedColumns, ImmutableList.of("begin_timestamp", "end_timestamp"));

        // Test non-mapped column
        List<String> unmappedColumns = filterProvider.remapColumnName("clp.default.table_1", "status_code");
        assertEquals(unmappedColumns, ImmutableList.of("status_code"));
    }

    /**
     * Test table-level configuration override.
     */
    @Test
    public void testTableLevelOverride()
    {
        // Test table_2 specific mapping
        String sql = "\"table2_column\" >= 100";
        String result = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_2", sql);
        assertEquals(result, "table2_upper >= 100");
    }

    /**
     * Test schema-level configuration.
     */
    @Test
    public void testSchemaLevelMapping()
    {
        // Test schema-level mapping applies to tables
        String sql = "\"schema_column\" <= 500";
        String result = filterProvider.remapSplitFilterPushDownExpression("clp.schema1.any_table", sql);
        assertEquals(result, "schema_lower <= 500");
    }

    /**
     * Test that configuration is correctly loaded.
     */
    @Test
    public void testConfigurationLoaded()
    {
        // Simply verify that the provider was instantiated correctly with the config
        assertTrue(filterConfigPath.endsWith("test-pinot-split-filter.json"));
    }
}
