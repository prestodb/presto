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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestJdbcClientNameMapping
{
    private JdbcClient jdbcClient;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        Map<String, List<String>> schemaTableNames = new TreeMap<>();

        schemaTableNames.put("schemaOne", Arrays.asList("MixedCaseTable1", "aTableTwo", "OneMoreTable", "lastone"));
        schemaTableNames.put("SchemaTwo", Arrays.asList("MixedCaseTable1", "aTableTwo", "OneMoreTable", "lastone"));
        schemaTableNames.put("schema_three", Arrays.asList("table_one", "table_two", "table_three"));

        jdbcClient = new BaseJdbcClient(
                new JdbcConnectorId("test"),
                new BaseJdbcConfig().setConnectionUrl(""),
                "\"",
                new TestingNameMappingDriver(schemaTableNames));
    }

    @Test
    public void testSchemaAndTableMapping()
    {
        assertTrue(jdbcClient.getSchemaNames().containsAll(ImmutableSet.of("schemaone", "schematwo", "schema_three")));

        assertEquals(jdbcClient.getTableNames("schemaone"), ImmutableList.of(
                new SchemaTableName("schemaone", "mixedcasetable1"),
                new SchemaTableName("schemaone", "atabletwo"),
                new SchemaTableName("schemaone", "onemoretable"),
                new SchemaTableName("schemaone", "lastone")));

        assertEquals(jdbcClient.getTableNames("schematwo"), ImmutableList.of(
                new SchemaTableName("schematwo", "mixedcasetable1"),
                new SchemaTableName("schematwo", "atabletwo"),
                new SchemaTableName("schematwo", "onemoretable"),
                new SchemaTableName("schematwo", "lastone")));

        assertEquals(jdbcClient.getTableNames("schema_three"), ImmutableList.of(
                new SchemaTableName("schema_three", "table_one"),
                new SchemaTableName("schema_three", "table_two"),
                new SchemaTableName("schema_three", "table_three")));

        SchemaTableName schemaTableName = new SchemaTableName("schemaone", "mixedcasetable1");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getSchemaName(), "schemaOne");
        assertEquals(table.getTableName(), "MixedCaseTable1");
        assertEquals(table.getSchemaTableName(), schemaTableName);
    }
}
