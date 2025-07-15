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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Set;

import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpMetadataFilterConfig
{
    private String filterConfigPath;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-metadata-filter.json");
        if (resource == null) {
            throw new FileNotFoundException("test-metadata-filter.json not found in resources");
        }

        filterConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
    }

    @Test
    public void checkRequiredFilters()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);
        SchemaTableName testTableSchemaTableName = new SchemaTableName("default", "table_1");
        assertThrows(PrestoException.class, () -> filterProvider.checkContainsRequiredFilters(
                testTableSchemaTableName,
                "(\"level\" >= 1 AND \"level\" <= 3)"));
        filterProvider.checkContainsRequiredFilters(
                testTableSchemaTableName,
                "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" < 5678)");
    }

    @Test
    public void getFilterNames()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);
        Set<String> catalogFilterNames = filterProvider.getColumnNames("clp");
        assertEquals(ImmutableSet.of("level"), catalogFilterNames);
        Set<String> schemaFilterNames = filterProvider.getColumnNames("clp.default");
        assertEquals(ImmutableSet.of("level", "author"), schemaFilterNames);
        Set<String> tableFilterNames = filterProvider.getColumnNames("clp.default.table_1");
        assertEquals(ImmutableSet.of("level", "author", "msg.timestamp", "file_name"), tableFilterNames);
    }

    @Test
    public void handleEmptyAndInvalidMetadataFilterConfig()
    {
        ClpConfig config = new ClpConfig();

        // Empty config
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);
        assertTrue(filterProvider.getColumnNames("clp").isEmpty());
        assertTrue(filterProvider.getColumnNames("abc.xyz").isEmpty());
        assertTrue(filterProvider.getColumnNames("abc.opq.xyz").isEmpty());

        // Invalid config
        config.setMetadataFilterConfig(randomUUID().toString());
        assertThrows(PrestoException.class, () -> new ClpMetadataFilterProvider(config));
    }

    @Test
    public void remapSql()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);

        String metadataFilterSql1 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql1 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql1);
        assertEquals(remappedSql1, "(end_timestamp > 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql2 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql2 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql2);
        assertEquals(remappedSql2, "(end_timestamp >= 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql3 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql3 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql3);
        assertEquals(remappedSql3, "(end_timestamp > 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql4 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql4 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql4);
        assertEquals(remappedSql4, "(end_timestamp >= 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql5 = "(\"msg.timestamp\" = 1234)";
        String remappedSql5 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql5);
        assertEquals(remappedSql5, "((begin_timestamp <= 1234 AND end_timestamp >= 1234))");
    }
}
