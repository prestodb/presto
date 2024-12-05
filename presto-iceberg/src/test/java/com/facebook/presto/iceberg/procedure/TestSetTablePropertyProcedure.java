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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSetTablePropertyProcedure
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "test_hadoop";
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), HADOOP, ImmutableMap.of());
    }

    public void createTable(String tableName)
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id integer, value VARCHAR)");
    }

    public void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    public void testSetTablePropertyProcedurePositionalArgs()
    {
        String tableName = "table_property_table_test";
        createTable(tableName);
        try {
            String propertyKey = "read.split.target-size";
            String propertyValue = "268435456";
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            assertEquals(table.properties().size(), 7);
            assertEquals(table.properties().get(propertyKey), null);

            assertUpdate(format("CALL system.set_table_property('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, propertyKey, propertyValue));
            table.refresh();

            // now the table property read.split.target-size should have new value
            assertEquals(table.properties().size(), 8);
            assertEquals(table.properties().get(propertyKey), propertyValue);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetTablePropertyProcedureNamedArgs()
    {
        String tableName = "table_property_table_arg_test";
        createTable(tableName);
        try {
            String propertyKey = "read.split.target-size";
            String propertyValue = "268435456";
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            assertEquals(table.properties().size(), 7);
            assertEquals(table.properties().get(propertyKey), null);

            assertUpdate(format("CALL system.set_table_property(schema => '%s', key => '%s', value => '%s', table_name => '%s')",
                    TEST_SCHEMA, propertyKey, propertyValue, tableName));
            table.refresh();

            // now the table property read.split.target-size should have new value
            assertEquals(table.properties().size(), 8);
            assertEquals(table.properties().get(propertyKey), propertyValue);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetTablePropertyProcedureUpdateExisting()
    {
        String tableName = "table_property_table_test_update";
        createTable(tableName);
        try {
            String propertyKey = "commit.retry.num-retries";
            String propertyValue = "10";

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            assertEquals(table.properties().size(), 7);
            assertEquals(table.properties().get(propertyKey), "4");

            assertUpdate(format("CALL system.set_table_property('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, propertyKey, propertyValue));
            table.refresh();

            // now the table property commit.retry.num-retries should have new value
            assertEquals(table.properties().size(), 7);
            assertEquals(table.properties().get(propertyKey), propertyValue);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidSetTablePropertyProcedureCases()
    {
        assertQueryFails("CALL system.set_table_property('test_table', key => 'key', value => 'value')",
                "line 1:1: Named and positional arguments cannot be mixed");
        assertQueryFails("CALL custom.set_table_property('test_table', 'key', 'value')",
                "Procedure not registered: custom.set_table_property");
        assertQueryFails("CALL system.set_table_property('schema', 'tablename', 'key')",
                "line 1:1: Required procedure argument 'value' is missing");
        assertQueryFails("CALL system.set_table_property('', 'main', 'key')",
                "line 1:1: Required procedure argument 'value' is missing");
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }
}
