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
import com.facebook.presto.iceberg.IcebergQueryRunner;
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
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static java.util.regex.Pattern.quote;

public class TestSetCurrentSnapshotProcedure
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), HADOOP, ImmutableMap.of());
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
    public void testSetCurrentSnapshotUsingPositionalArgs()
    {
        String tableName = "test_current_snapshot_table";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            Table table = loadTable(tableName);

            long snapShotIdv1 = table.currentSnapshot().snapshotId();
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            assertUpdate(format("CALL system.set_current_snapshot('%s', '%s', %d)", TEST_SCHEMA, tableName, snapShotIdv1));
            // now current table will have only 1 row same as snapShotIdv1
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetCurrentSnapshotUsingNamedArgs()
    {
        String tableName = "test_named_arg_table";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            Table table = loadTable(tableName);

            long snapShotIdv1 = table.currentSnapshot().snapshotId();
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            table.refresh();
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            assertUpdate(format("CALL system.set_current_snapshot(snapshot_id => %d, table_name => '%s', schema => '%s')",
                    snapShotIdv1, tableName, TEST_SCHEMA));
            // now current table will have only 1 row same as snapShotIdv1
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetCurrentSnapshotToInvalidSnapshot()
    {
        String tableName = "invalid_test_table";
        createTable(tableName);
        try {
            assertQueryFails(format("CALL system.set_current_snapshot(snapshot_id => %d, table_name => '%s', schema => '%s')",
                            -1L, tableName, TEST_SCHEMA),
                    "Cannot roll back to unknown snapshot id: -1");
            assertQueryFails(format("CALL system.set_current_snapshot('%s', '%s', %d)", TEST_SCHEMA, tableName, -1L),
                    "Cannot roll back to unknown snapshot id: -1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidRollbackToSnapshotCases()
    {
        assertQueryFails(format("CALL system.set_current_snapshot(schema => '%s', table_name => '%s', %d)",
                        TEST_SCHEMA, "tableName", 1),
                "line 1:1: Named and positional arguments cannot be mixed");
        assertQueryFails("CALL custom.set_current_snapshot('test_schema', 'test_table', 1)",
                "Procedure not registered: custom.set_current_snapshot");
        assertQueryFails("CALL system.set_current_snapshot('', 'test_table', 1)",
                "schemaName is empty");
        assertQueryFails("CALL system.set_current_snapshot('test_schema', '', 1)",
                "tableName is empty");
        assertQueryFails("CALL system.set_current_snapshot('test_schema', 'test_table')",
                "Either snapshot_id or reference must be provided, not both");
        assertQueryFails("CALL system.set_current_snapshot('test_schema', 'test_table', 1, 'branch1')",
                "Either snapshot_id or reference must be provided, not both");
        assertQueryFails("CALL system.set_current_snapshot('schema_name', 'table_name', 'branch1')",
                quote("line 1:63: Cannot cast type varchar(7) to bigint"));
    }

    @Test
    public void testSetCurrentSnapshotToRef()
    {
        String tableName = "test_set_snapshot_ref_table";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            Table table = loadTable(tableName);

            long snapShotIdv1 = table.currentSnapshot().snapshotId();
            table.manageSnapshots().createBranch("ref1", snapShotIdv1).commit();
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            table.refresh();
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            assertUpdate(format("CALL system.set_current_snapshot(ref => '%s', table_name => '%s', schema => '%s')",
                    "ref1", tableName, TEST_SCHEMA));
            // now current table will have only 1 row same as ref1
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
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
