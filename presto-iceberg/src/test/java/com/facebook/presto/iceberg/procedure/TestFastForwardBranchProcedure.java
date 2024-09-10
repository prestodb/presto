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

public class TestFastForwardBranchProcedure
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
    public void testFastForwardBranchUsingPositionalArgs()
    {
        String tableName = "fast_forward_table_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            table.refresh();

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            String fromBranch = "testBranch";
            String toBranch = "main";
            assertUpdate(format("CALL system.fast_forward('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, fromBranch, toBranch));

            // now testBranch branch should have 3 entries same as main
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testFastForwardBranchUsingNamedArgs()
    {
        String tableName = "fast_forward_table_arg_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            table.refresh();

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            String fromBranch = "testBranch";
            String toBranch = "main";
            assertUpdate(format("CALL system.fast_forward(schema => '%s', branch => '%s', to => '%s', table_name => '%s')",
                    TEST_SCHEMA, fromBranch, toBranch, tableName));

            // now testBranch branch should have 3 entries same as main
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testFastForwardWhenTargetIsNotAncestorFails()
    {
        String tableName = "fast_forward_table_fail_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch1").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch2").commit();
            table.refresh();

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch1' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch2' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            String fromBranch = "testBranch2";
            String toBranch = "testBranch1";
            // this should fail since fromBranch is not ancestor of toBranch
            assertQueryFails(format("CALL system.fast_forward('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, fromBranch, toBranch),
                    "Cannot fast-forward: testBranch2 is not an ancestor of testBranch1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidFastForwardBranchCases()
    {
        assertQueryFails("CALL system.fast_forward('test_table', branch => 'main', to => 'newBranch')",
                "line 1:1: Named and positional arguments cannot be mixed");
        assertQueryFails("CALL custom.fast_forward('test_table', 'main', 'newBranch')",
                "Procedure not registered: custom.fast_forward");
        assertQueryFails("CALL system.fast_forward('test_table', 'main')",
                "line 1:1: Required procedure argument 'branch' is missing");
        assertQueryFails("CALL system.fast_forward('', 'main', 'newBranch')",
                "line 1:1: Required procedure argument 'to' is missing");
    }

    @Test
    public void testFastForwardNonExistingToRefFails()
    {
        String tableName = "sample_table";
        createTable(tableName);
        try {
            String fromBranch = "main";
            String toBranch = "non_existing_branch";
            assertQueryFails(format("CALL system.fast_forward(branch => '%s', to => '%s', table_name => '%s', schema => '%s')",
                            fromBranch, toBranch, tableName, TEST_SCHEMA),
                    "Ref does not exist: non_existing_branch");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testFastForwardNonMain()
    {
        String tableName = "fast_forward_table_nonmain_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch1").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch2").commit();
            table.refresh();

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch1' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch2' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            String fromBranch = "testBranch1";
            String toBranch = "testBranch2";
            assertUpdate(format("CALL system.fast_forward('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, fromBranch, toBranch));

            // now testBranch1 branch should have 3 entries same as testBranch2
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch1' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testFastForwardNonExistingBranch()
    {
        String tableName = "fast_forward_table_non_existing_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            table.manageSnapshots().createBranch("testBranch1").commit();
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1); // main branch here
            table.refresh();

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'testBranch1' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");

            String fromBranch = "non_existing_branch"; // non existing branch
            String toBranch = "main";
            assertUpdate(format("CALL system.fast_forward('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, fromBranch, toBranch));

            // New branch non_existing_branch would be created and it should have 3 entries same as main branch
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'non_existing_branch' ORDER BY id", " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            String fromBranch1 = "non_existing_branch1"; // non existing branch
            String toBranch1 = "testBranch1";
            assertUpdate(format("CALL system.fast_forward('%s', '%s', '%s', '%s')", TEST_SCHEMA, tableName, fromBranch1, toBranch1));

            // New branch non_existing_branch1 would be created and it should have 2 entries same as testBranch1 branch
            assertQuery("SELECT * FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'non_existing_branch1' ORDER BY id", " VALUES (1, 'a'), (2, 'b')");
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
