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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestIcebergMetadataListing
        extends AbstractTestQueryFramework
{
    private static final int TEST_TIMEOUT = 10_000;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        ImmutableMap.of("hive", new SelectedRole(ROLE, Optional.of("admin"))),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path catalogDirectory = getIcebergDataDirectoryPath(queryRunner.getCoordinator().getDataDirectory(), HIVE.name(), new IcebergConfig().getFileFormat(), false);

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("iceberg.hive.table-refresh.max-retry-time", "20s") // improves test time for testTableDropWithMissingMetadata
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        queryRunner.installPlugin(new HivePlugin("hive"));
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.security", "sql-standard")
                .build();

        queryRunner.createCatalog("hive", "hive", hiveProperties);

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_table2 (_double DOUBLE) WITH (partitioning = ARRAY['_double'])");
        assertQuerySucceeds("CREATE TABLE hive.test_schema.hive_table (_double DOUBLE)");
        assertQuerySucceeds("CREATE VIEW iceberg.test_schema.iceberg_view AS SELECT * FROM iceberg.test_schema.iceberg_table1");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive.test_schema.hive_table");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_table2");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_table1");
        assertQuerySucceeds("DROP VIEW IF EXISTS iceberg.test_schema.iceberg_view");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive.test_schema");
    }

    @Test
    public void testTableListing()
    {
        // For now, iceberg connector will show all the tables(iceberg and non-iceberg) under a schema.
        assertQuery("SHOW TABLES FROM iceberg.test_schema", "VALUES 'iceberg_table1', 'iceberg_table2', 'hive_table', 'iceberg_view'");
    }

    @Test
    public void testTableColumnListing()
    {
        // Verify information_schema.columns does not include columns from non-Iceberg tables
        assertQuery("SELECT table_name, column_name FROM iceberg.information_schema.columns WHERE table_schema = 'test_schema'",
                "VALUES ('iceberg_table1', '_string'), ('iceberg_table1', '_integer'), ('iceberg_table2', '_double'), " +
                        "('iceberg_view', '_string'), ('iceberg_view', '_integer')");
    }

    @Test
    public void testTableDescribing()
    {
        assertQuery("DESCRIBE iceberg.test_schema.iceberg_table1", "VALUES ('_string', 'varchar', '', '', null, null, 2147483647L), ('_integer', 'integer', '', '', 10l, null, null)");
    }

    /*
     * The property iceberg.hive.table-refresh.max-retry-time is important for controlling the maximum retry duration
     * when refreshing Iceberg table metadata. If this test fails, check the refreshFromMetadataLocation method
     * in HiveTableOperations.
     */
    @Test(timeOut = TEST_TIMEOUT)
    public void testTableDropWithMissingMetadata()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.test_metadata_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_metadata_schema.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        assertQuerySucceeds("CREATE TABLE iceberg.test_metadata_schema.iceberg_table2 (_string VARCHAR, _integer INTEGER)");
        assertQuery("SHOW TABLES FROM iceberg.test_metadata_schema", "VALUES 'iceberg_table1', 'iceberg_table2'");

        Path dataDirectory = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory();
        Path icebergDataDirectory = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false);
        File tableMetadataDir = icebergDataDirectory.resolve("test_metadata_schema").resolve("iceberg_table1").resolve("metadata").toFile();
        for (File file : tableMetadataDir.listFiles()) {
            file.delete();
        }
        tableMetadataDir.delete();

        assertQueryFails("SELECT * FROM iceberg.test_metadata_schema.iceberg_table1", "Table metadata is missing");
        assertQuerySucceeds("DROP TABLE iceberg.test_metadata_schema.iceberg_table1");
        assertQuery("SHOW TABLES FROM iceberg.test_metadata_schema", "VALUES 'iceberg_table2'");
    }

    @Test
    public void testTableValidation()
    {
        assertQuerySucceeds("SELECT * FROM iceberg.test_schema.iceberg_table1");
        assertQueryFails("SELECT * FROM iceberg.test_schema.hive_table", "Not an Iceberg table: test_schema.hive_table");
    }
}
