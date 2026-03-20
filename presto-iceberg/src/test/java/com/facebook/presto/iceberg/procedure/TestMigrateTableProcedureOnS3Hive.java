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

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.container.IcebergMinIODataLake;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.ACCESS_KEY;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.SECRET_KEY;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMigrateTableProcedureOnS3Hive
        extends AbstractTestQueryFramework
{
    static final String WAREHOUSE_DATA_DIR = "warehouse_data/";
    final String bucketName;
    final String catalogWarehouseDir;

    private IcebergMinIODataLake dockerizedS3DataLake;
    HostAndPort hostAndPort;

    public TestMigrateTableProcedureOnS3Hive()
            throws IOException
    {
        bucketName = "forhive-" + randomTableSuffix();
        catalogWarehouseDir = new Path(createTempDirectory(bucketName).toUri()).toString();
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "hive.metastore", "file",
                        "hive.metastore.catalog.dir", catalogWarehouseDir,
                        "hive.s3.aws-access-key", ACCESS_KEY,
                        "hive.s3.aws-secret-key", SECRET_KEY,
                        "hive.s3.endpoint", format("http://%s:%s", hostAndPort.getHost(), hostAndPort.getPort()),
                        "hive.s3.path-style-access", "true"))
                .build().getQueryRunner();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        this.dockerizedS3DataLake = new IcebergMinIODataLake(bucketName, WAREHOUSE_DATA_DIR);
        this.dockerizedS3DataLake.start();
        hostAndPort = this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint();
        super.init();
        getQueryRunner().execute("create schema if not exists iceberg.new_schema");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        getQueryRunner().execute("drop schema if exists iceberg.new_schema");
        if (dockerizedS3DataLake != null) {
            dockerizedS3DataLake.stop();
        }
    }

    @Test
    protected void testMigrateTableProcedure()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        try {
            assertUpdate("create table test_lineitem as select * from tpch.tiny.lineitem", 60175);
            assertQuery("select count(*) from test_lineitem", "values 60175");

            assertUpdate(format("call system.migrate_table('%s', '%s', '%s', '%s')",
                    schemaName, "test_lineitem", "new_schema.new_lineitem", getTargetDataDirectory() + "new_lineitem"), 60175);
            assertQuery("select count(*) from new_schema.new_lineitem", "values 60175");

            MaterializedResult propertiesResult1 = getQueryRunner().execute("select * from \"test_lineitem$properties\"");
            MaterializedResult propertiesResult2 = getQueryRunner().execute("select * from new_schema.\"new_lineitem$properties\"");
            assertEquals(propertiesResult1, propertiesResult2);

            for (Object filePath : computeActual("SELECT file_path from new_schema.\"new_lineitem$files\"").getOnlyColumnAsSet()) {
                assertTrue(String.valueOf(filePath).startsWith(getTargetDataDirectory() + "new_lineitem"));
            }
        }
        finally {
            assertUpdate("drop table if exists test_lineitem");
            assertUpdate("drop table if exists new_schema.new_lineitem");
        }
    }

    @Test
    protected void testMigrateTableProcedureForEmptyTable()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        try {
            assertUpdate("create table test_empty_table(a int, b varchar)");
            assertQuery("select count(*) from test_empty_table", "values 0");

            assertUpdate(format("call system.migrate_table('%s', '%s', '%s', '%s')",
                    schemaName, "test_empty_table", "new_schema.new_empty_table", getTargetDataDirectory() + "new_empty_table"), 0);
            assertQuery("select count(*) from new_schema.new_empty_table", "values 0");

            MaterializedResult propertiesResult1 = getQueryRunner().execute("select * from \"test_empty_table$properties\"");
            MaterializedResult propertiesResult2 = getQueryRunner().execute("select * from new_schema.\"new_empty_table$properties\"");
            assertEquals(propertiesResult1, propertiesResult2);

            assertUpdate("insert into new_schema.new_empty_table values(1, '1001'), (2, '1002')", 2);
            assertUpdate("insert into new_schema.new_empty_table values(3, '1003'), (4, '1004')", 2);
            assertQuery("select * from new_schema.new_empty_table", "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
            for (Object filePath : computeActual("SELECT file_path from new_schema.\"new_empty_table$files\"").getOnlyColumnAsSet()) {
                assertTrue(String.valueOf(filePath).startsWith(getTargetDataDirectory() + "new_empty_table"));
            }
        }
        finally {
            assertUpdate("drop table if exists test_empty_table");
            assertUpdate("drop table if exists new_schema.new_empty_table");
        }
    }

    @Test
    protected void testMigrateTableProcedureForTableWithDeleteFiles()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        try {
            assertUpdate("create table test_table_with_delete_files(a int, b varchar)");
            assertUpdate("insert into test_table_with_delete_files values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')", 4);
            assertUpdate("delete from test_table_with_delete_files where a in (2, 4)", 2);
            assertQuery("select * from test_table_with_delete_files", "values(1, '1001'), (3, '1003')");

            assertUpdate(format("call system.migrate_table('%s', '%s', '%s', '%s')",
                    schemaName, "test_table_with_delete_files", "new_schema.new_table", getTargetDataDirectory() + "new_table"), 2);
            assertQuery("select * from new_schema.new_table", "values(1, '1001'), (3, '1003')");

            MaterializedResult propertiesResult1 = getQueryRunner().execute("select * from \"test_table_with_delete_files$properties\"");
            MaterializedResult propertiesResult2 = getQueryRunner().execute("select * from new_schema.\"new_table$properties\"");
            assertEquals(propertiesResult1, propertiesResult2);

            for (Object filePath : computeActual("SELECT file_path from new_schema.\"new_table$files\"").getOnlyColumnAsSet()) {
                assertTrue(String.valueOf(filePath).startsWith(getTargetDataDirectory() + "new_table"));
            }
        }
        finally {
            assertUpdate("drop table if exists test_table_with_delete_files");
            assertUpdate("drop table if exists new_schema.new_table");
        }
    }

    @Test
    protected void testMigrateTableProcedureToAlreadyExistsTargetTable()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        try {
            assertUpdate("create table test_source_table(a int, b varchar)");
            assertUpdate("insert into test_source_table values(1, '1001'), (2, '1002')", 2);
            assertUpdate("create table new_schema.new_target_table as select * from test_source_table", 2);

            assertQueryFails(format("call system.migrate_table('%s', '%s', '%s', '%s')",
                            schemaName, "test_source_table", "new_schema.new_target_table", getTargetDataDirectory() + "new_target_table"),
                    "Table already exists:.*");
        }
        finally {
            assertUpdate("drop table if exists test_source_table");
            assertUpdate("drop table if exists new_schema.new_target_table");
        }
    }

    private Path getTargetDataDirectory()
    {
        return new Path(URI.create(format("s3://%s/%s", bucketName, WAREHOUSE_DATA_DIR)));
    }
}
