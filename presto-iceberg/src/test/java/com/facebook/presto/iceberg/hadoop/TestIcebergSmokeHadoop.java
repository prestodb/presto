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
package com.facebook.presto.iceberg.hadoop;

import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestIcebergSmokeHadoop
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHadoop()
    {
        super(HADOOP);
    }

    @Test
    public void testShowCreateTableWithSpecifiedWriteDataLocation()
            throws IOException
    {
        String tableName = "test_table_with_specified_write_data_location";
        String dataWriteLocation = java.nio.file.Files.createTempDirectory("test1").toAbsolutePath().toString();
        try {
            assertUpdate(format("CREATE TABLE %s(a int, b varchar) with (\"write.data.path\" = '%s')", tableName, dataWriteLocation));
            String schemaName = getSession().getSchema().get();
            String location = getLocation(schemaName, tableName);
            assertThat(computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo(format("CREATE TABLE iceberg.%s.%s (\n" +
                            "   \"a\" integer,\n" +
                            "   \"b\" varchar\n" +
                            ")\n" +
                            "WITH (\n" +
                            "   delete_mode = 'merge-on-read',\n" +
                            "   format = 'PARQUET',\n" +
                            "   format_version = '2',\n" +
                            "   location = '%s',\n" +
                            "   metadata_delete_after_commit = false,\n" +
                            "   metadata_previous_versions_max = 100,\n" +
                            "   metrics_max_inferred_column = 100,\n" +
                            "   \"write.data.path\" = '%s'\n" +
                            ")", schemaName, tableName, location, dataWriteLocation));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testTableWithSpecifiedWriteDataLocation()
            throws IOException
    {
        String tableName = "test_table_with_specified_write_data_location2";
        String dataWriteLocation = Files.createTempDirectory(tableName).toAbsolutePath().toString();
        try {
            assertUpdate(format("create table %s(a int, b varchar) with (\"write.data.path\" = '%s')", tableName, dataWriteLocation));
            assertUpdate(format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
            assertUpdate(format("delete from %s where a > 2", tableName), 1);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate("drop table if exists " + tableName);
        }
    }

    @Test
    public void testPartitionedTableWithSpecifiedWriteDataLocation()
            throws IOException
    {
        String tableName = "test_partitioned_table_with_specified_write_data_location";
        String dataWriteLocation = Files.createTempDirectory(tableName).toAbsolutePath().toString();
        try {
            assertUpdate(format("create table %s(a int, b varchar) with (partitioning = ARRAY['a'], \"write.data.path\" = '%s')", tableName, dataWriteLocation));
            assertUpdate(format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
            assertUpdate(format("delete from %s where a > 2", tableName), 1);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate("drop table if exists " + tableName);
        }
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        Path tempLocation = getCatalogDirectory();
        return format("%s%s/%s", tempLocation.toUri(), schema, table);
    }

    @Override
    protected Path getCatalogDirectory()
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = new Path(getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false).toFile().toURI());
        return catalogDirectory;
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(HADOOP);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toString());

        IcebergNativeCatalogFactory catalogFactory = new IcebergNativeCatalogFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()));

        return IcebergUtil.getNativeIcebergTable(catalogFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }
}
