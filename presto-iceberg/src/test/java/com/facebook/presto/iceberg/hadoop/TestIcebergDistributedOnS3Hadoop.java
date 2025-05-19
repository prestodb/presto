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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.container.IcebergMinIODataLake;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.net.URI;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.ACCESS_KEY;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.SECRET_KEY;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

public class TestIcebergDistributedOnS3Hadoop
        extends IcebergDistributedTestBase
{
    static final String WAREHOUSE_DATA_DIR = "warehouse_data/";
    final String bucketName;
    final String catalogWarehouseDir;
    private IcebergMinIODataLake dockerizedS3DataLake;
    HostAndPort hostAndPort;

    public TestIcebergDistributedOnS3Hadoop()
            throws IOException
    {
        super(HADOOP);
        bucketName = "forhadoop-" + randomTableSuffix();
        catalogWarehouseDir = createTempDirectory(bucketName).toUri().toString();
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "iceberg.catalog.warehouse", catalogWarehouseDir,
                        "iceberg.catalog.hadoop.warehouse.datadir", getCatalogDataDirectory().toString(),
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
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (dockerizedS3DataLake != null) {
            dockerizedS3DataLake.stop();
        }
    }

    @Override
    public void testCreateTableWithCustomLocation()
            throws IOException
    {
        String tableName = "test_hadoop_table_with_custom_location";
        URI tableTargetURI = createTempDirectory(tableName).toUri();
        assertQueryFails(format("create table %s (a int, b varchar)" + " with (location = '%s')", tableName, tableTargetURI.toString()),
                "Cannot set a custom location for a path-based table.*");
    }

    protected Path getCatalogDataDirectory()
    {
        return new Path(URI.create(format("s3://%s/%s", bucketName, WAREHOUSE_DATA_DIR)));
    }

    protected Path getCatalogDirectory()
    {
        return new Path(catalogWarehouseDir);
    }

    protected HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HiveS3Config hiveS3Config = new HiveS3Config()
                .setS3AwsAccessKey(ACCESS_KEY)
                .setS3AwsSecretKey(SECRET_KEY)
                .setS3PathStyleAccess(true)
                .setS3Endpoint(format("http://%s:%s", hostAndPort.getHost(), hostAndPort.getPort()));
        return getHdfsEnvironment(hiveClientConfig, metastoreClientConfig, hiveS3Config);
    }

    protected Table loadTable(String tableName)
    {
        Configuration configuration = getHdfsEnvironment().getConfiguration(new HdfsContext(SESSION), getCatalogDataDirectory());
        Catalog catalog = CatalogUtil.loadCatalog(HADOOP.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), configuration);
        return catalog.loadTable(TableIdentifier.of("tpch", tableName));
    }
}
