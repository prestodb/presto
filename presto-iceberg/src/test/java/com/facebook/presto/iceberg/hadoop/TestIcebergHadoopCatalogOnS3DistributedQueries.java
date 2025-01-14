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

import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.TestIcebergDistributedQueries;
import com.facebook.presto.iceberg.container.IcebergMinIODataLake;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.ACCESS_KEY;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.SECRET_KEY;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestIcebergHadoopCatalogOnS3DistributedQueries
        extends TestIcebergDistributedQueries
{
    static final String WAREHOUSE_DIR = "warehouse/";
    String bucketName = "test-iceberg-hadoop-s3-" + randomTableSuffix();
    private IcebergMinIODataLake dockerizedS3DataLake;
    HostAndPort hostAndPort;

    public TestIcebergHadoopCatalogOnS3DistributedQueries()
    {
        super(HADOOP);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        this.dockerizedS3DataLake = new IcebergMinIODataLake(bucketName, WAREHOUSE_DIR);
        this.dockerizedS3DataLake.start();
        hostAndPort = this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint();
        super.init();
    }

    @AfterClass
    public void tearDown()
    {
        if (dockerizedS3DataLake != null) {
            dockerizedS3DataLake.stop();
        }
    }

    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testRenameTable()
    {
        // Rename table are not supported by hadoop catalog
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), HADOOP,
                ImmutableMap.of(
                        "iceberg.catalog.warehouse", format("s3://%s/%s", bucketName, WAREHOUSE_DIR),
                        "hive.s3.use-instance-credentials", "false",
                        "hive.s3.aws-access-key", ACCESS_KEY,
                        "hive.s3.aws-secret-key", SECRET_KEY,
                        "hive.s3.endpoint", format("http://%s:%s", hostAndPort.getHost(), hostAndPort.getPort()),
                        "hive.s3.path-style-access", "true"));
    }
}
