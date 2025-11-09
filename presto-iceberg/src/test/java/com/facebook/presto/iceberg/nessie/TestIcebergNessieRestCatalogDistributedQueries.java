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
package com.facebook.presto.iceberg.nessie;

import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.container.IcebergMinIODataLake;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.MinIOContainer;
import com.facebook.presto.testing.containers.NessieContainer;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.ACCESS_KEY;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.SECRET_KEY;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestIcebergNessieRestCatalogDistributedQueries
        extends TestIcebergNessieCatalogDistributedQueries
{
    static final String WAREHOUSE_DATA_DIR = "warehouse_data/";
    private NessieContainer nessieContainer;
    private IcebergMinIODataLake dockerizedS3DataLake;
    private String bucketName;
    HostAndPort minioApiEndpoint;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        Network network = Network.newNetwork();

        bucketName = "fornessie-" + randomTableSuffix();
        dockerizedS3DataLake = new IcebergMinIODataLake(bucketName, WAREHOUSE_DATA_DIR, network);
        dockerizedS3DataLake.start();
        minioApiEndpoint = dockerizedS3DataLake.getMinio().getMinioApiEndpoint();

        Map<String, String> envVars = ImmutableMap.<String, String>builder()
                .putAll(NessieContainer.DEFAULT_ENV_VARS)
                .put("NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ACCESS-KEY", "urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key")
                .put("NESSIE_CATALOG_SECRETS_ACCESS-KEY_NAME", ACCESS_KEY)
                .put("NESSIE_CATALOG_SECRETS_ACCESS-KEY_SECRET", SECRET_KEY)
                .put("NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_PATH_STYLE_ACCESS", "true")
                .put("NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ENDPOINT", format("http://%s:%s", MinIOContainer.DEFAULT_HOST_NAME, MinIOContainer.MINIO_API_PORT))
                .put("NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_REGION", Region.US_EAST_1.toString())
                .put("NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION", getCatalogDataDirectory().toString())
                .put("NESSIE_CATALOG_DEFAULT-WAREHOUSE", "warehouse")
                .put("NESSIE_CATALOG_SERVICE_S3.DEFAULT-OPTIONS_EXTERNAL-ENDPOINT", format("http://%s:%s", minioApiEndpoint.getHost(), minioApiEndpoint.getPort()))
                .buildOrThrow();
        nessieContainer = NessieContainer.builder().withEnvVars(envVars).withNetwork(network).build();
        nessieContainer.start();

        super.init();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void tearDown()
    {
        super.tearDown();
        if (nessieContainer != null) {
            nessieContainer.stop();
        }
        if (dockerizedS3DataLake != null) {
            dockerizedS3DataLake.stop();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest.uri", nessieContainer.getIcebergRestUri())
                .put("iceberg.catalog.warehouse", getCatalogDataDirectory().toString())
                .put("hive.s3.aws-access-key", ACCESS_KEY)
                .put("hive.s3.aws-secret-key", SECRET_KEY)
                .put("hive.s3.endpoint", format("http://%s:%s", minioApiEndpoint.getHost(), minioApiEndpoint.getPort()))
                .put("hive.s3.path-style-access", "true")
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(icebergProperties)
                .build().getQueryRunner();
    }

    protected org.apache.hadoop.fs.Path getCatalogDataDirectory()
    {
        return new org.apache.hadoop.fs.Path(URI.create(format("s3a://%s/%s", bucketName, WAREHOUSE_DATA_DIR)));
    }
}
