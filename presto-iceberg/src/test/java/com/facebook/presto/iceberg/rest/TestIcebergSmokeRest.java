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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.nessie.NessieConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test
public class TestIcebergSmokeRest
        extends IcebergDistributedSmokeTestBase
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    public TestIcebergSmokeRest()
    {
        super(REST);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        return format("%s/%s/%s", warehouseLocation, schema, table);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(
                ImmutableMap.of(),
                restConnectorProperties(serverUri),
                PARQUET,
                true,
                false,
                OptionalInt.empty(),
                Optional.of(warehouseLocation.toPath()));
    }

    protected IcebergResourceFactory getResourceFactory()
    {
        IcebergConfig icebergConfig = new IcebergConfig()
                .setCatalogType(REST)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath().toString());
        IcebergRestConfig restConfig = new IcebergRestConfig().setServerUri(serverUri);

        return new IcebergResourceFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new NessieConfig(),
                restConfig,
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                new NodeVersion("test_version"));
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        return getNativeIcebergTable(getResourceFactory(),
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    @Test
    public void testDeleteOnPartitionedV1Table()
    {
        // v1 table create fails due to Iceberg REST catalog bug (see: https://github.com/apache/iceberg/issues/8756)
        assertThatThrownBy(super::testDeleteOnPartitionedV1Table)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Cannot downgrade v2 table to v1");
    }

    @Test
    public void testCreateTableWithFormatVersion()
    {
        // v1 table create fails due to Iceberg REST catalog bug (see: https://github.com/apache/iceberg/issues/8756)
        assertThatThrownBy(() -> super.testMetadataDeleteOnNonIdentityPartitionColumn("1", "copy-on-write"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Cannot downgrade v2 table to v1");

        // v2 succeeds
        super.testCreateTableWithFormatVersion("2", "merge-on-read");
    }

    @Test(dataProvider = "version_and_mode")
    public void testMetadataDeleteOnNonIdentityPartitionColumn(String version, String mode)
    {
        if (version.equals("1")) {
            // v1 table create fails due to Iceberg REST catalog bug (see: https://github.com/apache/iceberg/issues/8756)
            assertThatThrownBy(() -> super.testMetadataDeleteOnNonIdentityPartitionColumn(version, mode))
                    .isInstanceOf(RuntimeException.class);
        }
        else {
            // v2 succeeds
            super.testMetadataDeleteOnNonIdentityPartitionColumn(version, mode);
        }
    }
}
