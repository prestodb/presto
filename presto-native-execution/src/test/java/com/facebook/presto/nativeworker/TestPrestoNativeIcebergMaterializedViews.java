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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.iceberg.TestIcebergMaterializedViewsBase;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

@Test(singleThreaded = true)
public class TestPrestoNativeIcebergMaterializedViews
        extends TestIcebergMaterializedViewsBase
{
    private TestingHttpServer restServer;
    private String serverUri;

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
        getQueryRunner().execute("CREATE SCHEMA IF NOT EXISTS test_schema");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(REST)
                .setSchemaName("test_schema")
                .setDataDirectory(warehouseLocation.toPath())
                .setExtraConnectorProperty("iceberg.rest.uri", serverUri)
                .setExtraProperty("experimental.legacy-materialized-views", "false")
                .setExtraProperty("materialized-view-default-refresh-type", "INCREMENTAL")
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(REST)
                .setSchemaName("test_schema")
                .setDataDirectory(warehouseLocation.toPath())
                .setExtraConnectorProperty("iceberg.rest.uri", serverUri)
                .setExtraProperty("experimental.legacy-materialized-views", "false")
                .setExtraProperty("materialized-view-default-refresh-type", "INCREMENTAL")
                .build();
    }

    @Override
    @Test
    public void testMaterializedViewStitchingForTimestamp()
    {
        // Disabled: Velox does not support timestamp partition filters in stitching scans
    }

    @Test
    public void testRefreshMaterializedViewWithTransformPartitioning()
    {
        assertUpdate("CREATE TABLE test_mv_transform_base (id BIGINT, value BIGINT, dt VARCHAR) " +
                "WITH (\"format-version\" = '3', partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_mv_transform_base VALUES (1, 15, '2024-01-01'), (2, 27, '2024-01-02')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_transform " +
                "WITH (\"format-version\" = '3', partitioning = ARRAY['dt', 'bucket(id, 4)', 'truncate(value, 10)']) " +
                "AS SELECT id, value, dt FROM test_mv_transform_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_transform", 2);
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_transform\" ORDER BY id",
                "VALUES (1, 15, '2024-01-01'), (2, 27, '2024-01-02')");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_transform");
        assertUpdate("DROP TABLE test_mv_transform_base");
    }
}
