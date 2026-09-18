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
import com.facebook.presto.iceberg.AbstractTestIcebergAnalyzeUppercaseColumns;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.apache.iceberg.catalog.Catalog;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

/**
 * E2E test verifying that ANALYZE succeeds on Iceberg tables whose schemas were written
 * by an external engine (e.g. Spark on Hadoop) with uppercase column names, then
 * registered into a Presto REST catalog via the {@code register_table} procedure.
 * Covers both unpartitioned and identity-partitioned tables; the partitioned variant
 * exercises mixed-case partition values (all-upper, all-lower, mixed).
 */
@Test
public class TestIcebergRestAnalyzeUppercaseColumns
        extends AbstractTestIcebergAnalyzeUppercaseColumns
{
    private static final String TABLE_NAME = "test_rest_uppercase_columns";
    private static final String PARTITIONED_TABLE = "test_rest_uppercase_columns_partitioned";

    private File restWarehouseLocation;
    private File hadoopWarehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @Override
    protected String getTableName()
    {
        return TABLE_NAME;
    }

    @Override
    protected String getPartitionedTableName()
    {
        return PARTITIONED_TABLE;
    }

    @Override
    protected File getHadoopWarehouseLocation()
    {
        return hadoopWarehouseLocation;
    }

    @BeforeClass
    public void init()
            throws Exception
    {
        // Separate temp dirs: REST catalog warehouse vs. the "external" Hadoop warehouse
        restWarehouseLocation = Files.newTemporaryFolder();
        hadoopWarehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(restWarehouseLocation.getAbsolutePath());
        restServer.start();
        serverUri = restServer.getBaseUrl().toString();

        super.init();
        assertQuerySucceeds("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);

        Catalog hadoopCatalog = loadHadoopCatalog();
        setupUnpartitionedTable(hadoopCatalog, TABLE_NAME);
        setupPartitionedTable(hadoopCatalog, PARTITIONED_TABLE);
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        unregisterTableQuietly(TABLE_NAME);
        unregisterTableQuietly(PARTITIONED_TABLE);
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(restWarehouseLocation.toPath(), ALLOW_INSECURE);
        deleteRecursively(hadoopWarehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(restWarehouseLocation.toPath()))
                .build()
                .getQueryRunner();
    }
}
