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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.TestIcebergSystemTables;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSystemTablesNessie
        extends TestIcebergSystemTables
{
    private NessieContainer nessieContainer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
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
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, "NESSIE", new IcebergConfig().getFileFormat(), false);

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", String.valueOf(NESSIE))
                .putAll(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .put("iceberg.catalog.warehouse", catalogDirectory.getParent().toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        return queryRunner;
    }

    @Override
    protected void checkTableProperties(String tableName, String deleteMode)
    {
        assertQuery(String.format("SHOW COLUMNS FROM test_schema.\"%s$properties\"", tableName),
                "VALUES " +
                        "('key', 'varchar', '', '', null, null, 2147483647)," +
                        "('value', 'varchar', '', '', null, null, 2147483647)");
        assertQuery(String.format("SELECT COUNT(*) FROM test_schema.\"%s$properties\"", tableName), "VALUES 11");
        List<MaterializedRow> materializedRows = computeActual(getSession(),
                String.format("SELECT * FROM test_schema.\"%s$properties\"", tableName)).getMaterializedRows();

        assertThat(materializedRows).hasSize(11);
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.delete.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.format.default", "PARQUET")))
                .anySatisfy(row -> assertThat(row.getField(0)).isEqualTo("nessie.commit.id"))
                .anySatisfy(row -> assertThat(row).isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "gc.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.parquet.compression-codec", "GZIP")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.delete-after-commit.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "commit.retry.num-retries", "4")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.previous-versions-max", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.metrics.max-inferred-column-defaults", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, IcebergTableProperties.TARGET_SPLIT_SIZE, Long.toString(DataSize.valueOf("128MB").toBytes()))));
    }

    @Override
    protected void checkORCFormatTableProperties(String tableName, String deleteMode)
    {
        assertQuery(String.format("SHOW COLUMNS FROM test_schema.\"%s$properties\"", tableName),
                "VALUES " +
                        "('key', 'varchar', '', '', null, null, 2147483647)," +
                        "('value', 'varchar', '', '', null, null, 2147483647)");

        assertQuery(String.format("SELECT COUNT(*) FROM test_schema.\"%s$properties\"", tableName), "VALUES 12");
        List<MaterializedRow> materializedRows = computeActual(getSession(),
                String.format("SELECT * FROM test_schema.\"%s$properties\"", tableName)).getMaterializedRows();

        assertThat(materializedRows).hasSize(12);
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.delete.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.format.default", "ORC")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.orc.compression-codec", "ZLIB")))
                .anySatisfy(row -> assertThat(row.getField(0)).isEqualTo("nessie.commit.id"))
                .anySatisfy(row -> assertThat(row).isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "gc.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.parquet.compression-codec", "zstd")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.delete-after-commit.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "commit.retry.num-retries", "4")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.previous-versions-max", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.metrics.max-inferred-column-defaults", "100")));
    }
}
