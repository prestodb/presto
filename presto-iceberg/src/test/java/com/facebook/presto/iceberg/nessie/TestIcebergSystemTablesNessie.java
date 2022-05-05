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

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.iceberg.TestIcebergSystemTables;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
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

        Path catalogDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").resolve("catalog");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .putAll(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .put("iceberg.catalog.warehouse", catalogDir.getParent().toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        return queryRunner;
    }

    @Test
    @Override
    public void testPropertiesTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$properties\"",
                "VALUES ('key', 'varchar', '', '')," + "('value', 'varchar', '', '')");
        assertQuery("SELECT COUNT(*) FROM test_schema.\"test_table$properties\"", "VALUES 2");
        List<MaterializedRow> materializedRows = computeActual(getSession(),
                "SELECT * FROM test_schema.\"test_table$properties\"").getMaterializedRows();

        // nessie writes a "nessie.commit.id" to the table properties
        assertThat(materializedRows).hasSize(2);
        assertThat(materializedRows)
            .anySatisfy(row -> assertThat(row)
                .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.format.default", "PARQUET")))
            .anySatisfy(row -> assertThat(row.getField(0)).isEqualTo("nessie.commit.id"));
    }
}
