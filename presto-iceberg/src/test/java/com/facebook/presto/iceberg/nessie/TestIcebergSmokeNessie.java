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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.apache.iceberg.Table;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.DELAY_TABLE_PREFIX_FOR_TEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Test
public class TestIcebergSmokeNessie
        extends IcebergDistributedSmokeTestBase
{
    private static final Logger log = Logger.get(TestIcebergSmokeNessie.class);
    private NessieContainer nessieContainer;

    public TestIcebergSmokeNessie()
    {
        super(NESSIE);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
        super.init();
    }

    @AfterClass
    public void tearDown()
    {
        if (nessieContainer != null) {
            nessieContainer.stop();
        }
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        QueryRunner queryRunner = getQueryRunner();

        Path dataDirectory = ((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory();
        Path icebergDataDirectory = getIcebergDataDirectoryPath(dataDirectory, NESSIE.name(), new IcebergConfig().getFileFormat(), false);
        Optional<File> tempTableLocation = Arrays.stream(requireNonNull(icebergDataDirectory.resolve(schema).toFile().listFiles()))
                .filter(file -> endsWithTableUUID(table, file.toURI().toString())).findFirst();

        String dataLocation = icebergDataDirectory.toFile().toURI().toString();
        String relativeTableLocation = tempTableLocation.get().toURI().toString().replace(dataLocation, "");

        return format("%s%s", dataLocation, relativeTableLocation.substring(0, relativeTableLocation.length() - 1));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(NESSIE)
                .setExtraConnectorProperties(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .build().getQueryRunner();
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(NESSIE);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toString());

        IcebergNessieConfig nessieConfig = new IcebergNessieConfig().setServerUri(nessieContainer.getRestApiUri());

        IcebergNativeCatalogFactory catalogFactory = new IcebergNessieCatalogFactory(icebergConfig,
                nessieConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()));

        return IcebergUtil.getNativeIcebergTable(catalogFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    @Test
    public void testRegisterTableFailedOnDeletedSchema()
    {
        String schemaName = getSession().getSchema().get();
        String anotherSchemaName = "test_ano_schema";
        String sourceTableName = "source_table_1";
        String registerTableName = DELAY_TABLE_PREFIX_FOR_TEST + "t1";
        assertUpdate("CREATE SCHEMA " + anotherSchemaName);
        assertUpdate("CREATE TABLE " + sourceTableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES(1, 1)", 1);

        String metadataLocation = getLocation(schemaName, sourceTableName);

        new Thread(() -> {
            log.info("====> Nessie Smoke: start to register table: %s.%s", anotherSchemaName, registerTableName);
            assertQueryFails("CALL system.register_table('" + anotherSchemaName + "', '" + registerTableName + "', '" + metadataLocation + "')",
                    ".*");
        }).start();

        // Wait 1000ms to make sure that the schema drop operation occurs between the pre-check of schema existence
        // and the actual call of Iceberg's registerTable API
        try {
            Thread.sleep(1000);
        }
        catch (Exception e) {
            // ignored
        }
        log.info("====> Nessie Smoke: start to drop schema: %s", anotherSchemaName);
        assertUpdate("DROP SCHEMA " + anotherSchemaName);
        log.info("====> Nessie Smoke: complete drop schema: %s", anotherSchemaName);

        // Wait another 2000ms to ensure that the Iceberg registerTable API call has failed.
        try {
            Thread.sleep(2000);
        }
        catch (Exception e) {
            // ignored
        }

        // An error will occur here due to a bug in the Iceberg lib, where a register table failure causes
        // the metadata file of the source table to be deleted.
        // In other words, data corruption has occurred.
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES(2, 2)", 1);
        assertQuery("SELECT * FROM " + sourceTableName, "VALUES(1, 1), (2, 2)");
        dropTable(getSession(), sourceTableName);
    }

    private static boolean endsWithTableUUID(String tableName, String tablePath)
    {
        return tablePath.matches(format(".*%s_[-a-fA-F0-9]{36}/$", tableName));
    }
}
