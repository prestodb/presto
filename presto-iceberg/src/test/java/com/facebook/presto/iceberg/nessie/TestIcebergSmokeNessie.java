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

import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Test
public class TestIcebergSmokeNessie
        extends IcebergDistributedSmokeTestBase
{
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

        Optional<File> tempTableLocation = Arrays.stream(requireNonNull(((DistributedQueryRunner) queryRunner)
                        .getCoordinator().getDataDirectory().resolve(schema).toFile().listFiles()))
                .filter(file -> file.toURI().toString().contains(table)).findFirst();

        String dataLocation = ((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory().toFile().toURI().toString();
        String relativeTableLocation = tempTableLocation.get().toURI().toString().replace(dataLocation, "");

        return format("%s/%s", dataLocation, relativeTableLocation.substring(0, relativeTableLocation.length() - 1));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), nessieConnectorProperties(nessieContainer.getRestApiUri()));
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(NESSIE);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toFile().getPath());

        NessieConfig nessieConfig = new NessieConfig().setServerUri(nessieContainer.getRestApiUri());

        IcebergResourceFactory resourceFactory = new IcebergResourceFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                nessieConfig,
                new PrestoS3ConfigurationUpdater(new HiveS3Config()));

        return IcebergUtil.getNativeIcebergTable(resourceFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }
}
