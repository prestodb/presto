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

import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.nessie.NessieConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static java.lang.String.format;

@Test
public class TestIcebergSmokeHadoop
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHadoop()
    {
        super(HADOOP);
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s%s/%s", tempLocation.toURI(), schema, table);
    }

    @Override
    protected Path getCatalogDirectory()
    {
        return getDistributedQueryRunner().getCoordinator().getDataDirectory();
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(HADOOP);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toFile().getPath());

        IcebergResourceFactory resourceFactory = new IcebergResourceFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new NessieConfig(),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()));

        return IcebergUtil.getNativeIcebergTable(resourceFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }
}
