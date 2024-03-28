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
package com.facebook.presto.iceberg.jdbc;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.JdbcContainer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.iceberg.CatalogType.JDBC;
import static java.lang.String.format;

@Test
public class TestIcebergSmokeJdbc
        extends IcebergDistributedSmokeTestBase
{
    private JdbcContainer jdbcContainer;

    public TestIcebergSmokeJdbc()
    {
        super(JDBC);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        jdbcContainer = JdbcContainer.builder().build();
        jdbcContainer.start();
        super.init();
    }

    @AfterClass
    public void tearDown()
    {
        if (jdbcContainer != null) {
            jdbcContainer.stop();
        }
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s%s/%s", tempLocation.toURI(), schema, table);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), JdbcTestUtil.jdbcConnectorProperties(jdbcContainer.getJdbcURI()));
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(JDBC);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toFile().getPath());

        IcebergResourceFactory resourceFactory = JdbcTestUtil.getIcebergResourceFactory(icebergConfig, jdbcContainer);
        return IcebergUtil.getNativeIcebergTable(resourceFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }
}
