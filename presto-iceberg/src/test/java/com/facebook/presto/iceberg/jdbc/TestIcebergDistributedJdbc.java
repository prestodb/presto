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
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.JdbcContainer;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.JDBC;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;

@Test
public class TestIcebergDistributedJdbc
        extends IcebergDistributedTestBase
{
    private JdbcContainer jdbcContainer;

    protected TestIcebergDistributedJdbc()
    {
        super(JDBC);
    }

    @Override
    protected Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString(), "uri", jdbcContainer.getJdbcURI(),
                "user", jdbcContainer.getDefaultUser(), "password", jdbcContainer.getDefaultPassword());
    }

    @Override
    protected Table loadTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(JDBC);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().getPath());

        IcebergResourceFactory resourceFactory =  JdbcTestUtil.getIcebergResourceFactory(icebergConfig, jdbcContainer);
        return IcebergUtil.getNativeIcebergTable(
                resourceFactory,
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
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
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), JdbcTestUtil.jdbcConnectorProperties(jdbcContainer.getJdbcURI()));
    }
}
