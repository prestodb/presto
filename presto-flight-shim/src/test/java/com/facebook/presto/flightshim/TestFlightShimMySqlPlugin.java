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
package com.facebook.presto.flightshim;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testcontainers.mysql.MySQLContainer;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;

@Test
public class TestFlightShimMySqlPlugin
        extends AbstractTestFlightShimPlugins
{
    private static final String DATABASE_USERNAME = "testuser";
    private static final String DATABASE_PASSWORD = "testpass";
    private final MySQLContainer mysqlContainer;

    public TestFlightShimMySqlPlugin()
    {
        this.mysqlContainer = new MySQLContainer("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername(DATABASE_USERNAME)
                .withPassword(DATABASE_PASSWORD);
        mysqlContainer.start();
        closables.add(mysqlContainer);
    }

    @Override
    protected String getConnectorId()
    {
        return "mysql";
    }

    @Override
    protected String getConnectionUrl()
    {
        return addDatabaseCredentialsToJdbcUrl(
                removeDatabaseFromJdbcUrl(mysqlContainer.getJdbcUrl()),
                DATABASE_USERNAME,
                DATABASE_PASSWORD);
    }

    @Override
    protected String getPluginBundles()
    {
        return "../presto-mysql/pom.xml";
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlContainer.getJdbcUrl(), ImmutableMap.of(), TpchTable.getTables());
    }
}
