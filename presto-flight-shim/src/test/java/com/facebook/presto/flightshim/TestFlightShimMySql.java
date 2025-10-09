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
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;

@Test
public class TestFlightShimMySql
        extends AbstractTestFlightShimQueries
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .build();
    private final TestingMySqlServer mysqlServer;

    public TestFlightShimMySql()
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of("tpch"), MY_SQL_OPTIONS);
        closables.add(mysqlServer);
    }

    @Override
    protected String getConnectorId()
    {
        return "mysql";
    }

    @Override
    protected String getConnectionUrl()
    {
        return mysqlServer.getJdbcUrl();
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
        return createMySqlQueryRunner(mysqlServer, ImmutableMap.of(), TpchTable.getTables());
    }
}
