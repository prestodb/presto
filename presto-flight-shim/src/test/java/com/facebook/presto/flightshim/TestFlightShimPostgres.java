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

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;
import com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

@Test
public class TestFlightShimPostgres
        extends AbstractTestFlightShimQueries
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestFlightShimPostgres()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer("testuser", "tpch");
        closables.add(postgreSqlServer);
    }

    @Override
    protected String getConnectorId()
    {
        return "postgresql";
    }

    @Override
    protected String getConnectionUrl()
    {
        return postgreSqlServer.getJdbcUrl();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ImmutableMap.of(), TpchTable.getTables());
    }
}
