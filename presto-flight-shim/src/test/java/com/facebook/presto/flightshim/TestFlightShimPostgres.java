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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;

@Test
public class TestFlightShimPostgres
        extends AbstractTestFlightShimQueries
{
    private static final String DATABASE_USERNAME = "testuser";
    private static final String DATABASE_PASSWORD = "testpass";
    private final PostgreSQLContainer<?> postgresContainer;

    public TestFlightShimPostgres()
    {
        this.postgresContainer = new PostgreSQLContainer<>("postgres:14")
                .withDatabaseName("tpch")
                .withUsername(DATABASE_USERNAME)
                .withPassword(DATABASE_PASSWORD);
        postgresContainer.start();
        closables.add(postgresContainer);
    }

    @Override
    protected String getConnectorId()
    {
        return "postgresql";
    }

    @Override
    protected String getConnectionUrl()
    {
        return addDatabaseCredentialsToJdbcUrl(postgresContainer.getJdbcUrl(), DATABASE_USERNAME, DATABASE_PASSWORD);
    }

    @Override
    protected String getPluginBundles()
    {
        return "../presto-postgresql/pom.xml";
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return createPostgreSqlQueryRunner(postgresContainer.getJdbcUrl(), ImmutableMap.of(), TpchTable.getTables());
    }
}
