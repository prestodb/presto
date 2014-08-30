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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyAllTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestPostgreSqlDistributedQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestPostgreSqlDistributedQueries.class);

    private final TestingPostgreSqlServer postgresqlServer;

    @SuppressWarnings("UnusedDeclaration")
    public TestPostgreSqlDistributedQueries()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlDistributedQueries(TestingPostgreSqlServer postgresqlServer)
            throws Exception
    {
        super(createQueryRunner(postgresqlServer));
        this.postgresqlServer = postgresqlServer;
    }

    @AfterClass(alwaysRun = true)
    @SuppressWarnings({"EmptyTryBlock", "UnusedDeclaration"})
    public void destroy()
            throws IOException
    {
        try (QueryRunner queryRunner = this.queryRunner;
                TestingPostgreSqlServer server = this.postgresqlServer) {
            // use try-with-resources to close everything safely
        }
    }

    private static QueryRunner createQueryRunner(TestingPostgreSqlServer server)
            throws Exception
    {
        try {
            return doCreateQueryRunner(server);
        }
        catch (Exception e) {
            // use try-with-resources to close everything safely
            try (TestingPostgreSqlServer ignored = server) {
                throw e;
            }
        }
    }

    private static QueryRunner doCreateQueryRunner(TestingPostgreSqlServer server)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 3);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Map<String, String> properties = ImmutableMap.of("connection-url", server.getJdbcUrl());
        createSchema(server.getJdbcUrl(), "tpch");

        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("postgresql", "postgresql", properties);

        log.info("Loading data...");
        long startTime = System.nanoTime();
        copyAllTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession("tpch"));
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "postgresql", schema, UTC_KEY, ENGLISH, null, null);
    }

    private static void createSchema(String url, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }
}
