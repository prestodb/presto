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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;

import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyAllTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMySqlDistributedQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestMySqlDistributedQueries.class);

    private final TestingMySqlServer mysqlServer;

    @SuppressWarnings("UnusedDeclaration")
    public TestMySqlDistributedQueries()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch"));
    }

    public TestMySqlDistributedQueries(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createQueryRunner(mysqlServer));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    @SuppressWarnings({"EmptyTryBlock", "UnusedDeclaration"})
    public void destroy()
    {
        try (QueryRunner queryRunner = this.queryRunner;
                TestingMySqlServer mysqlServer = this.mysqlServer) {
            // use try-with-resources to close everything safely
        }
    }

    private static QueryRunner createQueryRunner(TestingMySqlServer server)
            throws Exception
    {
        try {
            return doCreateQueryRunner(server);
        }
        catch (Exception e) {
            // use try-with-resources to close everything safely
            try (TestingMySqlServer ignored = server) {
                throw e;
            }
        }
    }

    private static QueryRunner doCreateQueryRunner(TestingMySqlServer server)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 3);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Map<String, String> properties = ImmutableMap.of("connection-url", server.getJdbcUrl());

        queryRunner.installPlugin(new MySqlPlugin());
        queryRunner.createCatalog("mysql", "mysql", properties);

        log.info("Loading data...");
        long startTime = System.nanoTime();
        copyAllTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession("tpch"));
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "mysql", schema, UTC_KEY, ENGLISH, null, null);
    }
}
