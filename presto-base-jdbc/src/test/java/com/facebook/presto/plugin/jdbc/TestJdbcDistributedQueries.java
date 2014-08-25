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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import io.airlift.log.Logger;
import org.testng.annotations.AfterClass;

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

public abstract class TestJdbcDistributedQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestJdbcDistributedQueries.class);

    public TestJdbcDistributedQueries()
            throws Exception
    {
        super(createQueryRunner());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        queryRunner.close();
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 2);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Map<String, String> properties = TestingH2JdbcModule.createProperties();
        createSchema(properties, "tpch");

        queryRunner.installPlugin(new JdbcPlugin("base-jdbc", new TestingH2JdbcModule()));
        queryRunner.createCatalog("jdbc", "base-jdbc", properties);

        log.info("Loading data...");
        long startTime = System.nanoTime();
        copyAllTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession("tpch"));
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "jdbc", schema, UTC_KEY, ENGLISH, null, null);
    }

    private static void createSchema(Map<String, String> properties, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }
}
