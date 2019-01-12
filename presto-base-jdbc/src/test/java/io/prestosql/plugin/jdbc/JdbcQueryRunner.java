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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.tests.DistributedQueryRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;

public final class JdbcQueryRunner
{
    private JdbcQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createJdbcQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createJdbcQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createJdbcQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> properties = TestingH2JdbcModule.createProperties();
            createSchema(properties, "tpch");

            queryRunner.installPlugin(new JdbcPlugin("base-jdbc", new TestingH2JdbcModule()));
            queryRunner.createCatalog("jdbc", "base-jdbc", properties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void createSchema(Map<String, String> properties, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("jdbc")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
