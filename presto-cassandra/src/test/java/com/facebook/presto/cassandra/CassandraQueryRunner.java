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
package com.facebook.presto.cassandra;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class CassandraQueryRunner
{
    private CassandraQueryRunner()
    {
    }

    private static boolean tpchLoaded;

    public static DistributedQueryRunner createCassandraQueryRunner(CassandraServer server, Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createCassandraSession("tpch"), 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
        connectorProperties.putIfAbsent("cassandra.contact-points", server.getHost());
        connectorProperties.putIfAbsent("cassandra.native-protocol-port", Integer.toString(server.getPort()));
        connectorProperties.putIfAbsent("cassandra.allow-drop-table", "true");
        // Driver 4.x requires datacenter configuration
        connectorProperties.putIfAbsent("cassandra.load-policy.use-dc-aware", "true");
        connectorProperties.putIfAbsent("cassandra.load-policy.dc-aware.local-dc", "datacenter1");

        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog("cassandra", "cassandra", connectorProperties);

        createKeyspace(server.getSession(), "tpch");
        List<TpchTable<?>> tables = TpchTable.getTables();
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createCassandraSession("tpch"), tables);

        // Refresh size estimates for each table
        for (TpchTable<?> table : tables) {
            server.refreshSizeEstimates("tpch", table.getTableName());
        }

        // Force refresh of Cassandra driver's metadata cache
        server.refreshMetadata();

        // Invalidate the application-level metadata cache to ensure fresh schema is loaded
        server.getSession().invalidateKeyspaceCache("tpch");

        // Verify that all tables are visible before returning
        verifyTablesExist(server.getSession(), "tpch", tables);

        return queryRunner;
    }

    private static void verifyTablesExist(CassandraSession session, String keyspace, List<TpchTable<?>> tables)
    {
        List<String> tableNames = session.getCaseSensitiveTableNames(keyspace);
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            if (!tableNames.contains(tableName)) {
                throw new RuntimeException(String.format(
                        "Table %s.%s was not found after loading TPCH data. Available tables: %s",
                        keyspace, tableName, tableNames));
            }
        }
    }

    public static Session createCassandraSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(schema)
                .build();
    }
}
