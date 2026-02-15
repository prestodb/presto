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
import com.facebook.presto.testing.MaterializedResult;
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

    public static DistributedQueryRunner createCassandraQueryRunner(CassandraServer server, Map<String, String> connectorProperties)
            throws Exception
    {
        System.out.println("=== CassandraQueryRunner: Starting createCassandraQueryRunner ===");
        DistributedQueryRunner queryRunner = null;
        try {
            System.out.println("=== CassandraQueryRunner: Creating DistributedQueryRunner ===");
            queryRunner = new DistributedQueryRunner(createCassandraSession("tpch"), 4);

            System.out.println("=== CassandraQueryRunner: Installing TpchPlugin ===");
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("cassandra.contact-points", server.getHost());
            connectorProperties.putIfAbsent("cassandra.native-protocol-port", Integer.toString(server.getPort()));
            connectorProperties.putIfAbsent("cassandra.allow-drop-table", "true");
            connectorProperties.putIfAbsent("cassandra.load-policy.dc-aware.local-dc", "datacenter1");

            System.out.println("=== CassandraQueryRunner: Installing CassandraPlugin ===");
            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog("cassandra", "cassandra", connectorProperties);

            System.out.println("=== CassandraQueryRunner: Creating keyspace 'tpch' ===");
            createKeyspace(server.getSession(), "tpch");

            System.out.println("=== CassandraQueryRunner: Starting to copy TPCH tables ===");
            List<TpchTable<?>> tables = TpchTable.getTables();
            System.out.println("=== CassandraQueryRunner: Tables to copy: " + tables.size() + " ===");

            try {
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createCassandraSession("tpch"), tables, true);
                System.out.println("=== CassandraQueryRunner: Successfully copied TPCH tables ===");
                
                // Validate that tables were actually created and populated
                System.out.println("=== CassandraQueryRunner: Validating table creation ===");
                for (TpchTable<?> table : tables) {
                    String tableName = table.getTableName();
                    try {
                        MaterializedResult result = queryRunner.execute(
                            createCassandraSession("tpch"),
                            String.format("SELECT COUNT(*) FROM cassandra.tpch.%s", tableName)
                        );
                        long count = (Long) result.getMaterializedRows().get(0).getField(0);
                        System.out.println(String.format("=== Table %s: %d rows ===", tableName, count));
                        
                        if (count == 0) {
                            throw new RuntimeException(String.format("Table %s was created but contains no data", tableName));
                        }
                    }
                    catch (Exception e) {
                        System.err.println(String.format("=== VALIDATION FAILED for table %s ===", tableName));
                        throw new RuntimeException(String.format("Table validation failed for %s", tableName), e);
                    }
                }
                System.out.println("=== CassandraQueryRunner: All tables validated successfully ===");
            }
            catch (Exception e) {
                System.err.println("=== CassandraQueryRunner: ERROR copying TPCH tables ===");
                e.printStackTrace(System.err);
                throw new RuntimeException("Failed to copy TPCH tables", e);
            }

            System.out.println("=== CassandraQueryRunner: Refreshing size estimates ===");
            for (TpchTable<?> table : tables) {
                server.refreshSizeEstimates("tpch", table.getTableName());
            }

            System.out.println("=== CassandraQueryRunner: Successfully completed createCassandraQueryRunner ===");
            return queryRunner;
        }
        catch (Exception e) {
            System.err.println("=== CassandraQueryRunner: FATAL ERROR in createCassandraQueryRunner ===");
            e.printStackTrace(System.err);
            if (queryRunner != null) {
                try {
                    System.err.println("=== CassandraQueryRunner: Attempting to close queryRunner due to error ===");
                    queryRunner.close();
                }
                catch (Exception closeException) {
                    System.err.println("=== CassandraQueryRunner: Error closing queryRunner ===");
                    closeException.printStackTrace(System.err);
                }
            }
            throw e;
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
