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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

/**
 * Plan-level tests for materialized views in the Memory connector.
 * Tests verify plan structure with legacy_materialized_views=false.
 */
@Test(singleThreaded = true)
public class TestMemoryMaterializedViewPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .setSystemProperty("legacy_materialized_views", "false")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .setExtraProperties(ImmutableMap.of("experimental.allow-legacy-materialized-views-toggle", "true"))
                .build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        return queryRunner;
    }

    @Test
    public void testMaterializedViewNotRefreshed()
    {
        assertUpdate("CREATE TABLE base_table (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
        assertUpdate("CREATE MATERIALIZED VIEW simple_mv AS SELECT id, name, value FROM base_table");

        assertPlan(getSession(), "SELECT * FROM simple_mv",
                anyTree(tableScan("base_table")));

        assertUpdate("DROP MATERIALIZED VIEW simple_mv");
        assertUpdate("DROP TABLE base_table");
    }

    @Test
    public void testMaterializedViewRefreshed()
    {
        assertUpdate("CREATE TABLE base_table (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO base_table VALUES (1, 100), (2, 200)", 2);
        assertUpdate("CREATE MATERIALIZED VIEW mv AS SELECT id, value FROM base_table");
        assertUpdate("REFRESH MATERIALIZED VIEW mv", 2);

        assertPlan("SELECT * FROM mv",
                anyTree(tableScan("mv")));

        assertUpdate("DROP MATERIALIZED VIEW mv");
        assertUpdate("DROP TABLE base_table");
    }

    @Test
    public void testQueryDroppedMaterializedView()
    {
        assertUpdate("CREATE TABLE base_table (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO base_table VALUES (1, 100), (2, 200)", 2);
        assertUpdate("CREATE MATERIALIZED VIEW dropped_mv AS SELECT id, value FROM base_table");

        assertUpdate("DROP MATERIALIZED VIEW dropped_mv");

        assertQueryFails("SELECT * FROM dropped_mv", ".*Table memory\\.default\\.dropped_mv does not exist.*");

        assertUpdate("DROP TABLE base_table");
    }
}
