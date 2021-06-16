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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertNotEquals;
import static com.facebook.presto.SystemSessionProperties.FORCE_COORDINATOR_NODE_EXECUTION_FOR_VALUES_QUERIES;
import static com.facebook.presto.spi.WarningCollector.NOOP;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestPlanFragmenter
{
    private FinalizerService finalizerService;
    private NodeScheduler nodeScheduler;
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("tpch"));

        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSelectionStats(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        queryRunner = createQueryRunner(ImmutableMap.of());
    }

    private static LocalQueryRunner createQueryRunner(Map<String, String> sessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .addPreparedStatement("my_query", "SELECT * FROM nation");

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));
        LocalQueryRunner queryRunner = new LocalQueryRunner(sessionBuilder.build());
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        finalizerService.destroy();
        nodeScheduler.stop();
        nodeScheduler = null;
        queryRunner.close();
    }

    private Session createSession(Map<String, String> systemProperties)
    {
        Session.SessionBuilder sessionBuilder = Session.builder(queryRunner.getDefaultSession());
        for (Map.Entry<String, String> property : systemProperties.entrySet()) {
            sessionBuilder.setSystemProperty(property.getKey(), property.getValue());
        }
        return sessionBuilder.build();
    }

    @Test
    public void testForceCoordinator()
    {
        String[] canForceCoordinatorSqls = new String[]{"select 1+2", "show create table nation", "explain (type distributed) select * from nation", "DESCRIBE INPUT my_query", "DESCRIBE OUTPUT my_query", "SHOW STATS FOR (SELECT * FROM nation)", "SHOW catalogs"};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = queryRunner.inTransaction(createSession(ImmutableMap.of(FORCE_COORDINATOR_NODE_EXECUTION_FOR_VALUES_QUERIES, "true")), transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, false, NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertEquals(partitioningHandle.getConnectorHandle(), COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }

    @Test
    public void testCanNotForceCoordinator()
    {
        String[] canForceCoordinatorSqls = new String[]{"SHOW TABLES", "select * from nation "};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = queryRunner.inTransaction(createSession(ImmutableMap.of(FORCE_COORDINATOR_NODE_EXECUTION_FOR_VALUES_QUERIES, "true")), transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, false, NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertNotEquals(partitioningHandle.getConnectorHandle(), COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }

    @Test
    public void testDisableCoordinatorExecution()
    {
        String[] canForceCoordinatorSqls = new String[]{"select 1", "show create table nation", "explain select * from nation", "DESCRIBE INPUT my_query", "DESCRIBE OUTPUT my_query", "SHOW STATS FOR (SELECT * FROM nation)", "SHOW catalogs"};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = queryRunner.inTransaction(queryRunner.getDefaultSession(), transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, false, NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertNotEquals(partitioningHandle.getConnectorHandle(), COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }
}
