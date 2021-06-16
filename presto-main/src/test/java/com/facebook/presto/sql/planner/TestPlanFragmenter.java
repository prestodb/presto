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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertNotEquals;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.testng.Assert.assertEquals;

public class TestPlanFragmenter
{
    private PlanFragmenter planFragmenter;
    private MetadataManager metadata;
    private TransactionManager transactionManager;
    private FinalizerService finalizerService;
    private NodeScheduler nodeScheduler;
    private NodePartitioningManager nodePartitioningManager;
    private LocalQueryRunner forceQueryRunner;
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("tpch"));
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSelectionStats(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        PartitioningProviderManager partitioningProviderManager = new PartitioningProviderManager();
        nodePartitioningManager = new NodePartitioningManager(nodeScheduler, partitioningProviderManager);
        planFragmenter = new PlanFragmenter(metadata, nodePartitioningManager, new QueryManagerConfig(), new SqlParser(), new FeaturesConfig());
        Map<String, String> map = new HashMap<>();
        map.put(SystemSessionProperties.FORCE_COORDINATOR_EXECUTION, "true");
        queryRunner = createQueryRunner(Collections.EMPTY_MAP);
        forceQueryRunner = createQueryRunner(map);
    }

    private static LocalQueryRunner createQueryRunner(Map<String, String> sessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle
        // exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        LocalQueryRunner queryRunner = new LocalQueryRunner(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        planFragmenter = null;
        transactionManager = null;
        metadata = null;
        finalizerService.destroy();
        finalizerService = null;
        nodeScheduler.stop();
        nodeScheduler = null;
        nodePartitioningManager = null;
        forceQueryRunner.close();
        queryRunner.close();
    }

    @Test
    public void testForceCoordinator()
    {
        String[] canForceCoordinatorSqls = new String[]{"select 1", "show create table nation", "explain select * from nation", "DESCRIBE INPUT my_query", "DESCRIBE OUTPUT my_query", "show create catalog local", "SHOW STATS FOR (SELECT * FROM nation)"};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = forceQueryRunner.inTransaction(forceQueryRunner.getDefaultSession(), transactionSession -> {
                Plan plan = forceQueryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
                return forceQueryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertEquals(partitioningHandle.getConnectorHandle(), SystemPartitioningHandle.COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }

    @Test
    public void testCanNotForceCoordinator()
    {
        String[] canForceCoordinatorSqls = new String[]{"SHOW TABLES", "SHOW catalogs", "select * from nation "};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = forceQueryRunner.inTransaction(forceQueryRunner.getDefaultSession(), transactionSession -> {
                Plan plan = forceQueryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
                return forceQueryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertNotEquals(partitioningHandle.getConnectorHandle(), SystemPartitioningHandle.COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }

    @Test
    public void testCloseForceCoordinator()
    {
        String[] canForceCoordinatorSqls = new String[]{"select 1", "show create table nation", "explain select * from nation", "DESCRIBE INPUT my_query", "DESCRIBE OUTPUT my_query", "show create catalog local", "SHOW STATS FOR (SELECT * FROM nation)"};
        for (String sql : canForceCoordinatorSqls) {
            SubPlan subPlan = queryRunner.inTransaction(queryRunner.getDefaultSession(), transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, false);
            });
            PartitioningHandle partitioningHandle = subPlan.getFragment().getPartitioning();
            assertNotEquals(partitioningHandle.getConnectorHandle(), SystemPartitioningHandle.COORDINATOR_DISTRIBUTION.getConnectorHandle());
        }
    }
}
