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
package com.facebook.presto.cost;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.USE_EXTERNAL_PLAN_STATISTICS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

public class TestHistoricalStatsCalculator
{
    private final LocalQueryRunner queryRunner;

    public TestHistoricalStatsCalculator()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(USE_EXTERNAL_PLAN_STATISTICS, "true")
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build();

        this.queryRunner = new LocalQueryRunner(session, new FeaturesConfig(), new NodeSpillConfig(), false, false, getObjectMapper());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ExternalPlanStatisticsProvider> getExternalPlanStatisticsProviders()
            {
                return ImmutableList.of(new TestExternalPlanStatisticsProvider());
            }
        });
    }

    // TODO: Add a proper test once we add a write path for historical statistics
    @Test
    public void testHistoricalStatistics()
    {
        assertPlan(
                "SELECT orderstatus, 1 FROM orders WHERE orderkey < 100",
                anyTree(node(FilterNode.class, node(TableScanNode.class)).withOutputRowCount(100)));

        assertPlan(
                "SELECT orderstatus, 2 FROM orders WHERE orderkey < 100",
                anyTree(node(FilterNode.class, node(TableScanNode.class)).withOutputRowCount(100)));
    }

    private static ObjectMapper getObjectMapper()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        return new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)))
                .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    private static class TestExternalPlanStatisticsProvider
            implements ExternalPlanStatisticsProvider
    {
        @Override
        public String getName()
        {
            return "test";
        }

        // TODO: Add a proper test once we add a write path for historical statistics
        @Override
        public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodesWithHash)
        {
            for (PlanNodeWithHash planNodeWithHash : planNodesWithHash) {
                if (planNodeWithHash.getHash().equals(Optional.of("a31e6a5798fdd29cd930668d2b7becc13bd21baaf572d5dbeff66d6e15efc342"))) {
                    return ImmutableMap.of(planNodeWithHash, new HistoricalPlanStatistics(new PlanStatistics(Estimate.of(100), Estimate.of(100), 1.0)));
                }
            }
            return ImmutableMap.of();
        }

        @Override
        public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics) {}
    }
}
