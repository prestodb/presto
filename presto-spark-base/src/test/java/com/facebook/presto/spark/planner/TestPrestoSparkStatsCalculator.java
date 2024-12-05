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

package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.FragmentStatsProvider;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.cost.JoinNodeStatsEstimate;
import com.facebook.presto.cost.PartialAggregationStatsEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculatorTester;
import com.facebook.presto.cost.TableWriterNodeStatsEstimate;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.IGNORE_SAFE_CONSTANTS;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NaN;

@Test(singleThreaded = true)
public class TestPrestoSparkStatsCalculator
{
    private static final QueryId TEST_QUERY_ID = new QueryId("testqueryid");
    private HistoryBasedPlanStatisticsCalculator historyBasedPlanStatisticsCalculator;
    private FragmentStatsProvider fragmentStatsProvider;
    private PrestoSparkStatsCalculator prestoSparkStatsCalculator;
    private Metadata metadata;
    private StatsCalculatorTester tester;
    private Session session;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder()
                .setQueryId(TEST_QUERY_ID)
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .build();
        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new InMemoryHistoryBasedPlanStatisticsProvider());
            }
        });

        historyBasedPlanStatisticsCalculator = (HistoryBasedPlanStatisticsCalculator) queryRunner.getStatsCalculator();
        fragmentStatsProvider = queryRunner.getFragmentStatsProvider();
        prestoSparkStatsCalculator = new PrestoSparkStatsCalculator(
                historyBasedPlanStatisticsCalculator,
                historyBasedPlanStatisticsCalculator.getDelegate(),
                new HistoryBasedOptimizationConfig());
        metadata = queryRunner.getMetadata();
        tester = new StatsCalculatorTester(
                queryRunner,
                prestoSparkStatsCalculator);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @AfterMethod(alwaysRun = true)
    public void resetCaches()
    {
        ((InMemoryHistoryBasedPlanStatisticsProvider) historyBasedPlanStatisticsCalculator.getHistoryBasedPlanStatisticsProvider().get()).clearCache();
        fragmentStatsProvider.invalidateStats(TEST_QUERY_ID, 1);
    }

    @Test
    public void testUsesHboStatsWhenMatchRuntime()
    {
        fragmentStatsProvider.putStats(TEST_QUERY_ID, new PlanFragmentId(1), new PlanNodeStatsEstimate(NaN, 1000, HIGH, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown()));
        PlanBuilder planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), metadata);
        PlanNode statsEquivalentRemoteSource = planBuilder
                .registerVariable(planBuilder.variable("c1"))
                .filter(planBuilder.rowExpression("c1 IS NOT NULL"),
                        planBuilder.values(planBuilder.variable("c1")));
        Optional<String> hash = historyBasedPlanStatisticsCalculator.getPlanCanonicalInfoProvider().hash(session, statsEquivalentRemoteSource, IGNORE_SAFE_CONSTANTS, false);

        InMemoryHistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = (InMemoryHistoryBasedPlanStatisticsProvider) historyBasedPlanStatisticsCalculator.getHistoryBasedPlanStatisticsProvider().get();
        historyBasedPlanStatisticsProvider.putStats(ImmutableMap.of(
                new PlanNodeWithHash(
                        statsEquivalentRemoteSource,
                        hash),
                new HistoricalPlanStatistics(
                        ImmutableList.of(
                                new HistoricalPlanStatisticsEntry(
                                        new PlanStatistics(Estimate.of(100), Estimate.of(1000), 1, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty()),
                                        ImmutableList.of(), new HistoricalPlanStatisticsEntryInfo(HistoricalPlanStatisticsEntryInfo.WorkerType.JAVA, QueryId.valueOf("0"), "test"))))));

        tester.assertStatsFor(pb -> pb.remoteSource(ImmutableList.of(new PlanFragmentId(1)), statsEquivalentRemoteSource))
                .check(check -> check.totalSize(1000)
                        .outputRowsCount(100));
    }

    @Test
    public void testUsesRuntimeStatsWhenNoHboStats()
    {
        fragmentStatsProvider.putStats(TEST_QUERY_ID, new PlanFragmentId(1), new PlanNodeStatsEstimate(NaN, 1000, HIGH, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown()));
        tester.assertStatsFor(pb -> pb.remoteSource(ImmutableList.of(new PlanFragmentId(1))))
                .check(check -> check.totalSize(1000)
                        .outputRowsCountUnknown());
    }

    @Test
    public void testUsesRuntimeStatsWhenHboDisabled()
    {
        Session session = testSessionBuilder()
                .setQueryId(TEST_QUERY_ID)
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "false")
                .build();
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);
        HistoryBasedPlanStatisticsCalculator historyBasedPlanStatisticsCalculator = (HistoryBasedPlanStatisticsCalculator) localQueryRunner.getStatsCalculator();
        FragmentStatsProvider fragmentStatsProvider = localQueryRunner.getFragmentStatsProvider();
        PrestoSparkStatsCalculator prestoSparkStatsCalculator = new PrestoSparkStatsCalculator(
                historyBasedPlanStatisticsCalculator,
                historyBasedPlanStatisticsCalculator.getDelegate(),
                new HistoryBasedOptimizationConfig());
        StatsCalculatorTester tester = new StatsCalculatorTester(
                localQueryRunner,
                prestoSparkStatsCalculator);
        fragmentStatsProvider.putStats(TEST_QUERY_ID, new PlanFragmentId(1), new PlanNodeStatsEstimate(NaN, 1000, HIGH, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown()));

        PlanBuilder planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), localQueryRunner.getMetadata());
        PlanNode statsEquivalentRemoteSource = planBuilder
                .registerVariable(planBuilder.variable("c1"))
                .filter(planBuilder.rowExpression("c1 IS NOT NULL"),
                        planBuilder.values(planBuilder.variable("c1")));
        HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsCalculator.getHistoryBasedPlanStatisticsProvider().get();
        historyBasedPlanStatisticsProvider.putStats(ImmutableMap.of(
                new PlanNodeWithHash(
                        statsEquivalentRemoteSource,
                        Optional.empty()),
                new HistoricalPlanStatistics(
                        ImmutableList.of(
                                new HistoricalPlanStatisticsEntry(
                                        new PlanStatistics(Estimate.of(100), Estimate.of(1000), 1, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty()),
                                        ImmutableList.of(), new HistoricalPlanStatisticsEntryInfo(HistoricalPlanStatisticsEntryInfo.WorkerType.JAVA, QueryId.valueOf("0"), "test"))))));

        tester.assertStatsFor(pb -> pb.remoteSource(ImmutableList.of(new PlanFragmentId(1))))
                .check(check -> check.totalSize(1000)
                        .outputRowsCountUnknown());
        tester.close();
    }

    @Test
    public void testUsesRuntimeStatsWhenDiffersFromHbo()
    {
        fragmentStatsProvider.putStats(TEST_QUERY_ID, new PlanFragmentId(1), new PlanNodeStatsEstimate(NaN, 1000, HIGH, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown()));

        PlanBuilder planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), metadata);
        PlanNode statsEquivalentRemoteSource = planBuilder
                .registerVariable(planBuilder.variable("c1"))
                .filter(planBuilder.rowExpression("c1 IS NOT NULL"),
                        planBuilder.values(planBuilder.variable("c1")));
        HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsCalculator.getHistoryBasedPlanStatisticsProvider().get();
        historyBasedPlanStatisticsProvider.putStats(ImmutableMap.of(
                new PlanNodeWithHash(
                        statsEquivalentRemoteSource,
                        Optional.empty()),
                new HistoricalPlanStatistics(
                        ImmutableList.of(
                                new HistoricalPlanStatisticsEntry(
                                        new PlanStatistics(Estimate.of(10), Estimate.of(100), 1, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty()),
                                        ImmutableList.of(), new HistoricalPlanStatisticsEntryInfo(HistoricalPlanStatisticsEntryInfo.WorkerType.JAVA, QueryId.valueOf("0"), "test"))))));

        tester.assertStatsFor(pb -> pb.remoteSource(ImmutableList.of(new PlanFragmentId(1))))
                .check(check -> check.totalSize(1000)
                        .outputRowsCountUnknown());
    }
}
