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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskRdd;
import com.facebook.presto.spark.execution.FragmentExecutionResult;
import com.facebook.presto.spark.execution.PrestoSparkAdaptiveQueryExecution;
import com.facebook.presto.spark.execution.PrestoSparkStaticQueryExecution;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.QueryAssertions;
import org.apache.spark.Dependency;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.STORAGE_BASED_BROADCAST_JOIN_ENABLED;
import static com.facebook.presto.spark.execution.RuntimeStatistics.createRuntimeStats;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoSparkQueryExecution
        extends AbstractTestQueryFramework
{
    PrestoSparkQueryRunner prestoSparkQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
    {
        prestoSparkQueryRunner = createHivePrestoSparkQueryRunner();
        return prestoSparkQueryRunner;
    }

    private IPrestoSparkQueryExecution getPrestoSparkQueryExecution(Session session, String sql)
    {
        return prestoSparkQueryRunner.createPrestoSparkQueryExecution(session, sql, Optional.empty());
    }

    @Test
    public void testQueryExecutionCreation()
    {
        String sqlText = "select * from lineitem";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .build();
        IPrestoSparkQueryExecution psQueryExecution = getPrestoSparkQueryExecution(session, sqlText);
        assertTrue(psQueryExecution instanceof PrestoSparkStaticQueryExecution);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .build();
        psQueryExecution = getPrestoSparkQueryExecution(session, sqlText);
        assertTrue(psQueryExecution instanceof PrestoSparkAdaptiveQueryExecution);
    }

    @Test
    public void testSingleFragmentQueryAdaptiveExecution()
    {
        String sqlText = "select * from lineitem";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testJoinQueryAdaptiveExecution()
    {
        String sqlText = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testQroupByAdaptiveExecution()
    {
        String sqlText = "SELECT custkey, orderstatus FROM orders ORDER BY orderkey DESC LIMIT 10";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testRddCreationForPartitionedJoinWithoutShuffle()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForPartitionedJoinWithShuffle()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey UNION ALL select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForMemoryBasedBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForStorageBasedBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testMapOutputStatsExtraction()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        PrestoSparkStaticQueryExecution execution = (PrestoSparkStaticQueryExecution) getPrestoSparkQueryExecution(session, sql);
        Optional<PlanNodeStatsEstimate> planNodeStatsEstimate;

        // Empty stats case
        planNodeStatsEstimate = createRuntimeStats(Optional.empty());
        assertFalse(planNodeStatsEstimate.isPresent());

        // Empty partition array
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 0);

        // One partition case
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {23})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 23);

        // Multiple partition case
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {23, 520, 190})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 733);
    }

    private void validateFragmentedRddCreation(Session session, String sql)
    {
        PrestoSparkStaticQueryExecution execution = (PrestoSparkStaticQueryExecution) getPrestoSparkQueryExecution(session, sql);
        RddAndMore rddAndMoreStatic = null;
        RddAndMore rddAndMoreFromFragmentedExecution = null;
        try {
            SubPlan rootFragmentedPlan = execution.createFragmentedPlan();
            TableWriteInfo tableWriteInfo = execution.getTableWriteInfo(session, rootFragmentedPlan);
            rddAndMoreStatic = execution.createRdd(rootFragmentedPlan, PrestoSparkSerializedPage.class, tableWriteInfo);
            FragmentExecutionResult fragmentExecutionResult = executeInStages(execution, rootFragmentedPlan, tableWriteInfo);
            rddAndMoreFromFragmentedExecution = fragmentExecutionResult.getRddAndMore();
        }
        catch (Exception e) {
            fail("Failed while creating RDD", e);
        }

        assertRddAndMoreEquals(rddAndMoreStatic, rddAndMoreFromFragmentedExecution);
    }

    // For testing purpose, execute plan by executing sub-plans starting at(in the same order) :
    // 1. Level 2
    // 2. Level 1
    // 3. level 0
    // For plans with more than 3 levels, it will execute child lvels in same step
    // todo - this can be updated in future with methods in Adaptive execution to get subplans at shuffle boundaries
    private FragmentExecutionResult executeInStages(PrestoSparkStaticQueryExecution execution,
            SubPlan rootFragmentedPlan,
            TableWriteInfo tableWriteInfo)
    {
        // Level 2
        rootFragmentedPlan.getChildren().stream()
                .map(SubPlan::getChildren)
                .flatMap(Collection::stream)
                .forEach(subPlan -> excecuteSubPlanWithUncheckedException(execution, subPlan, tableWriteInfo));

        // Level 1
        rootFragmentedPlan.getChildren().stream()
                .forEach(subPlan -> excecuteSubPlanWithUncheckedException(execution, subPlan, tableWriteInfo));

        // Level 0 - Root
        return excecuteSubPlanWithUncheckedException(execution, rootFragmentedPlan, tableWriteInfo);
    }

    // This exists as forEach can't handle methods with checked exception
    private FragmentExecutionResult excecuteSubPlanWithUncheckedException(PrestoSparkStaticQueryExecution execution,
            SubPlan subPlan,
            TableWriteInfo tableWriteInfo)
    {
        try {
            return execution.executeFragment(subPlan, tableWriteInfo);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void assertRddAndMoreEquals(RddAndMore rddAndMore1, RddAndMore rddAndMore2)
    {
        assertEquals(rddAndMore1.getBroadcastDependencies().size(), rddAndMore2.getBroadcastDependencies().size());
        assertRddEquals(rddAndMore1.getRdd().rdd(), rddAndMore2.getRdd().rdd());
    }

    private void assertRddEquals(RDD rdd1, RDD rdd2)
    {
        assertEquals(rdd1.name(), rdd2.name());
        assertEquals(rdd1.getClass(), rdd2.getClass());
        assertEquals(rdd1.getDependencies().size(), rdd2.getDependencies().size());
        assertEquals(rdd1.getNumPartitions(), rdd2.getNumPartitions());

        // type specific assertions
        if (rdd1 instanceof PrestoSparkTaskRdd) {
            assertPrestoSparkTaskRddEquals((PrestoSparkTaskRdd) rdd1, (PrestoSparkTaskRdd) rdd2);
        }
        else if (rdd1 instanceof ShuffledRDD) {
            assertShuffledRddEquals((ShuffledRDD) rdd1, (ShuffledRDD) rdd2);
        }
    }

    private void assertShuffledRddEquals(ShuffledRDD shuffledRDD1, ShuffledRDD shuffledRDD2)
    {
        assertEquals(shuffledRDD1.getNumPartitions(), shuffledRDD2.getNumPartitions());
        assertRddEquals(shuffledRDD1.prev(), shuffledRDD2.prev());
        for (int i = 0; i < shuffledRDD1.getDependencies().size(); i++) {
            assertRddEquals(
                    ((Dependency) shuffledRDD1.getDependencies().apply(i)).rdd(),
                    ((Dependency) shuffledRDD2.getDependencies().apply(i)).rdd());
        }
    }

    private void assertPrestoSparkTaskRddEquals(PrestoSparkTaskRdd prestoSparkTaskRdd1, PrestoSparkTaskRdd prestoSparkTaskRdd2)
    {
        assertEquals(
                prestoSparkTaskRdd1.getShuffleInputRdds().size(),
                prestoSparkTaskRdd2.getShuffleInputRdds().size(),
                "Expected same number of shuffle inputs");

        assertEquals(
                prestoSparkTaskRdd1.getShuffleInputFragmentIds().stream().collect(Collectors.toSet()),
                prestoSparkTaskRdd2.getShuffleInputFragmentIds().stream().collect(Collectors.toSet()),
                "Expected same input fragment ids");

        assertEquals(
                prestoSparkTaskRdd1.getTaskSourceRdd() == null,
                prestoSparkTaskRdd2.getTaskSourceRdd() == null,
                "Expected both RDDs to either contain TaskSourceRdd or not contain it");

        for (int i = 0; i < prestoSparkTaskRdd1.getShuffleInputRdds().size(); i++) {
            assertRddEquals((RDD) prestoSparkTaskRdd1.getShuffleInputRdds().get(i), (RDD) prestoSparkTaskRdd2.getShuffleInputRdds().get(i));
        }

        for (int i = 0; i < prestoSparkTaskRdd1.getDependencies().size(); i++) {
            assertRddEquals(
                    ((Dependency) prestoSparkTaskRdd1.getDependencies().apply(i)).rdd(),
                    ((Dependency) prestoSparkTaskRdd2.getDependencies().apply(i)).rdd());
        }
    }
}
