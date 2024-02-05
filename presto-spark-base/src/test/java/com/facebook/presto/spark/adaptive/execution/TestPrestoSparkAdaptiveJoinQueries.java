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
package com.facebook.presto.spark.adaptive.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spark.TestPrestoSparkJoinQueries;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;

public class TestPrestoSparkAdaptiveJoinQueries
        extends TestPrestoSparkJoinQueries
{
    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .setSystemProperty(ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED, "true")
                .build();
    }

    @DataProvider(name = "optimize_hash_generation")
    public Object[][] optimizeHashGeneration()
    {
        return new Object[][] {{"true"}, {"false"}};
    }

    @Test(dataProvider = "optimize_hash_generation")
    public void testQuerySucceedsWithAQE(String optimizeHashGeneration)
    {
        // we don't add a memory limit and test that the query succeeds with aqe and fails without
        // because the memory used by the PrestoSparkRowOutputOperator is very variable and too
        // close to the memory used by the build side of the join to allow such a test to run reliably
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "false")
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();

        assertQuery(session, "SELECT * FROM nation n JOIN orders o ON n.nationkey = o.orderkey");
    }
}
