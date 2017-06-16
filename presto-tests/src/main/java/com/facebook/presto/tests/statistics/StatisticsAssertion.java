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

package com.facebook.presto.tests.statistics;

import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class StatisticsAssertion
{
    private final DistributedQueryRunner runner;

    public StatisticsAssertion(DistributedQueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public Result check(@Language("SQL") String query)
    {
        return new Result(metricComparisons(query));
    }

    public List<MetricComparison> metricComparisons(@Language("SQL") String query)
    {
        String queryId = runner.executeWithQueryId(runner.getDefaultSession(), query).getQueryId();
        Plan queryPlan = runner.getQueryPlan(new QueryId(queryId));
        StageInfo stageInfo = runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();
        return new MetricComparator().getMetricComparisons(queryPlan, stageInfo);
    }

    public static class Result
    {
        private final List<MetricComparison> metricComparisons;

        public Result(List<MetricComparison> metricComparisons)
        {
            Preconditions.checkArgument(metricComparisons.isEmpty(), "No metric ");
            this.metricComparisons = ImmutableList.copyOf(metricComparisons);
        }

        public void matches()
        {
            metricComparisons.stream()
                    .map(MetricComparison::result)
                    .forEach(result -> assertEquals(result, MATCH));
        }
    }
}
