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

import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.Metrics.distinctValuesCount;
import static com.facebook.presto.tests.statistics.Metrics.highValue;
import static com.facebook.presto.tests.statistics.Metrics.lowValue;
import static com.facebook.presto.tests.statistics.Metrics.nullsFraction;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class StatisticsAssertion
{
    private final QueryRunner runner;

    public StatisticsAssertion(QueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public void check(@Language("SQL") String query, Consumer<Checks> checksBuilderConsumer)
    {
        Checks checks = new Checks();
        checksBuilderConsumer.accept(checks);
        checks.run(query, runner);
    }

    private static class MetricsCheck<T>
    {
        public final Metric<T> metric;
        public final MetricComparisonStrategy<T> strategy;
        public final MetricComparison.Result expectedComparisonResult;

        public MetricsCheck(Metric<T> metric, MetricComparisonStrategy<T> strategy, MetricComparison.Result expectedComparisonResult)
        {
            this.metric = metric;
            this.strategy = strategy;
            this.expectedComparisonResult = expectedComparisonResult;
        }
    }

    public static class Checks
    {
        private final List<MetricsCheck<?>> checks = new ArrayList<>();

        public Checks verifyExactColumnStatistics(String columnName)
        {
            verifyColumnStatistics(columnName, noError());
            return this;
        }

        public Checks verifyColumnStatistics(String columnName, MetricComparisonStrategy<Double> strategy)
        {
            estimate(nullsFraction(columnName), strategy);
            estimate(distinctValuesCount(columnName), strategy);
            estimate(lowValue(columnName), strategy);
            estimate(highValue(columnName), strategy);
            return this;
        }

        public <T> Checks estimate(Metric<T> metric, MetricComparisonStrategy<T> strategy)
        {
            checks.add(new MetricsCheck<>(metric, strategy, MATCH));
            return this;
        }

        public <T> Checks noEstimate(Metric<T> metric)
        {
            checks.add(new MetricsCheck<>(metric, (actual, estimate) -> true, NO_ESTIMATE));
            return this;
        }

        public <T> Checks noBaseline(Metric<T> metric)
        {
            checks.add(new MetricsCheck<>(metric, (actual, estimate) -> true, NO_BASELINE));
            return this;
        }

        void run(@Language("SQL") String query, QueryRunner runner)
        {
            Set<Metric<?>> metrics = checks.stream()
                    .map(check -> check.metric)
                    .collect(toImmutableSet());
            Set<MetricComparison<?>> metricComparisons = metricComparisons(query, runner, metrics);
            for (MetricsCheck check : checks) {
                testMetrics(check.metric, metricComparison -> metricComparison.result(check.strategy) == check.expectedComparisonResult, metricComparisons);
            }
        }

        private Set<MetricComparison<?>> metricComparisons(@Language("SQL") String query, QueryRunner queryRunner, Set<Metric<?>> metrics)
        {
            return new MetricComparator().getMetricComparisons(query, queryRunner, metrics);
        }

        private Checks testMetrics(Metric metric, Predicate<MetricComparison> assertCondition, Set<MetricComparison<?>> metricComparisons)
        {
            List<MetricComparison> testMetrics = metricComparisons.stream()
                    .filter(metricComparison -> metricComparison.getMetric().equals(metric))
                    .collect(toImmutableList());
            assertTrue(testMetrics.size() > 0, "No metric found for: " + metric);
            assertTrue(testMetrics.stream().allMatch(assertCondition), "Following metrics do not match: " + testMetrics);
            return this;
        }
    }
}
