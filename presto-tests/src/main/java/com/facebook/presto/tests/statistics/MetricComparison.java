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

import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static java.lang.String.format;

public class MetricComparison<T>
{
    private final PlanNode planNode;
    private final Metric metric;
    private final Optional<T> estimatedValue;
    private final Optional<T> actualValue;

    public MetricComparison(PlanNode planNode, Metric metric, Optional<T> estimatedValue, Optional<T> actualValue)
    {
        this.planNode = planNode;
        this.metric = metric;
        this.estimatedValue = estimatedValue;
        this.actualValue = actualValue;
    }

    public Metric getMetric()
    {
        return metric;
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    @Override
    public String toString()
    {
        return format("Metric [%s] - estimated: [%s], real: [%s] - plan node: [%s]",
                metric, print(estimatedValue), print(actualValue), planNode);
    }

    public Result result(MetricComparisonStrategy<T> metricComparisonStrategy)
    {
        if (!estimatedValue.isPresent() && !actualValue.isPresent()) {
            return MATCH;
        }
        return estimatedValue
                .map(estimate -> actualValue
                        .map(execution -> metricComparisonStrategy.matches(execution, estimate) ? MATCH : DIFFER)
                        .orElse(NO_BASELINE))
                .orElse(NO_ESTIMATE);
    }

    private String print(Optional<T> cost)
    {
        return cost.map(Object::toString).orElse("UNKNOWN");
    }

    public enum Result
    {
        NO_ESTIMATE,
        NO_BASELINE,
        DIFFER,
        MATCH
    }
}
