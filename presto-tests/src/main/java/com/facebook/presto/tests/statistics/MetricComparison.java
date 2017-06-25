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
import static java.lang.Math.abs;
import static java.lang.String.format;

public class MetricComparison
{
    private final PlanNode planNode;
    private final Metric metric;
    private final Optional<Double> estimatedCost;
    private final Optional<Double> executionCost;
    private final double tolerance;

    public MetricComparison(PlanNode planNode, Metric metric, Optional<Double> estimatedCost, Optional<Double> executionCost, double tolerance)
    {
        this.planNode = planNode;
        this.metric = metric;
        this.estimatedCost = estimatedCost;
        this.executionCost = executionCost;
        this.tolerance = tolerance;
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
        return format("Metric [%s] - [%s] - estimated: [%s], real: [%s] - plan node: [%s]",
                metric, result(), print(estimatedCost), print(executionCost), planNode);
    }

    public Result result()
    {
        return estimatedCost
                .map(estimate -> executionCost
                        .map(execution -> estimateMatchesReality(estimate, execution) ? MATCH : DIFFER)
                        .orElse(NO_BASELINE))
                .orElse(NO_ESTIMATE);
    }

    private String print(Optional<Double> cost)
    {
        return cost.map(Object::toString).orElse("UNKNOWN");
    }

    private boolean estimateMatchesReality(double estimate, double execution)
    {
        return abs(execution - estimate) / execution < tolerance;
    }

    public enum Result
    {
        NO_ESTIMATE,
        NO_BASELINE,
        DIFFER,
        MATCH
    }
}
