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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.testing.MaterializedRow;

import java.util.Objects;
import java.util.Optional;

public abstract class Metric<T>
{
    public abstract Optional<T> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext);

    public abstract Optional<T> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext);

    public abstract String getComputingAggregationSql();

    public abstract String getName();

    // name based equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Metric<?> metric = (Metric<?>) o;
        return Objects.equals(getName(), metric.getName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getName());
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
