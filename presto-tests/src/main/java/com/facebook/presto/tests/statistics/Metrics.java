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
import com.facebook.presto.cost.SymbolStatsEstimate;
import com.facebook.presto.testing.MaterializedRow;

import java.util.Optional;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class Metrics
{
    private Metrics() {}

    public static final Metric<Double> OUTPUT_ROW_COUNT = new Metric<Double>()
    {
        @Override
        public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
        {
            return asOptional(planNodeStatsEstimate.getOutputRowCount());
        }

        @Override
        public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
        {
            return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
        }

        @Override
        public String getComputingAggregationSql()
        {
            return "count(*)";
        }

        @Override
        public String getName()
        {
            return "OUTPUT_ROW_COUNT";
        }
    };

    public static Metric<Double> nullsFraction(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getNullsFraction());
            }

            @Override
            public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "(count(*) filter(where " + columnName + " is null)) / cast(count(*) as double)";
            }

            @Override
            public String getName()
            {
                return "NULLS_FRACTION(" + columnName + ")";
            }
        };
    }

    public static Metric<Double> distinctValuesCount(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getDistinctValuesCount());
            }

            @Override
            public Optional getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "count(distinct " + columnName + ")";
            }

            @Override
            public String getName()
            {
                return "DISTINCT_VALUES_COUNT(" + columnName + ")";
            }
        };
    }

    public static Metric<Double> lowValue(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double lowValue = getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getLowValue();
                if (isInfinite(lowValue)) {
                    return Optional.empty();
                }
                else {
                    return Optional.of(lowValue);
                }
            }

            @Override
            public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.ofNullable(aggregationQueryResult.getField(fieldId)).map(value -> ((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "try_cast(min(" + columnName + ") as double)";
            }

            @Override
            public String getName()
            {
                return "LOW_VALUE(" + columnName + ")";
            }
        };
    }

    public static Metric<Double> highValue(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double highValue = getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getHighValue();
                if (isInfinite(highValue)) {
                    return Optional.empty();
                }
                else {
                    return Optional.of(highValue);
                }
            }

            @Override
            public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.ofNullable(aggregationQueryResult.getField(fieldId)).map(value -> ((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "max(try_cast(" + columnName + " as double))";
            }

            @Override
            public String getName()
            {
                return "HIGH_VALUE(" + columnName + ")";
            }
        };
    }

    private static SymbolStatsEstimate getSymbolStatistics(PlanNodeStatsEstimate planNodeStatsEstimate, String columnName, StatsContext statsContext)
    {
        return planNodeStatsEstimate.getSymbolStatistics(statsContext.getSymbolForColumn(columnName));
    }

    private static Optional<Double> asOptional(double value)
    {
        return isNaN(value) ? Optional.empty() : Optional.of(value);
    }
}
