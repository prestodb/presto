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
package io.prestosql.tests.statistics;

import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.SymbolStatsEstimate;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class Metrics
{
    private Metrics() {}

    public static final Metric OUTPUT_ROW_COUNT = new Metric()
    {
        @Override
        public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
        {
            return asOptional(planNodeStatsEstimate.getOutputRowCount());
        }

        @Override
        public OptionalDouble getValueFromAggregationQueryResult(Object value)
        {
            return OptionalDouble.of(((Number) value).doubleValue());
        }

        @Override
        public String getComputingAggregationSql()
        {
            return "count(*)";
        }

        @Override
        public String toString()
        {
            return "OUTPUT_ROW_COUNT";
        }
    };

    public static Metric nullsFraction(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getNullsFraction());
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return OptionalDouble.of(((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "(count(*) filter(where " + columnName + " is null)) / cast(count(*) as double)";
            }

            @Override
            public String toString()
            {
                return "nullsFraction(\"" + columnName + "\")";
            }
        };
    }

    public static Metric distinctValuesCount(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getDistinctValuesCount());
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return OptionalDouble.of(((Number) value).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "count(distinct " + columnName + ")";
            }

            @Override
            public String toString()
            {
                return "distinctValuesCount(\"" + columnName + "\")";
            }
        };
    }

    public static Metric lowValue(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double lowValue = getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getLowValue();
                if (isInfinite(lowValue)) {
                    return OptionalDouble.empty();
                }
                return OptionalDouble.of(lowValue);
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return Optional.ofNullable(value)
                        .map(Number.class::cast)
                        .map(Number::doubleValue)
                        .map(OptionalDouble::of)
                        .orElseGet(OptionalDouble::empty);
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "try_cast(min(" + columnName + ") as double)";
            }

            @Override
            public String toString()
            {
                return "lowValue(\"" + columnName + "\")";
            }
        };
    }

    public static Metric highValue(String columnName)
    {
        return new Metric()
        {
            @Override
            public OptionalDouble getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                double highValue = getSymbolStatistics(planNodeStatsEstimate, columnName, statsContext).getHighValue();
                if (isInfinite(highValue)) {
                    return OptionalDouble.empty();
                }
                return OptionalDouble.of(highValue);
            }

            @Override
            public OptionalDouble getValueFromAggregationQueryResult(Object value)
            {
                return Optional.ofNullable(value)
                        .map(Number.class::cast)
                        .map(Number::doubleValue)
                        .map(OptionalDouble::of)
                        .orElseGet(OptionalDouble::empty);
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "max(try_cast(" + columnName + " as double))";
            }

            @Override
            public String toString()
            {
                return "highValue(\"" + columnName + "\")";
            }
        };
    }

    private static SymbolStatsEstimate getSymbolStatistics(PlanNodeStatsEstimate planNodeStatsEstimate, String columnName, StatsContext statsContext)
    {
        return planNodeStatsEstimate.getSymbolStatistics(statsContext.getSymbolForColumn(columnName));
    }

    private static OptionalDouble asOptional(double value)
    {
        return isNaN(value) ? OptionalDouble.empty() : OptionalDouble.of(value);
    }
}
