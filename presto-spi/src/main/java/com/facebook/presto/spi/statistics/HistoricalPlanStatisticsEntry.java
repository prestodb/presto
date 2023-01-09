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
package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Objects;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

@ThriftStruct
public class HistoricalPlanStatisticsEntry
{
    private final PlanStatistics planStatistics;
    // Size of input tables when plan statistics was recorded. This list will be sorted by input tables canonical order.
    private final List<PlanStatistics> inputTableStatistics;

    @ThriftConstructor
    public HistoricalPlanStatisticsEntry(PlanStatistics planStatistics, List<PlanStatistics> inputTableStatistics)
    {
        // Check for nulls, to make it thrift backwards compatible
        this.planStatistics = planStatistics == null ? PlanStatistics.empty() : planStatistics;
        this.inputTableStatistics = unmodifiableList(inputTableStatistics == null ? emptyList() : inputTableStatistics);
    }

    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PlanStatistics getPlanStatistics()
    {
        return planStatistics;
    }

    @ThriftField(value = 2, requiredness = OPTIONAL)
    public List<PlanStatistics> getInputTableStatistics()
    {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HistoricalPlanStatisticsEntry that = (HistoricalPlanStatisticsEntry) o;
        return Objects.equals(planStatistics, that.planStatistics) && Objects.equals(inputTableStatistics, that.inputTableStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(planStatistics, inputTableStatistics);
    }

    @Override
    public String toString()
    {
        return format("HistoricalPlanStatisticsEntry{planStatistics=%s, inputTableStatistics=%s}", planStatistics, inputTableStatistics);
    }
}
