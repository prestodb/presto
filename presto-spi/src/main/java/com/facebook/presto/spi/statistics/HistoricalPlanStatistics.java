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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;

@ThriftStruct
public class HistoricalPlanStatistics
{
    // TODO: Add more data for historical runs, like avg/min/max over last N days/runs
    private final PlanStatistics lastRunStatistics;

    @ThriftConstructor
    public HistoricalPlanStatistics(@JsonProperty("lastRunStatistics") PlanStatistics lastRunStatistics)
    {
        this.lastRunStatistics = lastRunStatistics;
    }

    @ThriftField(1)
    public PlanStatistics getLastRunStatistics()
    {
        return lastRunStatistics;
    }

    @Override
    public String toString()
    {
        return format("lastRunStatistics: %s", lastRunStatistics);
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

        HistoricalPlanStatistics other = (HistoricalPlanStatistics) o;

        return Objects.equals(lastRunStatistics, other.lastRunStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastRunStatistics);
    }
}
