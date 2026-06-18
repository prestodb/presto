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

import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PlanStatisticsWithSourceInfo
{
    private final PlanNodeId id;
    private final PlanStatistics planStatistics;
    private final SourceInfo sourceInfo;

    public PlanStatisticsWithSourceInfo(PlanNodeId id, PlanStatistics planStatistics, SourceInfo sourceInfo)
    {
        this.id = requireNonNull(id, "id is null");
        this.planStatistics = requireNonNull(planStatistics, "planStatistics is null");
        this.sourceInfo = requireNonNull(sourceInfo, "sourceInfo is null");
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public PlanStatistics getPlanStatistics()
    {
        return planStatistics;
    }

    public SourceInfo getSourceInfo()
    {
        return sourceInfo;
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
        PlanStatisticsWithSourceInfo that = (PlanStatisticsWithSourceInfo) o;
        return id.equals(that.id) && planStatistics.equals(that.planStatistics) && sourceInfo.equals(that.sourceInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, planStatistics, sourceInfo);
    }
}
