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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;

import java.util.function.Function;

public class EmptyPlanStatisticsProvider
        implements ExternalPlanStatisticsProvider
{
    private static final EmptyPlanStatisticsProvider SINGLETON = new EmptyPlanStatisticsProvider();

    @Override
    public String getName()
    {
        return "default";
    }

    @Override
    public PlanStatistics getStats(PlanNode plan, Function<PlanNode, String> planPrinter, Function<TableScanNode, TableStatistics> tableStatisticsProvider)
    {
        return PlanStatistics.empty();
    }

    public static EmptyPlanStatisticsProvider getInstance()
    {
        return SINGLETON;
    }
}
