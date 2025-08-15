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
package com.facebook.presto.cost;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.function.Description;

import javax.validation.constraints.Min;

public class HistoryBasedOptimizationConfig
{
    private int maxLastRunsHistory = 10;
    private double historyMatchingThreshold = 0.1;

    @Min(1)
    public int getMaxLastRunsHistory()
    {
        return maxLastRunsHistory;
    }

    @Config("hbo.max-last-runs-history")
    @Description("Number of last runs for which historical stats are stored")
    public HistoryBasedOptimizationConfig setMaxLastRunsHistory(int maxLastRunsHistory)
    {
        this.maxLastRunsHistory = maxLastRunsHistory;
        return this;
    }

    @Min(0)
    public double getHistoryMatchingThreshold()
    {
        return historyMatchingThreshold;
    }

    @Config("hbo.history-matching-threshold")
    @Description("Historical statistics are reused when input table sizes are within this threshold")
    public HistoryBasedOptimizationConfig setHistoryMatchingThreshold(double historyMatchingThreshold)
    {
        this.historyMatchingThreshold = historyMatchingThreshold;
        return this;
    }
}
