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
import com.facebook.presto.spi.statistics.Estimate;

import java.util.function.Function;

public enum Metric
{
    OUTPUT_ROW_COUNT(PlanNodeStatsEstimate::getOutputRowCount),
    OUTPUT_SIZE_BYTES(PlanNodeStatsEstimate::getOutputSizeInBytes);

    private final Function<PlanNodeStatsEstimate, Estimate> extractor;

    Metric(Function<PlanNodeStatsEstimate, Estimate> extractor)
    {
        this.extractor = extractor;
    }

    Estimate getValue(PlanNodeStatsEstimate planNodeStatsEstimate)
    {
        return extractor.apply(planNodeStatsEstimate);
    }
}
