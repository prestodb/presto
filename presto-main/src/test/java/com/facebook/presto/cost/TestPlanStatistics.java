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

import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.statistics.PlanStatistics.toConfidenceLevel;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPlanStatistics
{
    @Test
    public void testGetEnumConfidenceHigh()
    {
        PlanStatistics planStatistics1 = new PlanStatistics(Estimate.of(100), Estimate.of(1000), 1, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());
        PlanStatistics planStatistics2 = new PlanStatistics(Estimate.of(100), Estimate.of(1000), .5, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());

        assertEquals(toConfidenceLevel(planStatistics1.getConfidence()), HIGH);
        assertEquals(toConfidenceLevel(planStatistics2.getConfidence()), HIGH);
    }

    @Test
    public void testGetEnumConfidenceLow()
    {
        PlanStatistics planStatistics1 = new PlanStatistics(Estimate.of(100), Estimate.of(1000), 0, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());

        assertEquals(toConfidenceLevel(planStatistics1.getConfidence()), LOW);
    }
}
