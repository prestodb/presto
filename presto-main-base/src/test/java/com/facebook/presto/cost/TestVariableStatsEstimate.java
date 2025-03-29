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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.statistics.UniformDistributionHistogram;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVariableStatsEstimate
{
    @Test
    public void testSkipHistogramSerialization()
    {
        JsonCodec<VariableStatsEstimate> codec = JsonCodec.jsonCodec(VariableStatsEstimate.class);
        VariableStatsEstimate estimate = VariableStatsEstimate.builder()
                .setAverageRowSize(100)
                .setDistinctValuesCount(100)
                .setStatisticsRange(new StatisticRange(55, 65, 100))
                .setHistogram(Optional.of(new UniformDistributionHistogram(55, 65)))
                .setNullsFraction(0.1)
                .build();
        VariableStatsEstimate serialized = codec.fromBytes(codec.toBytes(estimate));
        assertEquals(serialized.getAverageRowSize(), estimate.getAverageRowSize());
        assertEquals(serialized.getDistinctValuesCount(), estimate.getDistinctValuesCount());
        assertEquals(serialized.getLowValue(), estimate.getLowValue());
        assertEquals(serialized.getHighValue(), estimate.getHighValue());
        assertTrue(estimate.getHistogram().isPresent());
        assertFalse(serialized.getHistogram().isPresent());
    }
}
