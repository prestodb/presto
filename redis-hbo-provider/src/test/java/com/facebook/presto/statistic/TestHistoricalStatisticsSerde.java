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
package com.facebook.presto.statistic;

import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.testng.Assert.assertThrows;

public class TestHistoricalStatisticsSerde
{
    @Test
    public void TestSimpleHistoricalStatisticsEncoderDecoder()
    {
        HistoricalPlanStatistics samplePlanStatistics = new HistoricalPlanStatistics(ImmutableList.of(new HistoricalPlanStatisticsEntry(
                new PlanStatistics(Estimate.of(100), Estimate.of(1000), 1, Estimate.unknown(), Estimate.unknown()),
                ImmutableList.of(new PlanStatistics(Estimate.of(15000), Estimate.unknown(), 1, Estimate.unknown(), Estimate.unknown())))));
        HistoricalStatisticsSerde historicalStatisticsEncoderDecoder = new HistoricalStatisticsSerde();

        // Test PlanHash
        ByteBuffer encodedKey = historicalStatisticsEncoderDecoder.encodeKey("test");
        historicalStatisticsEncoderDecoder.decodeKey(encodedKey).equals("test");

        // Test Plan Statistics
        ByteBuffer encodedValue = historicalStatisticsEncoderDecoder.encodeValue(samplePlanStatistics);
        historicalStatisticsEncoderDecoder.decodeValue(encodedValue).equals(samplePlanStatistics);
    }

    @Test
    public void TestHistoricalPlanStatisticsEntryList()
    {
        List<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntryList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            historicalPlanStatisticsEntryList.add(new HistoricalPlanStatisticsEntry(new PlanStatistics(Estimate.of(i * 5), Estimate.of(i * 5), 1, Estimate.unknown(), Estimate.unknown()),
                    ImmutableList.of(new PlanStatistics(Estimate.of(100), Estimate.of(i), 0, Estimate.unknown(), Estimate.unknown()))));
        }
        HistoricalPlanStatistics samplePlanStatistics = new HistoricalPlanStatistics(historicalPlanStatisticsEntryList);
        HistoricalStatisticsSerde historicalStatisticsEncoderDecoder = new HistoricalStatisticsSerde();

        // Test Plan Statistics
        ByteBuffer encodedValue = historicalStatisticsEncoderDecoder.encodeValue(samplePlanStatistics);
        assert (historicalStatisticsEncoderDecoder.decodeValue(encodedValue).equals(samplePlanStatistics)) : "Decoded value is different from original encoded value ";
    }

    @Test
    public void TestHistoricalPlanStatisticsEmptyList()
    {
        HistoricalPlanStatistics emptySamplePlanStatistics = new HistoricalPlanStatistics(emptyList());
        HistoricalStatisticsSerde historicalStatisticsEncoderDecoder = new HistoricalStatisticsSerde();

        // Test Plan Statistics
        ByteBuffer encodedValue = historicalStatisticsEncoderDecoder.encodeValue(emptySamplePlanStatistics);
        assert (historicalStatisticsEncoderDecoder.decodeValue(encodedValue).equals(emptySamplePlanStatistics)) : "Decoded value is different from original encoded value ";
    }

    @Test
    public void TestPlanStatisticsList()
    {
        List<PlanStatistics> planStatisticsEntryList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            planStatisticsEntryList.add(new PlanStatistics(Estimate.of(i * 5), Estimate.of(i * 5), 1, Estimate.unknown(), Estimate.unknown()));
        }
        List<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntryList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            historicalPlanStatisticsEntryList.add(new HistoricalPlanStatisticsEntry(new PlanStatistics(Estimate.of(i * 5), Estimate.of(i * 5), 1, Estimate.unknown(), Estimate.unknown()),
                    planStatisticsEntryList));
        }
        HistoricalPlanStatistics samplePlanStatistics = new HistoricalPlanStatistics(historicalPlanStatisticsEntryList);
        HistoricalStatisticsSerde historicalStatisticsEncoderDecoder = new HistoricalStatisticsSerde();

        // Test Plan Statistics
        ByteBuffer encodedValue = historicalStatisticsEncoderDecoder.encodeValue(samplePlanStatistics);
        assert (historicalStatisticsEncoderDecoder.decodeValue(encodedValue).equals(samplePlanStatistics)) : "Decoded value is different from original encoded value ";
    }

    @Test
    public void TestHistoricalStatisticsDecodeValueException()
    {
        HistoricalStatisticsSerde historicalStatisticsEncoderDecoder = new HistoricalStatisticsSerde();

        // Test PlanHash
        ByteBuffer encodedKey = historicalStatisticsEncoderDecoder.encodeKey("test");
        assertThrows(
                RuntimeException.class,
                () -> historicalStatisticsEncoderDecoder.decodeValue(encodedKey));
    }
}
