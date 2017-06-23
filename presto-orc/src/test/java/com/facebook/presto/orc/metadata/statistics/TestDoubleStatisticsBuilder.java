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
package com.facebook.presto.orc.metadata.statistics;

import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DOUBLE;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestDoubleStatisticsBuilder
        extends AbstractStatisticsBuilderTest<DoubleStatisticsBuilder, Double>
{
    public TestDoubleStatisticsBuilder()
    {
        super(DOUBLE, DoubleStatisticsBuilder::new, DoubleStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0.0, 0.0);
        assertMinMaxValues(42.42, 42.42);
        assertMinMaxValues(NEGATIVE_INFINITY, NEGATIVE_INFINITY);
        assertMinMaxValues(POSITIVE_INFINITY, POSITIVE_INFINITY);

        assertMinMaxValues(0.0, 42.42);
        assertMinMaxValues(42.42, 42.42);
        assertMinMaxValues(NEGATIVE_INFINITY, 42.42);
        assertMinMaxValues(42.42, POSITIVE_INFINITY);
        assertMinMaxValues(NEGATIVE_INFINITY, POSITIVE_INFINITY);
    }

    @Test
    public void testNanValue()
    {
        DoubleStatisticsBuilder statisticsBuilder = new DoubleStatisticsBuilder();
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1);
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2);
        statisticsBuilder.addValue(42.42);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3);

        statisticsBuilder = new DoubleStatisticsBuilder();
        statisticsBuilder.addValue(42.42);
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1, 42.42, 42.42);
        statisticsBuilder.addValue(NaN);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2);
        statisticsBuilder.addValue(42.42);
        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3);
    }
}
