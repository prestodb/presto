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

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static org.testng.Assert.assertEquals;

public class TestColumnStatistics
{
    /**
     * Test for all non-histogram stats that memory is accounted accurately
     */
    @Test
    public void testColumnStatisticsEstimatedSizeAccuracySimple()
    {
        ColumnStatistics stats = ColumnStatistics.builder()
                .setDataSize(Estimate.of(100))
                .setDistinctValuesCount(Estimate.of(1))
                .setRange(new DoubleRange(100, 100))
                .setNullsFraction(Estimate.of(0.1))
                .build();

        // test without histogram
        long actualSize = GraphLayout.parseInstance(stats).totalSize();
        assertEquals(actualSize, stats.getEstimatedSize());
    }

    /**
     * Test for when histogram is included but has little-to-no memory usage.
     */
    @Test
    public void testColumnStatisticsEstimatedSizeAccuracyHistogramEmpty()
    {
        ColumnStatistics stats = ColumnStatistics.builder()
                .setDataSize(Estimate.of(100))
                .setDistinctValuesCount(Estimate.of(1))
                .setRange(new DoubleRange(100, 100))
                .setNullsFraction(Estimate.of(0.1))
                .setHistogram(Optional.of(new ConnectorHistogram()
                {
                    @Override
                    public Estimate cumulativeProbability(double value, boolean inclusive)
                    {
                        return null;
                    }

                    @Override
                    public Estimate inverseCumulativeProbability(double percentile)
                    {
                        return null;
                    }

                    @Override
                    public long getEstimatedSize()
                    {
                        return ClassLayout.parseClass(this.getClass()).instanceSize() * 2L;
                    }
                }))
                .build();
        long actualSize = GraphLayout.parseInstance(stats).totalSize();
        assertEquals(actualSize, stats.getEstimatedSize());
    }

    /**
     * Test for when histogram is included and has a significant memory footprint
     */
    @Test
    public void testColumnStatisticsEstimatedSizeAccuracyHistogram()
    {
        ColumnStatistics stats = ColumnStatistics.builder()
                .setDataSize(Estimate.of(100))
                .setDistinctValuesCount(Estimate.of(1))
                .setRange(new DoubleRange(100, 100))
                .setNullsFraction(Estimate.of(0.1))
                .setHistogram(Optional.of(new ConnectorHistogram()
                {
                    final byte[] memory = new byte[4096];

                    @Override
                    public Estimate cumulativeProbability(double value, boolean inclusive)
                    {
                        return null;
                    }

                    @Override
                    public Estimate inverseCumulativeProbability(double percentile)
                    {
                        return null;
                    }

                    @Override
                    public long getEstimatedSize()
                    {
                        return sizeOf(memory) + (ClassLayout.parseClass(this.getClass()).instanceSize() * 2L);
                    }
                }))
                .build();

        // test histogram with fields histogram
        long actualSize = GraphLayout.parseInstance(stats).totalSize();
        double error = actualSize * 1E-2;
        assertEquals(actualSize, stats.getEstimatedSize(), error);
    }
}
