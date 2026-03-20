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
import org.testng.annotations.Ignore;
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
        // System.out.println("GLayout:" + GraphLayout.parseInstance(stats).toPrintable());
        // GLayout:com.facebook.presto.spi.statistics.ColumnStatistics object externals:
        //          ADDRESS       SIZE TYPE                                                PATH                           VALUE
        //        7f2f671e0         16 java.util.Optional                                  .stringRange                   (object)
        //        7f2f671f0   69278752 (something else)                                    (somewhere else)               (something else)
        //        7f7178e10         24 com.facebook.presto.spi.statistics.Estimate         .dataSize                      (object)
        //        7f7178e28         24 com.facebook.presto.spi.statistics.Estimate         .distinctValuesCount           (object)
        //        7f7178e40         32 com.facebook.presto.spi.statistics.DoubleRange      .range.value                   (object)
        //        7f7178e60         16 java.util.Optional                                  .range                         (object)
        //        7f7178e70         24 com.facebook.presto.spi.statistics.Estimate         .nullsFraction                 (object)
        //        7f7178e88         40 com.facebook.presto.spi.statistics.ColumnStatistics

        // In the above we see, when histograms are not present the size of Optional is also omitted.
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
     * Following test is ignored, because GraphLayout's estimation could not be predicted.
     */
    @Ignore
    public void testColumnStatisticsEstimatedSizeAccuracyWithStringRange()
    {
        ColumnStatistics stats = ColumnStatistics.builder()
                .setDataSize(Estimate.of(100))
                .setDistinctValuesCount(Estimate.of(1))
                .setStringRange(new StringRange("", "zZxZop"))
                .setNullsFraction(Estimate.of(0.1))
                .build();
        long actualSize = GraphLayout.parseInstance(stats).totalSize();
        // System.out.println("GLayout with stringRange:" + GraphLayout.parseInstance(stats).toPrintable());
        // 3f900bad8         24 [B                                                  .stringRange.value.max.value   [122, 90, 120, 90, 111, 112]
        // System.out.println("size of string range " + GraphLayout.parseInstance(new StringRange("", "")).toPrintable());
        // size of string range com.facebook.presto.spi.statistics.StringRange object externals:
        //          ADDRESS       SIZE TYPE                                           PATH                           VALUE
        //        5d0dc3c58         24 com.facebook.presto.spi.statistics.StringRange                                (object)
        //        5d0dc3c70   67355536 (something else)                               (somewhere else)               (something else)
        //        5d4e00000         16 [B                                             .min.value                     []
        //        5d4e00010         24 java.lang.String                               .min                           (object)
        // Why Max is not included?
        // When does it increase this 24 byte size? seems to be not related to the content of min.
        //
        assertEquals(actualSize, stats.getEstimatedSize());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeScenarioColumnStatisticsWithBothStringAndDoubleRange()
    {
        ColumnStatistics.builder()
                .setDataSize(Estimate.of(100))
                .setDistinctValuesCount(Estimate.of(1))
                .setStringRange(new StringRange("", "zZxZ"))
                .setRange(new DoubleRange(0, 1))
                .setNullsFraction(Estimate.of(0.1))
                .build();
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
