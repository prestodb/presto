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

import com.facebook.presto.common.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestCountStatisticsBuilder
{
    @Test
    public void testNoValues()
    {
        CountStatisticsBuilder statisticsBuilder = new CountStatisticsBuilder();
        ColumnStatistics columnStatistics = statisticsBuilder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 0);
    }

    @Test
    public void testAddValue()
    {
        CountStatisticsBuilder statisticsBuilder = new CountStatisticsBuilder();
        statisticsBuilder.addValue();
        statisticsBuilder.addValue();
        ColumnStatistics columnStatistics = statisticsBuilder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 2);
    }

    @Test
    public void testAddBlockValues()
    {
        Block block = BIGINT.createBlockBuilder(null, 3)
                .writeLong(3L)
                .appendNull()
                .writeLong(10L);

        CountStatisticsBuilder statisticsBuilder = new CountStatisticsBuilder();
        statisticsBuilder.addBlock(BIGINT, block);

        ColumnStatistics columnStatistics = statisticsBuilder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 2);
    }

    @Test
    public void testAddValueByPosition()
    {
        Block block = BIGINT.createBlockBuilder(null, 3)
                .writeLong(3L)
                .appendNull()
                .writeLong(10L);

        CountStatisticsBuilder statisticsBuilder = new CountStatisticsBuilder();
        statisticsBuilder.addValue(BIGINT, block, 0);
        statisticsBuilder.addValue(BIGINT, block, 1);

        ColumnStatistics columnStatistics = statisticsBuilder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 1);
    }
}
