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
package io.prestosql.orc.metadata.statistics;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.BOOLEAN;
import static io.prestosql.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static io.prestosql.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static org.testng.Assert.assertEquals;

public class TestBooleanStatisticsBuilder
        extends AbstractStatisticsBuilderTest<BooleanStatisticsBuilder, Boolean>
{
    public TestBooleanStatisticsBuilder()
    {
        super(BOOLEAN, BooleanStatisticsBuilder::new, BooleanStatisticsBuilder::addValue);
    }

    @Test
    public void testAddValueValues()
    {
        // add false then true
        BooleanStatisticsBuilder statisticsBuilder = new BooleanStatisticsBuilder();
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 0, 0);

        statisticsBuilder.addValue(false);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 1, 0);

        statisticsBuilder.addValue(false);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 2, 0);

        statisticsBuilder.addValue(true);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 3, 1);

        statisticsBuilder.addValue(true);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 4, 2);

        // add true then false
        statisticsBuilder = new BooleanStatisticsBuilder();
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 0, 0);

        statisticsBuilder.addValue(true);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 1, 1);

        statisticsBuilder.addValue(true);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 2, 2);

        statisticsBuilder.addValue(false);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 3, 2);

        statisticsBuilder.addValue(false);
        assertBooleanStatistics(statisticsBuilder.buildColumnStatistics(), 4, 2);
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        BooleanStatisticsBuilder statisticsBuilder = new BooleanStatisticsBuilder();
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBooleanStatistics(statisticsList, 0, 0);

        statisticsBuilder.addValue(false);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBooleanStatistics(statisticsList, 1, 0);

        statisticsBuilder.addValue(false);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBooleanStatistics(statisticsList, 3, 0);

        statisticsBuilder.addValue(true);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBooleanStatistics(statisticsList, 6, 1);

        statisticsBuilder.addValue(true);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBooleanStatistics(statisticsList, 10, 3);
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(BOOLEAN_VALUE_BYTES, ImmutableList.of(true));
        assertMinAverageValueBytes(BOOLEAN_VALUE_BYTES, ImmutableList.of(false));
        assertMinAverageValueBytes(BOOLEAN_VALUE_BYTES, ImmutableList.of(true, true, false, true));
    }

    private void assertMergedBooleanStatistics(List<ColumnStatistics> statisticsList, int expectedNumberOfValues, int trueValueCount)
    {
        assertBooleanStatistics(mergeColumnStatistics(statisticsList), expectedNumberOfValues, trueValueCount);

        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), expectedNumberOfValues + 10);
    }

    private void assertBooleanStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, int trueValueCount)
    {
        if (expectedNumberOfValues > 0) {
            assertColumnStatistics(columnStatistics, expectedNumberOfValues, null, null);
            assertEquals(columnStatistics.getBooleanStatistics().getTrueValueCount(), trueValueCount);
        }
    }
}
