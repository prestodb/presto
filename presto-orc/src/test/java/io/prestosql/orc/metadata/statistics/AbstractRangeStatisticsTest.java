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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public abstract class AbstractRangeStatisticsTest<R extends RangeStatistics<T>, T>
{
    protected abstract R getCreateStatistics(T min, T max);

    protected void assertMinMax(T min, T max)
    {
        assertMinMaxStatistics(min, min);
        assertMinMaxStatistics(max, max);
        assertMinMaxStatistics(min, max);
        assertMinMaxStatistics(min, null);
        assertMinMaxStatistics(null, max);

        if (!min.equals(max)) {
            assertThrows(() -> getCreateStatistics(max, min));
        }
    }

    void assertRetainedSize(T min, T max, long expectedSizeInBytes)
    {
        assertEquals(getCreateStatistics(min, max).getRetainedSizeInBytes(), expectedSizeInBytes);
    }

    private void assertMinMaxStatistics(T min, T max)
    {
        R statistics = getCreateStatistics(min, max);
        assertEquals(statistics.getMin(), min);
        assertEquals(statistics.getMax(), max);

        assertEquals(statistics, statistics);
        assertEquals(statistics.hashCode(), statistics.hashCode());
    }
}
