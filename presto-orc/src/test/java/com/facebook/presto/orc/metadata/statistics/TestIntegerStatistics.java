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

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

public class TestIntegerStatistics
        extends AbstractRangeStatisticsTest<IntegerStatistics, Long>
{
    @Override
    protected IntegerStatistics getCreateStatistics(Long min, Long max)
    {
        return new IntegerStatistics(min, max);
    }

    @Test
    public void test()
    {
        assertMinMax(0L, 42L);
        assertMinMax(42L, 42L);
        assertMinMax(MIN_VALUE, 42L);
        assertMinMax(42L, MAX_VALUE);
        assertMinMax(MIN_VALUE, MAX_VALUE);
    }
}
