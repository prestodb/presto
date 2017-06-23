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

import java.math.BigDecimal;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DECIMAL;

public class TestLongDecimalStatisticsBuilder
        extends AbstractStatisticsBuilderTest<LongDecimalStatisticsBuilder, BigDecimal>
{
    private static final BigDecimal MEDIUM_VALUE = new BigDecimal("890.37492");
    private static final BigDecimal LARGE_POSITIVE_VALUE = new BigDecimal("123456789012345678901234567890.12345");
    private static final BigDecimal LARGE_NEGATIVE_VALUE = LARGE_POSITIVE_VALUE.negate();

    public TestLongDecimalStatisticsBuilder()
    {
        super(DECIMAL, LongDecimalStatisticsBuilder::new, LongDecimalStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(BigDecimal.ZERO, BigDecimal.ZERO);
        assertMinMaxValues(MEDIUM_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, LARGE_NEGATIVE_VALUE);
        assertMinMaxValues(LARGE_POSITIVE_VALUE, LARGE_POSITIVE_VALUE);

        assertMinMaxValues(BigDecimal.ZERO, MEDIUM_VALUE);
        assertMinMaxValues(MEDIUM_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(MEDIUM_VALUE, LARGE_POSITIVE_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE);
    }
}
