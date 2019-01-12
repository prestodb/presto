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

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.statistics.LongDecimalStatisticsBuilder.LONG_DECIMAL_VALUE_BYTES;
import static java.math.BigDecimal.ZERO;

public class TestDecimalStatistics
        extends AbstractRangeStatisticsTest<DecimalStatistics, BigDecimal>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalStatistics.class).instanceSize();
    private static final long BIG_DECIMAL_INSTANCE_SIZE = ClassLayout.parseClass(BigDecimal.class).instanceSize() + ClassLayout.parseClass(BigInteger.class).instanceSize() + sizeOf(new int[0]);

    private static final BigDecimal MEDIUM_VALUE = new BigDecimal("890.37492");
    private static final BigDecimal LARGE_POSITIVE_VALUE = new BigDecimal("123456789012345678901234567890.12345");
    private static final BigDecimal LARGE_NEGATIVE_VALUE = LARGE_POSITIVE_VALUE.negate();

    @Override
    protected DecimalStatistics getCreateStatistics(BigDecimal min, BigDecimal max)
    {
        return new DecimalStatistics(min, max, LONG_DECIMAL_VALUE_BYTES);
    }

    @Test
    public void test()
    {
        assertMinMax(ZERO, MEDIUM_VALUE);
        assertMinMax(MEDIUM_VALUE, MEDIUM_VALUE);
        assertMinMax(LARGE_NEGATIVE_VALUE, MEDIUM_VALUE);
        assertMinMax(MEDIUM_VALUE, LARGE_POSITIVE_VALUE);
        assertMinMax(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE);
    }

    @Test
    public void testRetainedSize()
    {
        assertRetainedSize(LARGE_NEGATIVE_VALUE, LARGE_NEGATIVE_VALUE, INSTANCE_SIZE + BIG_DECIMAL_INSTANCE_SIZE + LONG_DECIMAL_VALUE_BYTES);
        assertRetainedSize(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE, INSTANCE_SIZE + (BIG_DECIMAL_INSTANCE_SIZE + LONG_DECIMAL_VALUE_BYTES) * 2);
        assertRetainedSize(null, LARGE_POSITIVE_VALUE, INSTANCE_SIZE + BIG_DECIMAL_INSTANCE_SIZE + LONG_DECIMAL_VALUE_BYTES);
        assertRetainedSize(LARGE_NEGATIVE_VALUE, null, INSTANCE_SIZE + BIG_DECIMAL_INSTANCE_SIZE + LONG_DECIMAL_VALUE_BYTES);
        assertRetainedSize(null, null, INSTANCE_SIZE);
    }
}
