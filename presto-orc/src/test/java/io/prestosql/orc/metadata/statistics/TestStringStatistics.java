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

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;

public class TestStringStatistics
        extends AbstractRangeStatisticsTest<StringStatistics, Slice>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StringStatistics.class).instanceSize();

    // U+0000 to U+D7FF
    private static final Slice LOW_BOTTOM_VALUE = utf8Slice("foo \u0000");
    private static final Slice LOW_TOP_VALUE = utf8Slice("foo \uD7FF");
    // U+E000 to U+FFFF
    private static final Slice MEDIUM_BOTTOM_VALUE = utf8Slice("foo \uE000");
    private static final Slice MEDIUM_TOP_VALUE = utf8Slice("foo \uFFFF");
    // U+10000 to U+10FFFF
    private static final Slice HIGH_BOTTOM_VALUE = utf8Slice("foo \uD800\uDC00");
    private static final Slice HIGH_TOP_VALUE = utf8Slice("foo \uDBFF\uDFFF");

    @Override
    protected StringStatistics getCreateStatistics(Slice min, Slice max)
    {
        // a fake sum is ok
        return new StringStatistics(min, max, 100L);
    }

    @Test
    public void test()
    {
        assertMinMax(EMPTY_SLICE, LOW_BOTTOM_VALUE);
        assertMinMax(EMPTY_SLICE, LOW_TOP_VALUE);
        assertMinMax(EMPTY_SLICE, MEDIUM_BOTTOM_VALUE);
        assertMinMax(EMPTY_SLICE, MEDIUM_TOP_VALUE);
        assertMinMax(EMPTY_SLICE, HIGH_BOTTOM_VALUE);
        assertMinMax(EMPTY_SLICE, HIGH_TOP_VALUE);

        assertMinMax(LOW_BOTTOM_VALUE, LOW_TOP_VALUE);
        assertMinMax(LOW_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);
        assertMinMax(LOW_BOTTOM_VALUE, MEDIUM_TOP_VALUE);
        assertMinMax(LOW_BOTTOM_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMax(LOW_BOTTOM_VALUE, HIGH_TOP_VALUE);

        assertMinMax(LOW_TOP_VALUE, MEDIUM_BOTTOM_VALUE);
        assertMinMax(LOW_TOP_VALUE, MEDIUM_TOP_VALUE);
        assertMinMax(LOW_TOP_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMax(LOW_TOP_VALUE, HIGH_TOP_VALUE);

        assertMinMax(MEDIUM_BOTTOM_VALUE, MEDIUM_TOP_VALUE);
        assertMinMax(MEDIUM_BOTTOM_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMax(MEDIUM_BOTTOM_VALUE, HIGH_TOP_VALUE);

        assertMinMax(MEDIUM_TOP_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMax(MEDIUM_TOP_VALUE, HIGH_TOP_VALUE);

        assertMinMax(HIGH_BOTTOM_VALUE, HIGH_TOP_VALUE);
    }

    @Test
    public void testRetainedSize()
    {
        assertRetainedSize(EMPTY_SLICE, LOW_BOTTOM_VALUE, INSTANCE_SIZE + EMPTY_SLICE.getRetainedSize() + LOW_BOTTOM_VALUE.getRetainedSize());
        assertRetainedSize(LOW_TOP_VALUE, LOW_TOP_VALUE, INSTANCE_SIZE + LOW_TOP_VALUE.getRetainedSize());
        assertRetainedSize(EMPTY_SLICE, EMPTY_SLICE, INSTANCE_SIZE + EMPTY_SLICE.getRetainedSize());
        assertRetainedSize(MEDIUM_TOP_VALUE, HIGH_BOTTOM_VALUE, INSTANCE_SIZE + MEDIUM_TOP_VALUE.getRetainedSize() + HIGH_BOTTOM_VALUE.getRetainedSize());
        assertRetainedSize(null, HIGH_BOTTOM_VALUE, INSTANCE_SIZE + HIGH_BOTTOM_VALUE.getRetainedSize());
        assertRetainedSize(EMPTY_SLICE, null, INSTANCE_SIZE + EMPTY_SLICE.getRetainedSize());
        assertRetainedSize(null, null, INSTANCE_SIZE);
    }
}
