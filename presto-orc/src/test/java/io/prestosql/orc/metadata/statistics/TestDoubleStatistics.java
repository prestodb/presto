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

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertThrows;

public class TestDoubleStatistics
        extends AbstractRangeStatisticsTest<DoubleStatistics, Double>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DoubleStatistics.class).instanceSize();

    @Override
    protected DoubleStatistics getCreateStatistics(Double min, Double max)
    {
        return new DoubleStatistics(min, max);
    }

    @Test
    public void test()
    {
        assertMinMax(0.0, 42.42);
        assertMinMax(-42.42, 0.0);
        assertMinMax(-42.42, 42.42);
        assertMinMax(0.0, POSITIVE_INFINITY);
        assertMinMax(NEGATIVE_INFINITY, 0.0);
        assertMinMax(NEGATIVE_INFINITY, POSITIVE_INFINITY);
    }

    @Test
    public void testNaN()
    {
        assertThrows(() -> new DoubleStatistics(0.0, NaN));
        assertThrows(() -> new DoubleStatistics(NaN, 0.0));
        assertThrows(() -> new DoubleStatistics(NaN, NaN));
    }

    @Test
    public void testRetainedSize()
    {
        assertRetainedSize(0.0, 42.0, INSTANCE_SIZE);
        assertRetainedSize(42.0, 42.0, INSTANCE_SIZE);
        assertRetainedSize(NEGATIVE_INFINITY, 42.0, INSTANCE_SIZE);
        assertRetainedSize(42.0, POSITIVE_INFINITY, INSTANCE_SIZE);
        assertRetainedSize(NEGATIVE_INFINITY, POSITIVE_INFINITY, INSTANCE_SIZE);
    }
}
