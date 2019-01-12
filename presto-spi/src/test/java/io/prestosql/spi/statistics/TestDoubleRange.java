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
package io.prestosql.spi.statistics;

import org.testng.annotations.Test;

import static io.prestosql.spi.statistics.DoubleRange.union;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDoubleRange
{
    @Test
    public void testRange()
    {
        assertRange(0, 0);
        assertRange(0, 0.1);
        assertRange(-0.1, 0.1);
        assertRange(Double.NEGATIVE_INFINITY, 0);
        assertRange(Float.NEGATIVE_INFINITY, 0);
        assertRange(Double.NEGATIVE_INFINITY, -1.0 * Double.MAX_VALUE);
        assertRange(Float.NEGATIVE_INFINITY, -1.0 * Double.MAX_VALUE);
        assertRange(Float.NEGATIVE_INFINITY, -1.0 * Float.MAX_VALUE);
        assertRange(Double.MAX_VALUE, Double.POSITIVE_INFINITY);
        assertRange(Float.MAX_VALUE, Double.POSITIVE_INFINITY);
        assertRange(Double.MAX_VALUE, Float.POSITIVE_INFINITY);
        assertRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        assertRange(Double.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
        assertRange(Float.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        assertThatThrownBy(() -> new DoubleRange(Double.NaN, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(0, Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.NaN, Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Float.NaN, Float.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(0, Double.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(0, Float.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(-1.0 * Double.MAX_VALUE, Double.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(-1.0 * Float.MAX_VALUE, Double.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(-1.0 * Double.MAX_VALUE, Float.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.POSITIVE_INFINITY, Double.MAX_VALUE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Float.POSITIVE_INFINITY, Double.MAX_VALUE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.POSITIVE_INFINITY, Float.MAX_VALUE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Double.POSITIVE_INFINITY, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleRange(Float.POSITIVE_INFINITY, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUnion()
    {
        assertEquals(union(new DoubleRange(1, 2), new DoubleRange(4, 5)), new DoubleRange(1, 5));
        assertEquals(union(new DoubleRange(1, 2), new DoubleRange(1, 2)), new DoubleRange(1, 2));
        assertEquals(union(new DoubleRange(4, 5), new DoubleRange(1, 2)), new DoubleRange(1, 5));
        assertEquals(union(new DoubleRange(Double.NEGATIVE_INFINITY, 0), new DoubleRange(0, Double.POSITIVE_INFINITY)), new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        assertEquals(union(new DoubleRange(0, Double.POSITIVE_INFINITY), new DoubleRange(Double.NEGATIVE_INFINITY, 0)), new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    private static void assertRange(double min, double max)
    {
        DoubleRange range = new DoubleRange(min, max);
        assertEquals(range.getMin(), min);
        assertEquals(range.getMax(), max);
    }
}
