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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.statistics.StringRange.union;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestStringRange
{
    @Test
    public void testRange()
    {
        assertRange("", "zZxX");
        assertRange("", "");
        assertThatThrownBy(() -> new StringRange("Z", "")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUnion()
    {
        assertEquals(union(new StringRange("", "aa"), new StringRange("bb", "cc")), new StringRange("", "cc"));
        assertEquals(union(new StringRange("", "Zz"), new StringRange("123", "bb")), new StringRange("", "bb"));
    }

    private static void assertRange(String min, String max)
    {
        StringRange range = new StringRange(min, max);
        assertEquals(range.getMin(), min);
        assertEquals(range.getMax(), max);
    }
}
