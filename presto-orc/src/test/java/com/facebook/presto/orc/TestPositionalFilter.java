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
package com.facebook.presto.orc;

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPositionalFilter
{
    @Test
    public void test()
    {
        PositionalFilter filter = new PositionalFilter();

        // a[1] = 1 and a[3] = 3

        TupleDomainFilter[] filters = new TupleDomainFilter[] {
                equals(1), null, equals(3), null,
                equals(1), null, equals(3), null,
                equals(1), null, equals(3), null, null,
                equals(1), null, equals(3), null, null, null,
                equals(1), null, equals(3), null, null, null, null
        };

        long[] values = new long[] {
            1, 2, 3, 4,         // pass
            0, 2, 3, 4,         // fail
            1, 2, 0, 4, 5,      // fail
            1, 0, 3, 0, 5, 6,   // pass
            1, 1, 2, 2, 3, 3, 4 // fail
        };

        boolean[] expectedResults = new boolean[] {
            true, true, true, true,
            false,
            true, true, false,
            true, true, true, true, true, true,
            true, true, false,
        };

        int[] offsets = new int[] {0, 4, 8, 13, 19, 26};

        filter.setFilters(filters, offsets);

        int valuesIndex = 0;
        for (int i = 0; i < expectedResults.length; i++) {
            assertEquals(expectedResults[i], filter.testLong(values[valuesIndex++]));
            if (expectedResults[i] == false) {
                valuesIndex += filter.getSucceedingPositionsToFail();
            }
        }
        assertEquals(new boolean[] {false, true, true, false, true, false}, filter.getFailed());
    }

    private TupleDomainFilter equals(int value)
    {
        return BigintRange.of(value, value, false);
    }
}
