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
package com.facebook.presto.operator.scalar;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.scalar.GroupingOperationFunction.grouping;
import static org.testng.Assert.assertEquals;

public class TestGroupingOperationFunction
{
    @Test
    public void testGroupingOperationAllBitsSet()
    {
        List groupingOrdinals = Arrays.asList(0, 4, 8);
        List groupingSetOrdinals = Arrays.asList(Arrays.asList(1), Arrays.asList(7, 3, 1), Arrays.asList(9, 1));

        for (int groupId = 0; groupId < groupingSetOrdinals.size(); groupId++) {
            assertEquals(grouping(groupId, groupingOrdinals, groupingSetOrdinals), 7L);
        }
    }

    @Test
    public void testGroupingOperationNoBitsSet()
    {
        List groupingOrdinals = Arrays.asList(4, 6);
        List groupingSetOrdinals = Arrays.asList(Arrays.asList(4, 6));

        for (int groupId = 0; groupId < groupingSetOrdinals.size(); groupId++) {
            assertEquals(grouping(groupId, groupingOrdinals, groupingSetOrdinals), 0L);
        }
    }

    @Test
    public void testGroupingOperationSomeBitsSet()
    {
        List groupingOrdinals = Arrays.asList(7, 2, 9);
        List groupingSetOrdinals = Arrays.asList(Arrays.asList(4, 6), Arrays.asList(4, 2), Arrays.asList(9, 7, 14));
        List expectedResults = Arrays.asList(7L, 5L, 2L);

        for (int groupId = 0; groupId < groupingSetOrdinals.size(); groupId++) {
            assertEquals(grouping(groupId, groupingOrdinals, groupingSetOrdinals), expectedResults.get(groupId));
        }
    }
}
