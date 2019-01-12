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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static io.prestosql.sql.planner.GroupingOperationRewriter.calculateGrouping;
import static org.testng.Assert.assertEquals;

public class TestGroupingOperationRewriter
{
    private static final List<Integer> fortyIntegers = ImmutableList.of(
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40);

    @Test
    public void testGroupingOperationAllBitsSet()
    {
        List<Integer> groupingOrdinals = ImmutableList.of(0, 4, 8);
        List<Set<Integer>> groupingSetOrdinals = ImmutableList.of(ImmutableSet.of(1), ImmutableSet.of(7, 3, 1), ImmutableSet.of(9, 1));

        for (Set<Integer> groupingSet : groupingSetOrdinals) {
            assertEquals(calculateGrouping(groupingSet, groupingOrdinals), 7L);
        }
    }

    @Test
    public void testGroupingOperationNoBitsSet()
    {
        List<Integer> groupingOrdinals = ImmutableList.of(4, 6);
        List<Set<Integer>> groupingSetOrdinals = ImmutableList.of(ImmutableSet.of(4, 6));

        for (Set<Integer> groupingSet : groupingSetOrdinals) {
            assertEquals(calculateGrouping(groupingSet, groupingOrdinals), 0L);
        }
    }

    @Test
    public void testGroupingOperationSomeBitsSet()
    {
        List<Integer> groupingOrdinals = ImmutableList.of(7, 2, 9, 3, 5);
        List<Set<Integer>> groupingSetOrdinals = ImmutableList.of(ImmutableSet.of(4, 2), ImmutableSet.of(9, 7, 14), ImmutableSet.of(5, 2, 7), ImmutableSet.of(3));
        List<Long> expectedResults = ImmutableList.of(23L, 11L, 6L, 29L);

        for (int groupId = 0; groupId < groupingSetOrdinals.size(); groupId++) {
            Set<Integer> groupingSet = groupingSetOrdinals.get(groupId);
            assertEquals(Long.valueOf(calculateGrouping(groupingSet, groupingOrdinals)), expectedResults.get(groupId));
        }
    }

    @Test
    public void testMoreThanThirtyTwoArguments()
    {
        List<Set<Integer>> groupingSetOrdinals = ImmutableList.of(ImmutableSet.of(20, 2, 13, 33, 40, 9, 14), ImmutableSet.of(28, 4, 5, 29, 31, 10));
        List<Long> expectedResults = ImmutableList.of(822283861886L, 995358664191L);

        for (int groupId = 0; groupId < groupingSetOrdinals.size(); groupId++) {
            Set<Integer> groupingSet = groupingSetOrdinals.get(groupId);
            assertEquals(Long.valueOf(calculateGrouping(groupingSet, fortyIntegers)), expectedResults.get(groupId));
        }
    }
}
