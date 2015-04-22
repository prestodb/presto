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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertTrue;

public class TestLocalProperties
{
    @Test
    public void testGrouping()
            throws Exception
    {
        builder()
                .grouped("a")
                .grouped("b")
                .grouped("c")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());

        builder()
                .grouped("a", "b", "c")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());

        builder()
                .grouped("a", "b")
                .grouped("c")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());

        builder()
                .grouped("a", "b")
                .grouped("a", "c")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());

        builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());

        builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("b", "c")
                .sorted("d", SortOrder.ASC_NULLS_FIRST)
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d", "e"), ImmutableSet.of("a", "b", "c", "d"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c", "d"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of())
                .assertMaxGroupingSubset(ImmutableSet.of("c"), ImmutableSet.of());
    }

    @Test
    public void testConstants()
            throws Exception
    {
        builder()
                .constants("a")
                .grouped("a", "b")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of("b"));

        builder()
                .constants("a")
                .grouped("b")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of("b"));

        builder()
                .constants("a")
                .grouped("a", "b")
                .grouped("a", "c")
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c", "d"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b", "c"), ImmutableSet.of("a", "b", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "b"), ImmutableSet.of("a", "b"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of("a"))
                .assertMaxGroupingSubset(ImmutableSet.of("b"), ImmutableSet.of("b"))
                .assertMaxGroupingSubset(ImmutableSet.of("b", "c"), ImmutableSet.of("b", "c"));

        builder()
                .constants("b")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c", "d"), ImmutableSet.of("a", "c"))
                .assertMaxGroupingSubset(ImmutableSet.of("a", "c"), ImmutableSet.of("a", "c"));
    }

    public static Checker builder()
    {
        return new Checker();
    }

    public static class Checker
    {
        private final List<LocalProperty<String>> properties = new ArrayList<>();
        private final Set<String> constants = new HashSet<>();

        public Checker grouped(String... columns)
        {
            properties.add(new GroupingProperty<>(Arrays.asList(columns)));
            return this;
        }

        public Checker sorted(String column, SortOrder order)
        {
            properties.add(new SortingProperty<>(column, order));
            return this;
        }

        public Checker constants(String... columns)
        {
            constants.addAll(Arrays.asList(columns));
            return this;
        }

        public Checker assertMaxGroupingSubset(Set<String> candidates, Set<String> expected)
        {
            Set<String> actual = LocalProperty.getMaxGroupingSubset(properties, constants, candidates);
            assertTrue(actual.equals(expected), String.format(
                    "Expected %s => %s given constants = %s, candidates = %s. However, found => %s",
                    properties,
                    new GroupingProperty<>(expected),
                    constants,
                    candidates,
                    new GroupingProperty<>(actual)));
            return this;
        }
    }
}
