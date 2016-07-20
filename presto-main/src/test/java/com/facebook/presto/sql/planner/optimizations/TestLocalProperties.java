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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.optimizations.LocalProperties.extractLeadingConstants;
import static com.facebook.presto.sql.planner.optimizations.LocalProperties.stripLeadingConstants;
import static org.testng.Assert.assertEquals;

public class TestLocalProperties
{
    @Test
    public void testConstantProcessing()
            throws Exception
    {
        assertEquals(stripLeadingConstants(ImmutableList.of()), ImmutableList.of());
        assertEquals(extractLeadingConstants(ImmutableList.of()), ImmutableSet.of());

        List<LocalProperty<String>> input = ImmutableList.of(grouped("a"));
        assertEquals(stripLeadingConstants(input), ImmutableList.of(grouped("a")));
        assertEquals(extractLeadingConstants(input), ImmutableSet.of());

        input = ImmutableList.of(constant("b"), grouped("a"));
        assertEquals(stripLeadingConstants(input), ImmutableList.of(grouped("a")));
        assertEquals(extractLeadingConstants(input), ImmutableSet.of("b"));

        input = ImmutableList.of(constant("a"), grouped("a"));
        assertEquals(stripLeadingConstants(input), ImmutableList.of(grouped("a")));
        assertEquals(extractLeadingConstants(input), ImmutableSet.of("a"));

        input = ImmutableList.of(grouped("a"), constant("b"));
        assertEquals(stripLeadingConstants(input), input);
        assertEquals(extractLeadingConstants(input), ImmutableSet.of());

        input = ImmutableList.of(constant("a"));
        assertEquals(stripLeadingConstants(input), ImmutableList.of());
        assertEquals(extractLeadingConstants(input), ImmutableSet.of("a"));

        input = ImmutableList.of(constant("a"), constant("b"));
        assertEquals(stripLeadingConstants(input), ImmutableList.of());
        assertEquals(extractLeadingConstants(input), ImmutableSet.of("a", "b"));
    }

    @Test
    public void testTranslate()
            throws Exception
    {
        Map<String, String> map = ImmutableMap.of();
        List<LocalProperty<String>> input = ImmutableList.of();
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of());

        map = ImmutableMap.of();
        input = ImmutableList.of(grouped("a"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of());

        map = ImmutableMap.of("a", "a1");
        input = ImmutableList.of(grouped("a"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1")));

        map = ImmutableMap.of();
        input = ImmutableList.of(constant("a"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of());

        map = ImmutableMap.of();
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of());

        map = ImmutableMap.of("b", "b1");
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("b1")));

        map = ImmutableMap.of("a", "a1", "b", "b1");
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(constant("a1"), grouped("b1")));

        map = ImmutableMap.of("a", "a1", "b", "b1");
        input = ImmutableList.of(grouped("a", "b"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1", "b1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(constant("a"), grouped("b"), grouped("c"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(constant("a1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a", "b"), grouped("c"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of());

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a"), grouped("b"), grouped("c"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(constant("b"), grouped("a", "b"), grouped("c")); // Because b is constant, we can rewrite (a, b)
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1"), grouped("c1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a"), constant("b"), grouped("c")); // Don't fail c translation due to a failed constant translation
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1"), grouped("c1")));

        map = ImmutableMap.of("a", "a1", "b", "b1", "c", "c1");
        input = ImmutableList.of(grouped("a"), constant("b"), grouped("c"));
        assertEquals(LocalProperties.translate(input, translateWithMap(map)), ImmutableList.of(grouped("a1"), constant("b1"), grouped("c1")));
    }

    private static <X, Y> Function<X, Optional<Y>> translateWithMap(Map<X, Y> translateMap)
    {
        return input -> Optional.ofNullable(translateMap.get(input));
    }

    @Test
    public void testNormalizeEmpty()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder().build();
        assertNormalize(localProperties);
        assertNormalizeAndFlatten(localProperties);
    }

    @Test
    public void testNormalizeSingleSmbolGroup()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder().grouped("a").build();
        assertNormalize(localProperties, Optional.of(grouped("a")));
        assertNormalizeAndFlatten(localProperties, grouped("a"));
    }

    @Test
    public void testNormalizeOverlappingSymbol()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder()
                .grouped("a")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .constant("a")
                .build();
        assertNormalize(
                localProperties,
                Optional.of(grouped("a")),
                Optional.empty(),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                grouped("a"));
    }

    @Test
    public void testNormalizeComplexWithLeadingConstant()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder()
                .constant("a")
                .grouped("a", "b")
                .grouped("b", "c")
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();
        assertNormalize(
                localProperties,
                Optional.of(constant("a")),
                Optional.of(grouped("b")),
                Optional.of(grouped("c")),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                constant("a"),
                grouped("b"),
                grouped("c"));
    }

    @Test
    public void testNormalizeComplexWithMiddleConstant()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("a", "b")
                .grouped("c")
                .constant("a")
                .build();
        assertNormalize(
                localProperties,
                Optional.of(sorted("a", SortOrder.ASC_NULLS_FIRST)),
                Optional.of(grouped("b")),
                Optional.of(grouped("c")),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                sorted("a", SortOrder.ASC_NULLS_FIRST),
                grouped("b"),
                grouped("c"));
    }

    @Test
    public void testNormalizeDifferentSorts()
            throws Exception
    {
        List<LocalProperty<String>> localProperties = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("a", SortOrder.DESC_NULLS_LAST)
                .build();
        assertNormalize(
                localProperties,
                Optional.of(sorted("a", SortOrder.ASC_NULLS_FIRST)),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                sorted("a", SortOrder.ASC_NULLS_FIRST));
    }

    @Test
    public void testMatchedGroupHierarchy()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a")
                .grouped("b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder()
                        .grouped("a")
                        .grouped("a")
                        .grouped("a")
                        .grouped("a")
                        .grouped("b")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testGroupedTuple()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("a", "b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("a").grouped("b").build(),
                Optional.of(grouped("a")),
                Optional.of(grouped("b")));
    }

    @Test
    public void testGroupedDoubleThenSingle()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("a", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void testGroupedDoubleThenDouble()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c", "a")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void testSortProperties()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .sorted("c", SortOrder.ASC_NULLS_FIRST)
                        .build(),
                Optional.empty(),
                Optional.of(sorted("c", SortOrder.ASC_NULLS_FIRST)));
    }

    @Test
    public void testSortGroupSort()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("b", "c")
                .sorted("d", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d", "e").build(),
                Optional.of(grouped("e")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .sorted("b", SortOrder.ASC_NULLS_FIRST)
                        .build(),
                Optional.empty(),
                Optional.of(sorted("b", SortOrder.ASC_NULLS_FIRST)));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .grouped("b", "c", "d")
                        .grouped("e", "a")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(grouped("e")));
    }

    @Test
    public void testPartialConstantGroup()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());
    }

    @Test
    public void testNonoverlappingConstantGroup()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder()
                        .grouped("b")
                        .grouped("a")
                        .build(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testConstantWithMultiGroup()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .grouped("a", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.empty());
    }

    @Test
    public void testConstantWithSort()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("b")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.empty());
    }

    @Test
    public void testMoreRequiredGroupsThanActual()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .constant("b")
                .grouped("a")
                .grouped("d")
                .build();

        assertMatch(
                actual,
                builder()
                        .grouped("a")
                        .grouped("b")
                        .grouped("c")
                        .grouped("d")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(grouped("c")),
                Optional.of(grouped("d")));
    }

    @Test
    public void testDifferentSortOrders()
            throws Exception
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_LAST)
                        .build(),
                Optional.of(sorted("a", SortOrder.ASC_NULLS_LAST)));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        }));

        TestingColumnHandle columnHandle = new TestingColumnHandle("a");

        LocalProperty<ColumnHandle> property1 = new ConstantProperty<>(columnHandle);
        assertEquals(property1, mapper.readValue(mapper.writeValueAsString(property1), new TypeReference<LocalProperty<ColumnHandle>>() {}));

        LocalProperty<ColumnHandle> property2 = new SortingProperty<>(columnHandle, SortOrder.ASC_NULLS_FIRST);
        assertEquals(property2, mapper.readValue(mapper.writeValueAsString(property2), new TypeReference<LocalProperty<ColumnHandle>>() {}));

        LocalProperty<ColumnHandle> property3 = new GroupingProperty<>(ImmutableList.of(columnHandle));
        assertEquals(property3, mapper.readValue(mapper.writeValueAsString(property3), new TypeReference<LocalProperty<ColumnHandle>>() {}));
    }

    @SafeVarargs
    private static <T> void assertMatch(List<LocalProperty<T>> actual, List<LocalProperty<T>> wanted, Optional<LocalProperty<T>>... match)
    {
        assertEquals(LocalProperties.match(actual, wanted), Arrays.asList(match));
    }

    @SafeVarargs
    private static <T> void assertNormalize(List<LocalProperty<T>> localProperties, Optional<LocalProperty<T>>... normalized)
    {
        assertEquals(LocalProperties.normalize(localProperties), Arrays.asList(normalized));
    }

    @SafeVarargs
    private static <T> void assertNormalizeAndFlatten(List<LocalProperty<T>> localProperties, LocalProperty<T>... normalized)
    {
        assertEquals(LocalProperties.normalizeAndPrune(localProperties), Arrays.asList(normalized));
    }

    private static ConstantProperty<String> constant(String column)
    {
        return new ConstantProperty<>(column);
    }

    private static GroupingProperty<String> grouped(String... columns)
    {
        return new GroupingProperty<>(Arrays.asList(columns));
    }

    private static SortingProperty<String> sorted(String column, SortOrder order)
    {
        return new SortingProperty<>(column, order);
    }

    private static Builder builder()
    {
        return new Builder();
    }

    private static class Builder
    {
        private final List<LocalProperty<String>> properties = new ArrayList<>();

        public Builder grouped(String... columns)
        {
            properties.add(new GroupingProperty<>(Arrays.asList(columns)));
            return this;
        }

        public Builder sorted(String column, SortOrder order)
        {
            properties.add(new SortingProperty<>(column, order));
            return this;
        }

        public Builder constant(String column)
        {
            properties.add(new ConstantProperty<>(column));
            return this;
        }

        public List<LocalProperty<String>> build()
        {
            return new ArrayList<>(properties);
        }
    }
}
