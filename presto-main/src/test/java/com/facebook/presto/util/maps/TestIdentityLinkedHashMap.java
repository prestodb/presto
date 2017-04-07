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
package com.facebook.presto.util.maps;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

public class TestIdentityLinkedHashMap
{
    private final BigDecimal key;
    private final BigDecimal equalToKey;
    private final BigDecimal value;
    private final BigDecimal equalToValue;

    public TestIdentityLinkedHashMap()
    {
        key = new BigDecimal("3.141592");
        equalToKey = new BigDecimal(key.toString());
        checkState(key.equals(equalToKey));

        value = new BigDecimal("42");
        equalToValue = new BigDecimal(value.toString());
        checkState(value.equals(equalToValue));
    }

    @Test
    public void testUsesIdentityAsEquivalenceForKeys()
    {
        String key = "foo";
        String otherKey = new String(key);
        assertEquals(key, otherKey);
        assertNotSame(key, otherKey);

        IdentityLinkedHashMap<String, Integer> map = new IdentityLinkedHashMap<>();
        assertTrue(map.isEmpty());

        int value = 1;
        int otherValue = 2;

        map.put(key, value);
        assertEquals(map, ImmutableMap.of(key, value));
        assertTrue(map.containsKey(key));
        assertFalse(map.containsKey(otherKey));
        assertTrue(map.containsValue(value));

        map.put(otherKey, otherValue);
        assertEquals(map.get(key), Integer.valueOf(value));
        assertEquals(map.get(otherKey), Integer.valueOf(otherValue));

        assertEquals(map.size(), otherValue);
        map.remove(key);
        assertEquals(map.size(), value);
        map.remove(otherKey);
        assertTrue(map.isEmpty());

        Set<String> keys = ImmutableSet.of("a", "aa");
        Map<String, Integer> expectedMap = ImmutableMap.of("a", value, "aa", otherValue);

        map.putAll(Maps.asMap(keys, String::length));
        assertEquals(expectedMap, map);

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testStableIterationOrder()
    {
        List<String> keys = ImmutableList.of("All", "your", "base", "are", "belong", "to", "us");
        List<Integer> expectedValues = keys.stream().map(String::length).collect(toImmutableList());

        range(0, 10).forEach(attempt -> {
            IdentityLinkedHashMap<String, Integer> map = new IdentityLinkedHashMap<>();

            keys.forEach(i -> map.put(i, i.length()));

            assertEquals(ImmutableList.copyOf(map.keySet()), keys);
            assertEquals(ImmutableList.copyOf(map.keySet().iterator()), keys);
            assertEquals(map.keySet().stream().collect(toImmutableList()), keys);

            assertEquals(ImmutableList.copyOf(map.values()), expectedValues);
            assertEquals(ImmutableList.copyOf(map.values().iterator()), expectedValues);
            assertEquals(map.values().stream().collect(toImmutableList()), expectedValues);

            assertEquals(ImmutableList.copyOf(map.entrySet()).stream().map(Entry::getKey).collect(toImmutableList()), keys);
            assertEquals(ImmutableList.copyOf(map.entrySet()::iterator).stream().map(Entry::getKey).collect(toImmutableList()), keys);
            assertEquals(map.entrySet().stream().map(Entry::getKey).collect(toImmutableList()), keys);
        });
    }

    @Test(dataProvider = "identityHashMaps")
    public void testKeySetIsIdentityBased(Supplier<Map<BigDecimal, Object>> mapSupplier)
    {
        Map<BigDecimal, Object> map = mapSupplier.get();
        map.put(key, new Object());
        Set<?> keySet = map.keySet();

        assertTrue(keySet.contains(key));
        assertFalse(keySet.contains(equalToKey));
        assertFalse(keySet.remove(equalToKey));
        assertEquals(keySet.size(), 1);

        assertTrue(keySet.remove(key)); // modification

        assertEquals(keySet.size(), 0);
        assertEquals(map.size(), 0);
        assertTrue(keySet.isEmpty());
        assertTrue(map.isEmpty());
    }

    @Test(dataProvider = "identityHashMaps")
    public void testEntrySetIsIdentityBased(Supplier<Map<BigDecimal, BigDecimal>> mapSupplier)
    {
        Map<BigDecimal, BigDecimal> map = mapSupplier.get();
        map.put(key, value);
        Set<Entry<BigDecimal, BigDecimal>> entrySet = map.entrySet();

        assertTrue(entrySet.contains(Maps.immutableEntry(key, value)));
        assertFalse(entrySet.contains(Maps.immutableEntry(equalToKey, value)));

        assertFalse(entrySet.contains(Maps.immutableEntry(key, equalToValue)));
        assertFalse(entrySet.contains(Maps.immutableEntry(equalToKey, equalToValue)));

        assertFalse(entrySet.contains(Maps.immutableEntry(key, (BigDecimal) null)));
        assertFalse(entrySet.contains(Maps.immutableEntry(equalToKey, (BigDecimal) null)));

        assertFalse(entrySet.remove(Maps.immutableEntry(equalToKey, equalToValue)));
        assertFalse(entrySet.remove(Maps.immutableEntry(equalToKey, value)));
        assertFalse(entrySet.remove(Maps.immutableEntry(key, equalToValue)));
        assertFalse(entrySet.remove(Maps.immutableEntry(key, (BigDecimal) null)));

        assertEquals(entrySet.size(), 1);

        assertTrue(entrySet.remove(Maps.immutableEntry(key, value))); // modification

        assertEquals(entrySet.size(), 0);
        assertEquals(map.size(), 0);
        assertTrue(entrySet.isEmpty());
        assertTrue(map.isEmpty());
    }

    @DataProvider
    public Object[][] identityHashMaps()
    {
        // Test IdentityHashMap too to ensure IdentityLinkedHashMap is a drop-in replacement except for deterministic iteration order
        return new Object[][] {
                {supplierOf(IdentityHashMap.class)},
                {supplierOf(IdentityLinkedHashMap.class)}
        };
    }

    /**
     * Same as {@code clazz::new} except the returned object implements {@link Object#toString()}.
     */
    private static <T> Supplier<T> supplierOf(Class<? extends T> clazz)
    {
        return new Supplier<T>()
        {
            @Override
            public T get()
            {
                try {
                    return clazz.getConstructor()
                            .newInstance();
                }
                catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public String toString()
            {
                return format("%s::new", clazz.getName());
            }
        };
    }
}
