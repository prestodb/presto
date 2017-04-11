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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

public class TestIdentityLinkedHashMap
{
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
}
