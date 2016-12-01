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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

public class IdentityLinkedHashMapTest
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
        Set<String> keys = ImmutableSet.of("All", "your", "base", "are", "belong", "to", "us");
        Map<String, Integer> expectedMap = Maps.asMap(keys, String::length);

        range(0, 10).forEach(attempt -> {
            IdentityLinkedHashMap<String, Integer> map = new IdentityLinkedHashMap<>();

            keys.forEach(i -> map.put(i, i.length()));

            assertEquals(map.keySet(), keys);
            assertEquals(map.values(), expectedMap.values());
            assertEquals(map.entrySet(), expectedMap.entrySet());
        });
    }
}
